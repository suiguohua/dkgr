# -*- coding:utf-8 -*-
import os
import json
from tqdm import tqdm
from neo4j.client import neo4j
from neo4j.models import TableInfoNode, DataNode, GetTableInfoNode
from oracle.client import oracle
from server.wells import well_cache


def process_raw_data(data):
    return str(data).strip().replace(' ', '').replace('(', '（').replace(')', '）').replace('\t', '')


class DataExtractor:
    def __init__(self):
        # 数据表根节点，所有的数据表路径都是从V_AZ01为起点的
        self.table_path_root_node = GetTableInfoNode("V_AZ01")
        if not self.table_path_root_node.exist:  # 判断节点是否在Neo4j中存在
            self.table_path_root_node.create()  # 在neo4j中新增该节点

    @staticmethod
    def table_list():
        cypher = "match (n:TableInfo) where n.table_name <> 'V_AZ01' return n"
        res = neo4j.run(cypher)

        tables = [{"user_space": table["n"]["user_space"],
                   "table_name": table["n"]["table_name"],
                   "table_common_name": table["n"]["table_common_name"]} for table in res]
        return tables

    @staticmethod
    def leaf_tables():
        cypher = "match (n:TableInfo)-[:key_to_table]->(m) return n"
        res = neo4j.run(cypher)

        non_leaf_tables = [table["n"]["table_name"] for table in res]
        all_tables = [table["table_name"] for table in DataExtractor.table_list()]
        return [table for table in all_tables if table not in non_leaf_tables]

    def add_table_path(self, path, from_well=True):
        """
        根据用户在前端输入的数据表路径，将该路径添加至neo4j中
        """
        if from_well:
            pre_node = self.table_path_root_node
            pre_relation_key = "JH"
        else:
            pre_node = None
            pre_relation_key = None

        path_added = list()

        for p in path:
            table_name = p["table_name"]
            table_common_name = p["table_common_name"]
            user_space = p["user_space"]
            primary_keys = p["primary_keys"]
            key_to_next = p["key_to_next"]
            if "unique_keys" in p:
                unique_keys = p["unique_keys"]
            else:
                unique_keys = p["primary_keys"]
            node = TableInfoNode(table_name, table_common_name, user_space, primary_keys, unique_keys)

            if not from_well and not node.exist:
                # 该路径不是从主体为起点，但是该路径的第一张表却还未被加入过，如果创建路径的话，会导致出现没有正确连接的孤立节点
                return path_added, False

            if not node.exist:
                node.create()

            if pre_node:
                if pre_node.link(node, r_type="key_to_table", r_properties={"keys": pre_relation_key}):
                    path_added.append({"start_table": pre_node.table_name,
                                       "end_table": node.table_name,
                                       "key": pre_relation_key})
            pre_node = node
            pre_relation_key = key_to_next

        return path_added, True

    def find_table_path(self, table_name):
        """
        根据数据表名，找到从根数据表到其的查询路径
        """
        table = GetTableInfoNode(table_name)  # 调用models中函数。从Oracle中抽取的表属性->neo4j数据库的标签、主键、属性
        if not table.exist:  # 调用models中函数。判断节点是否在Neo4j中存在
            return None

        cypher = "match (n)-[r:key_to_table *]->(m) " \
                 f"where id(n) = {self.table_path_root_node.identity} and id(m) = {table.identity} " \
                 "return r"

        res = neo4j.run(cypher)[0]["r"]
        path = list()
        for r in res:
            start_table = GetTableInfoNode(r.start_node["table_name"])
            end_table = GetTableInfoNode(r.end_node["table_name"])
            relation_field = r["keys"]
            path.append((start_table, end_table, relation_field))
        return path

    # 删除所有的表信息路径和节点
    def delete_all_exist_paths(self):
        # delete all current path
        cypher = "match ()-[r:key_to_table]->() delete r"
        neo4j.run(cypher)

        cypher = "match (n:TableInfo) delete n"
        neo4j.run(cypher)

        self.table_path_root_node.create()

    @staticmethod
    def add_paths_from_file():
        from config_parser import data_path
        path = os.path.join(data_path, "table_paths")
        files = list()

        def retrieval_all_json_files(cur_path):
            nonlocal files

            if os.path.isdir(cur_path):
                for sub_path in os.listdir(cur_path):
                    retrieval_all_json_files(os.path.join(cur_path, sub_path))
            elif os.path.isfile(cur_path):
                if os.path.basename(cur_path).endswith(".json"):
                    files.append(cur_path)

        retrieval_all_json_files(path)

        for json_file in files:
            with open(json_file, "r", encoding='utf8') as f:
                path_json = json.load(f)
                path = path_json.get("path", None)
                from_well = path_json.get("from_well", True)
                data_extractor.add_table_path(path, from_well)

    def extract_data_path_level_per_well(self, well_name, table_name, accumulated_added=None):
        from collections import defaultdict
        # defaultdict 作用是当字典里的key不存在时，返回的是函数的默认值，比如list对应[ ]，str对应的是空字符串，set对应set( )，int对应0
        accumulated_added = accumulated_added or defaultdict(int)
        path = self.find_table_path(table_name)   # 获取路径信息，包括当前层，下一层，关系————三元素列表

        well_node = DataNode(well_name, None, is_well=True)  # neo4j中主体的信息
        if well_node.identity is None:
            return accumulated_added

        cur_level_nodes = [well_node]   # 从主体实体节点开始，循环整条路径进行数据加载
        for cur_level_table, next_level_table, relation_field in path:  # relation_field:'JH'
            # 遍历路径上的每一层
            next_level_nodes = list()
            for node in cur_level_nodes:
                # 对于当前层的每个数据节点，先查询其是否已经和下一层数据节点相连，有的话说明下一层表的数据已经被导入过
                cypher = f"match (n)-[]->(m:{next_level_table.table_name}) where id(n) = {node.identity} return m"
                res = neo4j.run(cypher)    # 查出已有关联的下一层的所有节点，并存入列表
                for n in res:
                    n = n["m"]
                    data_node = DataNode(next_level_table.table_name,
                                         from_neo4j_identity=n.identity,
                                         from_neo4j_properties=dict(n.items()))
                    next_level_nodes.append(data_node)

                # 获取当前节点在db中对应的数据行的relation_field字段值
                if type(relation_field) != list:
                    relation_field = [relation_field]
                query_value = self.get_db_row_data(node, relation_field)
                if len(query_value) == 0:  # 表中查找不到则继续下一层循环
                    continue
                if type(relation_field) == list:
                    conditions = {relation_field[i]: query_value[i] for i in range(len(relation_field))}  # 整理成字典格式
                else:
                    conditions = {relation_field: query_value}

                # 根据下一层表名和关联主键，查询relation_field字段为query_value的数据行，获取其唯一键，不需要获取所有数据
                next_level_data = self.query_db_row_primary_values(next_level_table, conditions)  # 从oracle 表中取出相关数据
                # print(next_level_data)

                # --------------- 大数据量的表特殊处理 -------------------
                special_table = ['AZS04_1', 'AZY01']
                if table_name in special_table:
                    # 非主体斜表或主体无主体斜数据则直接返回
                    if not next_level_data:
                        return accumulated_added
                    print(f"{next_level_table.primary_value}是特殊表，数据量需一次性加载")

                    temp_group_data = list()
                    for temp in next_level_data:
                        data_node = DataNode(next_level_table.table_name, temp)
                        group_data = ""
                        for i in range(1, len(temp)):
                            group_data += f"{temp[i]}"
                            group_data += "," if i != len(temp)-1 else ""
                        temp_group_data.append(group_data)
                    format_data = ";".join(temp_group_data)

                    cypher = f"match (n:{next_level_table.primary_value} {{JH:'{query_value[0]}'}}) return n"
                    res = neo4j.run(cypher)

                    if not res:
                        # print("创建节点执行")
                        cypher = f"create (n:{next_level_table.primary_value} {{JH:'{query_value[0]}', CSZ:\"{format_data}\"}}) return n"
                        # print(cypher)
                        res = neo4j.run(cypher)[0]["n"]

                    # 查询头尾节点之间已有的所有关系
                    cypher = f"match (n:Well)-[r:{next_level_table.table_name.lower()}]->(m) where n.name='{query_value[0]}' and m.JH='{query_value[0]}' return r"
                    res1 = neo4j.run(cypher)
                    if not res1:
                    # MATCH (a:AZS04_1),(b:Well) where a.name='营11' AND b.name='营11' create (a)-[r:relation]->(b)
                        cypher = f"match (n:Well), (m:{next_level_table.primary_value}) where n.name='{query_value[0]}' " \
                                 f"and m.JH='{query_value[0]}' create (n)-[r:{next_level_table.table_name.lower()}]->(m)"
                        # print(cypher)
                        res2 = neo4j.run(cypher)

                    # 连接当前节点与下一层数据节点
                    next_level_nodes.append(data_node)
                    accumulated_added[next_level_table.table_name] += 1
                    # print("统计个数：{}".format(accumulated_added))
                # ------------------------ 源代码部分 ---------------------------
                else:
                    # next_level_data中即为当前数据行关联到下一层数据表中的下层数据行，即当前节点需要连接的下一层数据节点
                    for table_primary_values in next_level_data:
                        data_node = DataNode(next_level_table.table_name, table_primary_values)
                        for existing_node in next_level_nodes:
                            if data_node.equals(existing_node):
                                break
                        else:    # 所谓else指的是循环正常结束后要执行的代码，即如果是break终止循环的情况。else下方缩进的代码将不执行
                            # 新数据，创建节点
                            data_node.create()
                            # 连接当前节点与下一层数据节点
                            node.link(data_node, r_type=next_level_table.table_name.lower())
                            next_level_nodes.append(data_node)
                            accumulated_added[next_level_table.table_name] += 1

            cur_level_nodes = next_level_nodes
        return accumulated_added

    @staticmethod
    def update_wells():
        """
        扫描V_AZ01表，如果有新增的主体，则更新到系统中
        """
        from data_extraction.well2neo4j import Well, add_new_well_to_neo4j

        # query_fields = ["JH", "JB", "JX", "KTXM", "ZTMD", "DLWZ", "YQT", "ZZBX", "HZBY", "ZYMDC"]

        # ---------------------- 修改处 无id -----------------------
        query_fields = ['JH', 'JB', 'JX', 'KTXM', 'ZTMD', 'DLWZ', 'YQT', 'ZZBX', 'HZBY', 'ZYMDC', 'KZRQ', 'WZRQ',
                        'WZJS', 'WZCW']
        all_wells = oracle.query(query_fields, "V_AZ01", None)
        # cur_idx = len(well_cache.wells)

        added = 0
        count = 0
        total = len(all_wells)

        # for well_name, jb, jx, ktxm, ztmd, dlwz, yqt, zzbx, hzby, zymdc in all_wells:

        # --------------------- 修改处 无id ----------------------
        for well_name, jb, jx, ktxm, ztmd, dlwz, yqt, zzbx, hzby, zymdc, kzrq, wzrq, wzjs, wzcw in all_wells:
            count += 1
            print(f"\r当前更新进度: {count} / {total}", end="")
            # new_well = Well(cur_idx, well_name, jb, jx, ktxm, ztmd, dlwz, yqt, zzbx, hzby, zymdc)

            # --------------- 修改处 无id -----------------------
            new_well = Well(well_name, jb, jx, ktxm, ztmd, dlwz, yqt, zzbx, hzby, zymdc, kzrq, wzrq, wzjs, wzcw)

            # new_well = well_cache.add(new_well,
            #                           new_well.idx,
            #                           new_well.jh,
            #                           new_well.jb,
            #                           new_well.jx,
            #                           new_well.ktxm,
            #                           new_well.ztmd,
            #                           new_well.dlwz,
            #                           new_well.yqt,
            #                           new_well.zzbx,
            #                           new_well.hzby,
            #                           new_well.zymdc)

            # ---------------- 修改处 无id -----------------------
            new_well = well_cache.add(new_well,
                                      new_well.jh,
                                      new_well.jb,
                                      new_well.jx,
                                      new_well.ktxm,
                                      new_well.ztmd,
                                      new_well.dlwz,
                                      new_well.yqt,
                                      new_well.zzbx,
                                      new_well.hzby,
                                      new_well.zymdc,
                                      new_well.kzrq,
                                      new_well.wzrq,
                                      new_well.wzjs,
                                      new_well.wzcw)

            cypher = f"match (n:Well) where n.name = '{new_well.jh}' return n"
            if len(neo4j.run(cypher)) == 0:
                add_new_well_to_neo4j(new_well)
                added += 1

            cur_idx = len(well_cache.wells)

        print(f"\n主体更新完成，共向图谱中新增主体实体{added}个")


data_extractor = DataExtractor()

if __name__ == "__main__":

    data_extractor.extract_data_path_level_per_well("陈320", 'V_ASF03')


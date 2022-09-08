import time
import datetime
import py2neo.data
from neo4j.models import DataNode, Neo4jNode
import threading
from server.error import ErrorCode, Error, Result
from data_extraction.data_extractor import data_extractor
from neo4j.client import neo4j
from oracle.client import oracle
from server.wells import well_cache
from copy import deepcopy
from server.is_in_poly import is_in_poly
from server.is_in_date import is_in_date

logger_lock = threading.Lock()

# TODO cache封装成类，实现LRU，防止cache无限扩大
query_cache = dict()

# cache过期时间
CACHE_EXPIRED_ELAPSE = datetime.timedelta(days=0, hours=0, minutes=10, seconds=0)
DATA = 0
EXPIRED_TIME = 1


class DataErrorCode(ErrorCode):
    TableNameRequiredError = -301
    TableDataNotExtractedError = -302
    WellLayerDepthInfoNotFoundError = -303


def table_list():
    tables = data_extractor.table_list()
    return Result(data=tables)


class QueryLogAsyncThread(threading.Thread):
    def __init__(self, from_table, query_table):
        super().__init__()

        self.from_table = from_table
        self.query_table = query_table

    def run(self):
        if self.from_table is None or self.query_table is None:
            return

        with logger_lock:
            common_query_logger = Neo4jNode(["TableQueryLogger"], {"from_table": self.from_table}, "from_table")

            if self.query_table not in common_query_logger.properties:
                common_query_logger.properties[self.query_table] = 1
            else:
                common_query_logger.properties[self.query_table] += 1

            common_query_logger.upsert()


class QueryDataAsyncThread(threading.Thread):
    def __init__(self, well_name, table_name, min_depth, max_depth, from_table, return_raw_data=False):
        super().__init__()

        self.well_name = well_name
        self.table_name = table_name
        self.min_depth = min_depth
        self.max_depth = max_depth
        self.from_table = from_table
        self.return_raw_data = return_raw_data

        self.result = None

    def run(self):
        data = query_data_by_well_name_and_table_name_and_depth(self.well_name,
                                                                self.table_name,
                                                                self.min_depth,
                                                                self.max_depth,
                                                                self.from_table,
                                                                self.return_raw_data)

        if self.table_name == "AZF07":
            # AZF07需要特殊处理，过滤出FCLX为解释的
            data["datas"] = [item for item in data["datas"] if item["FCLX"] == "解释"]
            data["num"] = len(data["datas"])

        self.result = data


def query_data_by_well_name_and_table_name_and_depth(well_name, table_name, min_depth, max_depth, from_table,
                                                     return_raw_data=False):
    if well_name is None:
        return Error(DataErrorCode.WellNameRequiredError, "需传入主体号，请检查参数")

    if table_name is None:
        return Error(DataErrorCode.TableNameRequiredError, "需传入查询的表名，请检查参数")

    if well_cache.get(well_name) is None:
        return Error(DataErrorCode.WellNameNotFoundError, f"输入的主体号不存在: {well_name}")

    table_path = data_extractor.find_table_path(table_name)

    if table_path is None:
        return Error(DataErrorCode.TableDataNotExtractedError, f"表{table_name}的数据还未抽取，需先抽取该表数据")

    name_pool = 0
    tmp_variable_names = list()
    cypher_builder = list()
    node_builder = [f'match (:Well {{name: "{well_name}"}})']

    for _, table, _ in table_path:
        tmp_variable_name = ""
        if "HRange" in table.labels:
            tmp_variable_name = "hr" + str(name_pool)
            name_pool += 1
        elif table.table_name == table_name:
            tmp_variable_name = "target"
        node_builder.append(f"({tmp_variable_name}:{table.table_name})")
        if tmp_variable_name:
            tmp_variable_names.append(tmp_variable_name)

    cypher_builder.append("-[]->".join(node_builder))

    where_builder = list()
    for variable_name in tmp_variable_names:
        if variable_name.startswith("hr"):
            if max_depth:
                where_builder.append(f"{variable_name}.DJSD1 <= {max_depth}")
            if min_depth:
                where_builder.append(f"{variable_name}.DJSD2 >= {min_depth}")

    if where_builder:
        cypher_builder.append("where " + " and ".join(where_builder))

    cypher_builder.append(f"return {tmp_variable_names[-1]}")
    cypher = " ".join(cypher_builder)
    data_nodes = neo4j.run(cypher)
    data_nodes = [{k: v for k, v in n[tmp_variable_names[-1]].items() if v != "None"} for n in data_nodes]

    res = {
        "num": len(data_nodes),
        "fields": list(data_nodes[0].keys()),
        "datas": data_nodes
    }

    QueryLogAsyncThread(from_table, table_name).start()

    if return_raw_data:
        return res

    return Result(message="查询成功", data=res)


# ----------  新添通过条件进行主体筛选查询  -----------

def query_data_by_condition(mdc_name, syjg_name, yqt_name, rect_coords, poly, kzrq_limit, wzrq_limit, wzjs_limit,
                            wzcw_name):

    cypher_builder = list()
    node_builder = list()
    where_builder = list()

    if mdc_name:
        node_builder.append(f'(n:Well)-[:zymdc]->()-[*0..10]->(:MDC {{name: "{mdc_name}"}})')
    if syjg_name:
        node_builder.append(f'(n:Well)-[:ass01]->(:ASS01)-->(s:SYJG)')
        tmp = syjg_name.replace('，', ',')
        items = tmp.split(',')
        where_builder.append("s.name='" + "' or s.name='".join(items) + "'")
    if yqt_name:
        node_builder.append(f'(n:Well)-->(:YQT {{name: "{yqt_name}"}})')
    if wzcw_name:
        node_builder.append(f'(n:Well)-[:wzcw]->()-[*0..10]->(:MDC{{name: "{wzcw_name}"}})')

    if node_builder:
        cypher_builder.append("MATCH " + ", ".join(node_builder))
    else:
        cypher_builder = [f'MATCH (n:Well)']

    # 矩形限制条件
    if rect_coords:
        # 处理rect_coords参数（"x1,y1,x2,y2"）
        tmp = rect_coords.replace('，', ',')  # 防止有中文符号
        coords = tmp.split(',')
        x1 = float(coords[0])
        y1 = float(coords[1])
        x2 = float(coords[2])
        y2 = float(coords[3])
        where_builder.append(f"{x1} <= n.hzby <= {x2} and {y1} <= n.zzbx <= {y2}")

    # 完成主体深范围限制
    if wzjs_limit:
        # 处理wzjs_limit参数('min_wzjs, max_wzjs')
        tmp = wzjs_limit.replace('，', ',').replace(' ', '')  # 防止有中文符号
        wzjs = tmp.split(',')
        min_wzjs = wzjs[0]
        max_wzjs = wzjs[1]
        where_builder.append(f"{min_wzjs} <= n.wzjs <= {max_wzjs}")

    if where_builder:
        where_builder = "where "+" and ".join(where_builder)
        cypher_builder.append(where_builder)

    cypher_builder.append(f"return distinct(n)")
    cypher = " ".join(cypher_builder)
    print(f"cypher语句:{cypher}")
    data_nodes = neo4j.run(cypher)
    datas = [data_nodes[i]["n"] for i in range(len(data_nodes))]
    data_nodes = datas

    # 多边形限制条件
    if poly:
        # 先将poly格式处理成is_in_poly可执行的格式['a','b','c','d']->[[a,b],[c,d]]
        if type(poly) == str:
            tmp = poly.replace('，', ',')  # 防止有中文符号
            poly_coords = tmp.split(',')
            new_ploy = list()
            for i in range(0, len(poly_coords), 2):
                new_ploy.append([float(poly_coords[i]), float(poly_coords[i+1])])

        # 抽取点判断是否在多边形内部
        poly_datas = list()
        for temp in datas:
            x = temp['hzby']
            y = temp['zzbx']
            coords = [x, y]
            if is_in_poly(coords, new_ploy):
                poly_datas.append(temp)
        data_nodes = poly_datas

    # 开始日期限制条件
    if kzrq_limit:
        datas = list()
        for temp in data_nodes:
            neo4j_date = temp['kzrq']
            if is_in_date(kzrq_limit, neo4j_date):
                datas.append(temp)
        data_nodes = datas

    # 完成日期限制条件
    if wzrq_limit:
        datas = list()
        for temp in data_nodes:
            neo4j_date = temp['wzrq']
            if is_in_date(wzrq_limit, neo4j_date):
                datas.append(temp)
            data_nodes = datas
    print('结果：', data_nodes)

    if data_nodes:
        res = {
            "num": len(data_nodes),
            "fields": list(data_nodes[0].keys()),
            "datas": data_nodes,
        }
    else:
        res = {
            "num": 1,
            "fields": ['None'],
            # "datas": ['None'],
        }

    return Result(message="查询成功", data=res)

def get_djsd_by_layer(well_name: str, layers: dict):
    filters = deepcopy(layers)
    sql = f"select DJSD1, DJSD2 from geologdb.azf07"

    filters["FCLX"] = "解释"
    filters["JH"] = well_name
    conditions = [f"{k} = '{v}'" for k, v in filters.items()]
    conditions = " and ".join(conditions)
    sql += f" where {conditions}"
    sql += " order by DJSD1"

    res = oracle.run(sql)

    if not res:
        filters["FCLX"] = "现场"

        sql = f"select DJSD1, DJSD2 from geologdb.azf07"
        conditions = [f"{k} = '{v}'" for k, v in filters.items()]
        conditions = " and ".join(conditions)
        sql += f" where {conditions}"
        sql += " order by DJSD1"

        res = oracle.run(sql)

    if res:
        return res[0][0], res[-1][1]
    else:
        return None, None


if __name__ == "__main__":

    query_mdc_all_data('MDC')

import os
import xlrd
from neo4j.client import neo4j
from oracle.client import oracle


data_root = "D:\zstp\well_kg\data"


# -*----- 新增测主体解释 -------
wb = xlrd.open_workbook(os.path.join(data_root, "测主体解释.xls"), encoding_override="utf8")
table = wb.sheets()[0]
cjjs_dict = dict()
for i in range(1, table.nrows):
    cjjs_raw = table.cell_value(i, 0)
    cjjs = table.cell_value(i, 1)
    cjjs_dict[cjjs_raw] = cjjs


class Neo4jNode:
    """
    py2neo的操作有点复杂，为了方便操作Neo4j的节点，定义了Neo4j节点类
    """

    def __init__(self, labels=None, properties=None, primary_key=None):
        self.identity = None
        if not hasattr(self, "labels"):  # hasattr() 函数用于判断对象是否包含对应的属性。即self对象是否有‘lables'属性
            self.labels = labels
        if not hasattr(self, "properties"):  # 属性
            self.properties = properties
        if not hasattr(self, "primary_key"):  # 主键
            self.primary_key = primary_key

        self.find()

    @property
    def exist(self):
        """
        判断节点是否在Neo4j中存在
        """
        if self.identity:
            return True

        return self.find()

    def find(self):
        """
        根据labels的primary_key，尝试在neo4j中匹配该节点
        """
        cypher = f"match (n{self.parsed_labels}) where n.{self.primary_key} = '{self.primary_value}' return n"
        res = neo4j.run(cypher)
        if len(res) == 0:
            return False

        matched = res[0]["n"]
        self.identity = matched.identity
        self.properties = self.unwrap_properties(matched)
        return True

    def match_by_identity(self):
        """
        根据identity, 在neo4j中匹配节点，并获取properties
        """
        cypher = f"match (n) where id(n) = {self.identity} return n"
        res = neo4j.run(cypher)
        if len(res) == 0:
            return

        matched = res[0]["n"]
        self.properties = self.unwrap_properties(matched)

    @property
    def parsed_labels(self):
        """
        将节点的标签parse成cypher格式
        """
        return ":" + ":".join(self.labels)

    @property
    def parsed_properties(self):
        """
        将节点的属性值parse成cypher格式
        """
        each = list()
        if "WL02_2" in self.labels or "WL02_1" in self.labels:                # 针对特殊表进行处理
            for k, v in self.properties.items():
                if type(v) == str:
                    if k == "JSJLDM":
                        v = cjjs_dict[v]
                    else:
                        v = v.replace("\\", "$$").replace("'", "&&")
                    each.append("{}: '{}'".format(k, v))
                else:
                    each.append("{}: {}".format(k, v))
        else:
            for k, v in self.properties.items():
                if type(v) == str:
                    v = v.replace("\\", "$$").replace("'", "&&")
                    each.append("{}: '{}'".format(k, v))
                else:
                    each.append("{}: {}".format(k, v))
        return "{" + ", ".join(each) + "}"

    @property
    def primary_value(self):
        """
        获取节点的主键字段值，通过labels + 该值可以唯一确定一个节点
        """
        return self.properties[self.primary_key]

    def create(self):
        """
        在neo4j中新增该节点
        """
        cypher = f"create (n{self.parsed_labels} {self.parsed_properties}) return n"
        res = neo4j.run(cypher)[0]["n"]
        self.identity = res.identity

    def update(self):
        """
        更新节点属性
        """
        set_builder = [f"n.{k} = '{v}'" if type(v) == str else f"n.{k} = {v}" for k, v in self.properties.items()]
        set_builder = ",".join(set_builder)
        cypher = f"match (n{self.parsed_labels}) where n.{self.primary_key} = '{self.primary_value}' set {set_builder} return n"
        neo4j.run(cypher)

    def upsert(self):
        """
        如果存在则更新，否则创建
        """
        if self.exist:
            self.update()
        else:
            self.create()

    def link(self, end_node, r_type, r_properties=None):
        """
        已当前节点为头节点，end_node为尾节点，新增关系
        """
        relation_to_add = Neo4jRelation(r_type, r_properties)
        # print(relation_to_add)

        # 查询头尾节点之间已有的所有关系
        cypher = f"match (n)-[r:{r_type}]->(m) where id(n) = {self.identity} and id(m) = {end_node.identity} return r"
        res = neo4j.run(cypher)
        for relation_found in res:
            relation_found = relation_found["r"]
            relation_found = Neo4jRelation(r_type=r_type, r_properties={k: v for k, v in relation_found.items()})
            if relation_found == relation_to_add:
                # 如果关系的类型相同且属性值也完全相同，说明该关系已经存在了，不应该重复添加
                return False

        # 添加关系  MATCH (a:AZS04_1),(b:Well) where a.name='营11' AND b.name='营11' create (a)-[r:relation]->(b) return a,r
        cypher = f"match (n), (m) where id(n) = {self.identity} and id(m) = {end_node.identity} " \
                 f"create (n)-[r:{relation_to_add.r_type} {relation_to_add.parsed_properties}]->(m)"
        neo4j.run(cypher)
        return True

    def unwrap_properties(self, properties):
        res = dict()
        for k, v in properties.items():
            if type(v) == str:
                res[k] = v.replace("$$", "\\").replace("&&", "'")
            else:
                res[k] = v

        return res


class Neo4jRelation:
    def __init__(self, r_type, r_properties):
        self.r_type = r_type
        self.properties = r_properties

    def __eq__(self, other):
        return self.r_type == other.r_type and self.properties == other.properties

    @property
    def parsed_properties(self):
        if not self.properties:
            return ""

        each = list()
        for k, v in self.properties.items():
            if type(v) == str:
                each.append("{}: '{}'".format(k, v))
            else:
                each.append("{}: {}".format(k, v))
        return "{" + ", ".join(each) + "}"


class TableInfoNode(Neo4jNode):
    def __init__(self, table_name, table_common_name=None, user_space=None, primary_keys=None, unique_keys=None):
        if table_name == "V_AZ01":   # V_AZ01表信息
            table_common_name = "主体数据表"  # oracle中文表名
            primary_keys = ["JH"]  # 主键
            unique_keys = ["JH"]  # 唯一键
            user_space = "ktrj"  # 用户空间

        primary_keys = primary_keys or list()  # 默认为[]，否则primary_keys
        unique_keys = unique_keys or list()  # 默认为[]，否则primary_keys
        self.ordered_fields = oracle.get_table_fields(table_name, user_space)  # eg.获取数据库V_AZ01表列名['DW',...,'GXRQ','SYSDATE']
        self.labels = ["TableInfo"]  # neo4j中标签
        self.primary_key = "table_name"  # neo4j中主键
        self.properties = {  # neo4j中节点属性
            "table_name": table_name,
            "table_common_name": table_common_name,
            "user_space": user_space,
            "fields": self.ordered_fields,
            "primary_keys": primary_keys,
            "unique_keys": unique_keys
        }

        # 若查找的表的列名中存在DJSD1和DJSD2，则在Neo4j中添加HRange表，其属性增添DJSD1和DJSD2
        if "DJSD1" in self.ordered_fields and "DJSD2" in self.ordered_fields:
            self.labels.append("HRange")
            if not ("DJSD1" in self.properties["primary_keys"]):
                self.properties["primary_keys"].append("DJSD1")
            if not ("DJSD2" in self.properties["primary_keys"]):
                self.properties["primary_keys"].append("DJSD2")

        super().__init__()

    @property
    def table_name(self):
        return self.primary_value

    @property
    def table_primary_keys(self):
        return self.properties["primary_keys"]

    @property
    def table_unique_keys(self):
        if not ("unique_keys" in self.properties):
            print(self.table_name + "无唯一键")
            print(self.properties["primary_keys"])
            return self.properties["primary_keys"]
        return self.properties["unique_keys"]

    @property
    def table_query_name(self):
        if self.properties["user_space"]:
            return self.properties["user_space"] + "." + self.table_name
        else:
            return self.table_name

    @property
    def table_field_names(self):
        return self.properties["fields"]


class WellNode(Neo4jNode):
    def __init__(self, well_name):
        self.labels = ["Well"]
        self.primary_key = "name"
        self.properties = {
            "name": well_name,
        }

        super().__init__()


class DataNode(Neo4jNode):
    def __init__(self, name, primary_values=None, is_well=False, from_neo4j_identity=None, from_neo4j_properties=None):
        self.is_well = is_well
        if is_well:  # 判断是否是主体
            self.labels = ["Well"]
            self.primary_key = "name"
            self.properties = {
                "name": name,
            }
            super().__init__()
        else:
            self.identity = from_neo4j_identity  # 每个实体都有ID(Identity)唯一标识,
            self.table_node = GetTableInfoNode(name)
            self.labels = ["DATA", self.table_node.table_name]
            self.properties = dict()

            if from_neo4j_properties is not None:
                self.properties = from_neo4j_properties
            elif from_neo4j_identity is not None:
                self.match_by_identity()
            else:
                for i in range(len(self.table_node.table_primary_keys)):
                    value = "None" if primary_values[i] is None else primary_values[i]
                    if type(value) == str:
                        if self.table_node.table_primary_keys[i] == "JH":
                            value = value.strip()     # 主体号去空格处理
                        value = value.replace("\\", "/").replace("'", "")
                    self.properties[self.table_node.table_primary_keys[i]] = value

    def equals(self, other):
        if self.table_node.table_name != other.table_node.table_name:
            return False
        # 只判断唯一键中的数据是否相等即可
        for key in self.table_node.table_unique_keys:
            if self.properties[key] != other.properties[key]:
                break
        else:
            return True

        return False


TableInfoNodeCache = dict()


def GetTableInfoNode(table_name) -> TableInfoNode:  # 返回TableInfoNode类型
    if table_name not in TableInfoNodeCache:
        TableInfoNodeCache[table_name] = TableInfoNode(table_name)  # TableInfoNodeCache存放不重复的TableInfoNode（标签、主键、属性）

    return TableInfoNodeCache[table_name]



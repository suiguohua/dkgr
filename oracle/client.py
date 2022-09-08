import sys
import os
import traceback
import datetime
from typing import List
import configparser
import cx_Oracle
from retry import retry
from server.logger import log_info
import time


class OracleClient:
    def __init__(self):
        self.client = None
        self.config_path = os.path.join(os.path.abspath(os.path.dirname(__file__) + os.path.sep + ".."),
                                        "config.ini")

    def connect(self):
        print("连接Oracle...")
        try:
            config = configparser.ConfigParser()
            config.read(self.config_path, encoding="utf8")

            oracle_user = config.get("oracle", "user")
            oracle_passport = config.get("oracle", "passport")
            oracle_host = config.get("oracle", "host")
            oracle_service_name = config.get("oracle", "service_name")

            tns = "{}/{}@{}/{}".format(oracle_user,
                                       oracle_passport,
                                       oracle_host,
                                       oracle_service_name)
            self.client = cx_Oracle.connect(tns)
            print("Oracle连接成功")
        except Exception:
            print("Oracle连接失败，请检查oracle是否正确启动，config.ini中是否配置正确")
            print("错误堆栈: {}".format(traceback.format_exc()))
            sys.exit()

    @retry(tries=3, delay=1)
    def run(self, query, flask_app=None):
        log_info(f"execute sql: {query}", flask_app)
        s = time.time()
        # 专门针对“ora-29275: 部分多字节字符”错误，做的修改
        if "geologdb_wj.AZY11" in query:
            query = query.replace("CJCYX", "to_nchar(CJCYX) as CJCYX")
        if "geologdb_wj.AZY05" in query:
            query = query.replace("YX", "to_nchar(YX) as YX")
            # fields.remove("JH")
            # fields.append("trim(JH) as JH")

        cur = self.client.cursor()
        cur.execute(query)
        result = cur.fetchall()
        cur.close()

        elapsed_in_ms = round((time.time() - s) * 1000, 3)
        log_info(f"sql execute success, {len(result)} items fetched after {elapsed_in_ms} ms", flask_app)

        res = list()
        for item in result:
            item = list(item)
            for i in range(len(item)):
                if type(item[i]) == datetime.datetime:
                    item[i] = item[i].strftime('%Y-%m-%d %H:%M:%S')
                elif type(item[i]) == cx_Oracle.LOB:
                    # 字符型大对象
                    s = item[i].read()
                    s = s if type(s) == str else str(s, encoding='utf-8')
                    item[i] = s
            res.append(item)
        return res

    def get_table_fields(self, table_name, user_space=None):
        sql = "SELECT distinct column_name, column_id " \
              "FROM all_tab_columns " \
              f"WHERE table_name = upper('{table_name}')"

        if user_space:
            sql += f" and owner = upper('{user_space}')"

        sql += " ORDER BY column_id"
        return [field[0] for field in self.run(sql)]  # 获取数据库表列名 ['DW', 'JH', 'JHDM', 'JBDM',..., 'GXRQ', 'SYSDATE']

    # sql语句1/多个条件 通用写法
    def query(self, fields: List[str], table: str, conditions: dict):  # ["JH"], "V_AZ01", {‘JH’: '义47'}
        fields = ",".join(fields) if fields else '*'  # JH
        sql = f"select {fields} from {table}"  # "select JH from V_AZ01"

        if conditions:
            filters = list()
            for k, v in conditions.items():
                if type(v) == str:
                    try:
                        datetime.datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
                        f = f"{k} = to_date('{v}','yyyy-mm-dd hh24:mi:ss')"  # ValueError: time data '义47' does not match format '%Y-%m-%d %H:%M:%S'
                    except ValueError:
                        if k.upper() == "JH":
                            f = f"trim({k}) = '{v.strip()}'"  # "trim(JH) = '义47'"
                        else:
                            f = f"{k} = '{v}'"
                else:
                    f = f"{k} = {v}"
                filters.append(f)

            parsed_filters = " and ".join(filters)
            sql += f" where {parsed_filters}"  # "select JH from V_AZ01 where trim(JH) = '义47'"
            # print("sql语句：{}".format(sql))
        # ----------- AZS04_1主体特殊处理 -----------
        special_table = ['geologdb_wj.AZS04_1', 'geologdb_wj.AZY01']
        order_data = ""
        if table in special_table:
            # 表的key不同，需要分别处理
            if fields.find('CDSD') > 0:  # 找到则返回索引，找不到返回-1
                order_data = "CDSD"
            elif fields.find('JS') > 0:
                order_data = 'JS'
            sql += f" order by {order_data} ASC"
            # print("sql语句：{}".format(sql))
        return self.run(sql)


oracle = OracleClient()
oracle.connect()

if __name__ == "__main__":
    # resl = oracle.get_table_fields("V_AZ01", "ktrj")
    oresut = oracle.query(['JH', 'JS', 'QT'], 'geologdb_wj.AZY01', {'JH': 'zhaung6'})

    print(oresut)
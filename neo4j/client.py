import sys
import traceback
import os
import py2neo
import configparser
from retry import retry
from server.logger import log_info
import time


class Neo4jClient:
    def __init__(self):
        self.client = None
        self.config_path = os.path.join(os.path.abspath(os.path.dirname(__file__) + os.path.sep + ".."),
                                        "config.ini")

    def connect(self):
        print("连接Neo4j...")
        try:
            config = configparser.ConfigParser()
            config.read(self.config_path, encoding="utf8")

            neo4j_host = config.get("neo4j", "host")
            neo4j_user = config.get("neo4j", "user")
            neo4j_passport = config.get("neo4j", "passport")
            self.client = py2neo.Graph(neo4j_host, auth=(neo4j_user, neo4j_passport))
            print("Neo4j连接成功")
        except py2neo.Neo4jError:
            print("Neo4j连接失败，请检查neo4j是否正确启动，config.ini中是否配置正确")
            print("错误堆栈: {}".format(traceback.format_exc()))
            sys.exit()

    # py2neo在运行间隔非常短的两条cypher时可能会偶先报错，这里间隔1秒retry3次
    @retry(tries=3, delay=1)
    def run(self, cypher, flask_app=None):
        log_info(f"execute cypher: {cypher}", flask_app)
        s = time.time()
        res = self.client.run(cypher).data()
        elapsed_in_ms = round((time.time() - s) * 1000, 3)
        log_info(f"cypher execute success, {len(res)} items fetched after {elapsed_in_ms} ms", flask_app)
        return res

    def clear(self):
        cypher_del_relations = "match ()-[r]->() delete r"
        self.run(cypher_del_relations)

        cypher_del_nodes = "match (n) delete n"
        self.run(cypher_del_nodes)


neo4j = Neo4jClient()
neo4j.connect()

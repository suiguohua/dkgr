import sys
import os
import configparser
from wsgiref.simple_server import make_server


def error_and_exit(msg="运行错误"):
    sys.stderr.write(msg + "\n")
    sys.stderr.write("支持的指令: {}\n".format(str(Manager.SupportCommands)))
    sys.exit()


class Manager:
    SupportCommands = ["runserver", "well2neo4j", "admin", "update_data", "update_wells"]
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), "config.ini"), encoding="utf8")

    @staticmethod
    def runserver():
        import os
        import time
        import logging
        from server.app import app

        log_path = Manager.config.get("server", "log_path")
        if not os.path.isabs(log_path):
            log_path = os.path.join(os.path.curdir, log_path)
        print("日志路径: {}".format(os.path.abspath(log_path)))

        logging.basicConfig(level=logging.DEBUG)
        now = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime(int(time.time())))
        log_path = os.path.join(log_path, "server_{}.log".format(now))

        handler = logging.FileHandler(log_path, encoding='utf8')
        logging_format = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(lineno)s - %(message)s')
        handler.setFormatter(logging_format)
        app.logger.addHandler(handler)
        server = make_server("0.0.0.0", 5000, app)
        print("服务器启动成功！")
        server.serve_forever()

    @staticmethod
    def well2neo4j():
        data_path = Manager.config.get("server", "data_path")
        if not os.path.isabs(data_path):
            data_path = os.path.join(os.path.curdir, data_path)
        print("数据根路径: {}".format(os.path.abspath(data_path)))

        from data_extraction.well2neo4j import well_data_extraction
        well_data_extraction(data_path)

    @staticmethod
    def admin():
        server_host = Manager.config.get("server", "host")
        server_port = Manager.config.get("server", "port")

        import requests
        try:
            requests.get(f"http://{server_host}:{server_port}/ping")
            print("服务器连接成功")
        except requests.exceptions.RequestException:
            print("系统服务器没有响应，请检查服务器是否正确启动，且在config.ini中正确配置了服务器地址")
            return

        from qt.app import run_admin
        run_admin()

    @staticmethod
    def update_data():
        from data_extraction.data_extractor import data_extractor
        data_extractor.update_all_existing_data()

    @staticmethod
    def update_wells():
        from data_extraction.data_extractor import data_extractor
        data_extractor.update_wells()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        error_and_exit("缺少运行指令，请检查运行命令")

    if len(sys.argv) > 2:
        error_and_exit("运行指令过多，请检查运行命令")

    run_command = sys.argv[1]
    run_func = getattr(Manager, run_command, None)

    if run_func is None:
        error_and_exit("不支持的运行指令，请检查运行命令")

    run_func()

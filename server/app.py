from flask import Flask, request
from server.utils import interface_get, interface_post
import server.recommend as recommend_module
import server.data as data_module

app = Flask(__name__)


@app.route("/ping", methods=["GET"])
def ping():
    return "pong"


# 数据模块接口
@app.route("/data/table_list", methods=["GET"])
@interface_get
def table_list():
    res = data_module.table_list()
    return res


@app.route("/data/query/with_depth", methods=["GET"])
@interface_get
def query_by_depth():
    table_name = request.args.get("table", None)
    well_name = request.args.get("well", None)
    filter_min_depth = request.args.get("djsd1", None)
    filter_max_depth = request.args.get("djsd2", None)
    from_table = request.args.get("from", None)

    res = data_module.query_data_by_well_name_and_table_name_and_depth(well_name,
                                                                       table_name,
                                                                       filter_min_depth,
                                                                       filter_max_depth,
                                                                       from_table)
    return res


@app.route("/data/query/with_layer", methods=["GET"])
@interface_get
def query_by_layer():
    table_name = request.args.get("table", None)
    well_name = request.args.get("well", None)
    from_table = request.args.get("from", None)

    layers_support = ("JIE", "XI", "TONG", "ZU", "DUAN", "YD")

    layers = {layer: request.args.get(layer) for layer in layers_support if layer in request.args}
    res = data_module.query_data_by_well_name_and_table_name_and_layers(well_name,
                                                                        table_name,
                                                                        layers,
                                                                        from_table)
    return res



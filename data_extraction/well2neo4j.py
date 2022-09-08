
import os
import re
import pickle
import sys
import xlrd
from tqdm import tqdm
from neo4j.client import neo4j


data_root = ""


def process_raw_data(data):
    return str(data).strip().replace(' ', '').replace('(', '（').replace(')', '）').replace('\t', '')


# class Well:
#     def __init__(self, idx, jh, jb, jx, ktxm, ztmd, dlwz, yqt, zzbx, hzby, zymdc, kzrq, wzrq, wzjs, wzcw):
#         self.idx = idx
#         self.jh = jh
#         self.jb = jb_dict.get(process_raw_data(jb), ["未知主体别"])
#         self.jx = jx_dict.get(process_raw_data(jx), ["未知主体型"])
#         self.ktxm = ktxm.replace('"', "'") if ktxm != "None" and ktxm is not None else "未记录"
#         self.ztmd = ztmd.replace('"', "'") if ztmd != "None" and ztmd is not None else "未记录目的"
#         self.dlwz = dlwz.replace('"', "'") if dlwz != "None" and dlwz is not None else "未记录位置"
#         self.zzbx = float(re.sub("[^0-9.]", "", zzbx))
#         self.hzby = float(re.sub("[^0-9.]", "", hzby))
#         self.zymdc = mdc_dict.get(process_raw_data(zymdc), ["未知目的层"])
#         self.wzjs = float(re.sub("[^0-9.]", "", wzjs)) if wzjs != "None" and wzjs is not None else 0

# ------------------------------------- 修改处 无id----------------------------------------------
class Well:
    def __init__(self, jh, jb, jx, ktxm, ztmd, dlwz, yqt, zzbx, hzby, zymdc, kzrq, wzrq, wzjs, wzcw):
        self.jh = jh
        self.jb = jb_dict.get(process_raw_data(jb), ["未知主体别"])
        self.jx = jx_dict.get(process_raw_data(jx), ["未知主体型"])
        self.ktxm = ktxm.replace('"', "'") if ktxm != "None" and ktxm is not None else "未记录"
        self.ztmd = ztmd.replace('"', "'") if ztmd != "None" and ztmd is not None else "未记录目的"
        self.dlwz = dlwz.replace('"', "'") if dlwz != "None" and dlwz is not None else "未记位置"
        self.yqt = yqt_dict.get(process_raw_data(yqt), "未知")
        self.zzbx = float(re.sub("[^0-9.]", "", zzbx))
        self.hzby = float(re.sub("[^0-9.]", "", hzby))
        self.zymdc = mdc_dict.get(process_raw_data(zymdc), ["未知目的层"])
        self.kzrq = kzrq if kzrq != "None" and kzrq is not None else "未记录开始日期"  
        self.wzrq = wzrq if wzrq != "None" and wzrq is not None else "未记录完成日期"  
        self.wzjs = float(wzjs) if wzjs != "None" and wzjs is not None else 0
        self.wzcw = wzcw.replace('"', "'") if wzcw != "None" and wzcw is not None else "未记录完成层位"
# ------------------------------------------------------------------------------------------------

# class MDC:
#     def __init__(self, idx, name, level, abbr):
#         self.idx = idx
#         self.name = name
#         self.level = level
#         self.abbr = abbr
#
#
# class LITHO:
#     def __init__(self, idx, name, level):
#         self.idx = idx
#         self.name = name
#         self.level = level
#
#
# class SYJG:
#     def __init__(self, idx, name, level):
#         self.idx = idx
#         self.name = name
#         self.level = level

# --------------- 修改处 无id ---------------
class MDC:
    def __init__(self, name, level, abbr):
        self.name = name
        self.level = level
        self.abbr = abbr


class LITHO:
    def __init__(self, name, level):
        self.name = name
        self.level = level


class SYJG:
    def __init__(self, name, level):
        self.name = name
        self.level = level
# ---------------------------------------


wb = xlrd.open_workbook(os.path.join(data_root, "主体别.xls"), encoding_override="utf8")
table = wb.sheets()[0]
jb_dict = dict()
jb_set = set()

for i in range(1, table.nrows):
    jb_raw = table.cell_value(i, 0)
    jbs = table.cell_value(i, 1).split(";")
    jb_dict[jb_raw] = jbs
    for jb in jbs:
        if jb != "未知主体别":
            jb_set.add(jb)

wb = xlrd.open_workbook(os.path.join(data_root, "主体型.xls"), encoding_override="utf8")
table = wb.sheets()[0]
jx_dict = dict()
jx_set = set()
for i in range(1, table.nrows):
    jx_raw = table.cell_value(i, 0)
    jxs = table.cell_value(i, 1).split(";")
    jx_dict[jx_raw] = jxs
    for jx in jxs:
        if jx != "未知主体型":
            jx_set.add(jx)

wb = xlrd.open_workbook(os.path.join(data_root, "产物区.xls"), encoding_override="utf8")
table = wb.sheets()[0]
yqt_dict = dict()
yqt_set = set()
for i in range(1, table.nrows):
    yqt_raw = table.cell_value(i, 0)
    yqt = table.cell_value(i, 1)
    yqt_dict[yqt_raw] = yqt
    if yqt != "未知产物区":
        yqt_set.add(yqt)

wb = xlrd.open_workbook(os.path.join(data_root, "目的层实体.xls"), encoding_override="utf8")
mdc_table = wb.sheets()[0]
mdc_relation_table = wb.sheets()[1]

mdcs = list()
for i in range(mdc_table.nrows):
    # mdcs.append(MDC(i, mdc_table.cell_value(i, 0), mdc_table.cell_value(i, 1), mdc_table.cell_value(i, 2)))
    #  -------------------------- 修改处 无id -----------------------------
    mdcs.append(MDC(mdc_table.cell_value(i, 0), mdc_table.cell_value(i, 1), mdc_table.cell_value(i, 2)))

mdc_relations = list()
for i in range(mdc_relation_table.nrows):
    mdc_relations.append((mdc_relation_table.cell_value(i, 0), mdc_relation_table.cell_value(i, 1)))

wb = xlrd.open_workbook(os.path.join(data_root, "目的层.xls"), encoding_override="utf8")
table = wb.sheets()[0]
mdc_dict = dict()
for i in range(1, table.nrows):
    mdc_raw = table.cell_value(i, 0)
    mdc = table.cell_value(i, 1).split(";")
    mdc_dict[mdc_raw] = mdc

# -*----- 新增测主体解释 -------
wb = xlrd.open_workbook(os.path.join(data_root, "测主体解释.xls"), encoding_override="utf8")
table = wb.sheets()[0]
cjjs_dict = dict()
for i in range(1, table.nrows):
    cjjs_raw = table.cell_value(i, 0)
    cjjs = table.cell_value(i, 1).split(";")
    cjjs_dict[cjjs_raw] = cjjs

# -*----- 新增杂质实体 -------
wb = xlrd.open_workbook(os.path.join(data_root, "杂质实体.xls"), encoding_override="utf8")  # 打开当前路径下名为“杂质实体”的excel
litho_table = wb.sheets()[0]  # 读取sheet1
litho_relation_table = wb.sheets()[1]  # 读取sheet2

lithos = list()  # 创建空列表 lithos
for i in range(litho_table.nrows):
    # lithos.append(LITHO(i, litho_table.cell_value(i, 0), litho_table.cell_value(i, 1)))  # 将表格sheet1内容添加到lithos中
    #  -------------------------- 修改处 无id -----------------------------
    lithos.append(LITHO(litho_table.cell_value(i, 0), litho_table.cell_value(i, 1)))

litho_relations = list()
for i in range(litho_relation_table.nrows):
    litho_relations.append((litho_relation_table.cell_value(i, 0),
                            litho_relation_table.cell_value(i, 1)))  # 将表格sheet2内容添加到litho_relations中
# -------------------------

wb = xlrd.open_workbook(os.path.join(data_root, "含产物实体.xls"), encoding_override="utf8")
syjg_table = wb.sheets()[0]

syjgs = list()
for i in range(syjg_table.nrows):
    # syjgs.append(SYJG(i, process_raw_data(syjg_table.cell_value(i, 0)), str(int(syjg_table.cell_value(i, 1)))))
    #  -------------------------- 修改处 无id -----------------------------
    syjgs.append(SYJG(process_raw_data(syjg_table.cell_value(i, 0)), syjg_table.cell_value(i, 1)))


def add_new_well_to_neo4j(well: Well):
    # 创建主体节点
    # cypher = 'merge (n:Well {{name:"{1}"}}) set n+= {{id:{0}, name:"{1}", jh:"{1}", ztmd:"{2}", dlwz:"{3}", ' \
    #          'zzbx:{4}, hzby:{5}, ktxm:"{6}", kzrq:"{7}", wzrq:"{8}", wzjs:{9}, wzcw:"{10}"}}'.format(
    #     well.idx,
    #     well.jh,
    #     well.ztmd,
    #     well.dlwz,
    #     well.zzbx,
    #     well.hzby,
    #     well.ktxm,
    #     well.kzrq,
    #     well.wzrq,
    #     well.wzjs,
    #     well.wzcw)

    # ------------------------- 修改处 无id----------------------
    cypher = 'merge (n:Well {{name:"{0}"}}) set n+= {{name:"{0}", jh:"{0}", ztmd:"{1}", dlwz:"{2}", ' \
             'zzbx:{3}, hzby:{4}, ktxm:"{5}", kzrq:"{6}", wzrq:"{7}", wzjs:{8}, wzcw:"{9}"}}'.format(
        well.jh,
        well.ztmd,
        well.dlwz,
        well.zzbx,
        well.hzby,
        well.ktxm,
        well.kzrq,
        well.wzrq,
        well.wzjs,
        well.wzcw)
    # -----------------------------------------------------

    neo4j.run(cypher)

    # 连接 主体->主体别
    if not (len(well.jb) == 1 and well.jb[0] == "未知主体别"):
        for jb in well.jb:
            cypher = 'match (n:Well), (m:JB) where n.name="{}" and m.name="{}" merge (n)-[:jb]->(m)'.format(
                well.jh,
                jb)
            neo4j.run(cypher)

    # 连接 主体->主体型
    if not (len(well.jx) == 1 and well.jx[0] == "未知主体型"):
        for jx in well.jx:
            cypher = 'match (n:Well), (m:JX) where n.name="{}" and m.name="{}" merge (n)-[:jx]->(m)'.format(
                well.jh,
                jx)
            neo4j.run(cypher)

    # 连接主体->产物区
    if not well.yqt == "未知产物区":
        cypher = 'match (n:Well), (m:YQT) where n.name="{}" and m.name="{}" merge (n)-[:yqt]->(m)'.format(
            well.jh, well.yqt)
        neo4j.run(cypher)

    # 连接主体->目的层
    if not (len(well.zymdc) == 1 and well.zymdc[0] == "未知目的层"):
        for zymdc in well.zymdc:
            cypher = 'match (n:Well), (m:MDC) where n.name="{}" and m.name="{}" merge (n)-[:zymdc]->(m)'.format(
                well.jh,
                zymdc)
            neo4j.run(cypher)


def add_ass01_syjg_cw_relations():
    cypher = f"MATCH (n:ASS01) RETURN n"
    nodes = neo4j.run(cypher)
    total = len(nodes)
    count = 0
    added = 0
    for node in nodes:
        lst = [j for ii, j in node.items()]
        count += 1
        print(f"\r当前更新进度: {count} / {total}", end="")
        syjbid = lst[0]['SYJBID']
        syjg = process_raw_data(lst[0]['SYJG'])
        cw = mdc_dict.get(process_raw_data(lst[0]['CW']), ["未知"])[0]
        cypher = 'match (n:ASS01), (m:SYJG) where n.SYJBID="{}" and m.name="{}" and m.level=~"s.*" ' \
                 'merge (n)-[:syjg]->(m)'.format(syjbid, syjg)
        neo4j.run(cypher)
        if not (cw == "未知"):
            cypher = 'match (n:ASS01), (m:MDC) where n.SYJBID="{}" and m.name="{}" ' \
                     'merge (n)-[:sycw]->(m)'.format(syjbid, cw)
            neo4j.run(cypher)
        added += 2

    print(f"\n试验成果数据试验结果、层位关系创建完成，共向图谱中更新关系{added}个")


def add_azf09_zhjs_cw_relations():
    cypher = f"MATCH (n:AZF09) RETURN n"
    nodes = neo4j.run(cypher)
    total = len(nodes)
    count = 0
    added = 0
    for node in nodes:
        lst = [j for ii, j in node.items()]
        count += 1
        print(f"\r当前更新进度: {count} / {total}", end="")
        jh = lst[0]['JH']
        xh = lst[0]['XH']
        zhjs = process_raw_data(lst[0]['ZHJS'])
        cw = mdc_dict.get(process_raw_data(lst[0]['CW']), ["未知"])[0]
        cypher = 'match (n:AZF09), (m:SYJG) where n.JH="{}" and n.XH= {} and m.name="{}" and m.level=~"l.*" ' \
                 'merge (n)-[:zhjs]->(m)'.format(jh, xh, zhjs)
        neo4j.run(cypher)
        if not (cw == "未知"):
            cypher = 'match (n:AZF09), (m:MDC) where n.JH="{}" and n.XH= {} and m.name="{}" ' \
                     'merge (n)-[:zhjscw]->(m)'.format(jh, xh, cw)
            neo4j.run(cypher)
        added += 2

    print(f"\n产物综合解释数据解释结果、层位关系创建完成，共向图谱中更新关系{added}个")


def add_wl02_1_cjjs_relations():
    cypher = f"MATCH (n:WL02_1) RETURN n"
    nodes = neo4j.run(cypher)
    total = len(nodes)
    count = 0
    added = 0
    for node in nodes:
        lst = [j for ii, j in node.items()]
        count += 1
        print(f"\r当前更新进度: {count} / {total}", end="")
        pjid = lst[0]['PJID']
        ch = lst[0]['CH']
        jsjl = cjjs_dict.get(process_raw_data(lst[0]['JSJLDM']), ["未知"])[0]
        cypher = "merge (n:WL02_1 {{PJID:'{0}', CH:'{1}'}}) set n+={{JSJL:'{2}'}}".format(pjid, ch, jsjl)
        neo4j.run(cypher)
        if not (jsjl == "未知"):
            cypher = 'match (n:WL02_1), (m:SYJG) where n.PJID="{0}" and n.CH= "{1}" and m.name="{2}" ' \
                     'and m.level=~"c.*" merge (n)-[:jsjl]->(m)'.format(pjid, ch, jsjl)
            neo4j.run(cypher)
            added += 1

    print(f"\n测主体解释数据解释结果关系创建完成，共向图谱中更新关系{added}个")


def well_data_extraction(data_root):
    print("数据预处理...")

    # with open(os.path.join(data_root, "well.pickle"), "rb") as f:
    #     lines = pickle.load(f)

    # ----------- 从oracle中读取数据 无id------------
    from oracle.client import oracle

    query_fields = ['JH', 'JB', 'JX', 'KTXM', 'ZTMD', 'DLWZ', 'YQT', 'ZZBX', 'HZBY', 'ZYMDC', 'KZRQ', 'WZRQ', 'WZJS',
                    'WZCW']
    # jh, jb, jx, ktxm, ztmd, dlwz, yqt, zzbx, hzby, zymdc, kzrq, wzrq, wzjs, wzcw
    oracle_data = oracle.query(query_fields, "V_AZ01", None)
    lines = oracle_data
    # -----------------------------------------

    # id_idx = lines[0].index("ID")
    # jh_idx = lines[0].index("JH")
    # print(jh_idx)
    # jb_idx = lines[0].index("JB")
    # jx_idx = lines[0].index("JX")
    # ktxm_idx = lines[0].index("KTXM")
    # ztmd_idx = lines[0].index("ZTMD")
    # dlwz_idx = lines[0].index("DLWZ")
    # yqt_idx = lines[0].index("YQT")
    # zzbx_idx = lines[0].index("ZZBX")
    # hzby_idx = lines[0].index("HZBY")
    # zymdc_idx = lines[0].index("ZYMDC")
    # kzrq_idx = lines[0].index("KZRQ")
    # wzrq_idx = lines[0].index("WZRQ")
    # wzjs_idx = lines[0].index("WZJS")
    # wzcw_idx = lines[0].index("WZCW")

    # ----------- 修改部分 无id--------------
    jh_idx = 0
    jb_idx = 1
    jx_idx = 2
    ktxm_idx = 3
    ztmd_idx = 4
    dlwz_idx = 5
    yqt_idx = 6
    zzbx_idx = 7
    hzby_idx = 8
    zymdc_idx = 9
    kzrq_idx = 10
    wzrq_idx = 11
    wzjs_idx = 12
    wzcw_idx = 13
    # ----------------------------------

    # wells = [Well(well[id_idx],
    #               well[jh_idx],
    #               well[jb_idx],
    #               well[jx_idx],
    #               well[ktxm_idx],
    #               well[ztmd_idx],
    #               well[dlwz_idx],
    #               well[yqt_idx],
    #               well[zzbx_idx],
    #               well[hzby_idx],
    #               well[zymdc_idx],
    #               well[kzrq_idx],
    #               well[wzrq_idx],
    #               well[wzjs_idx],
    #               well[wzcw_idx]) for well in lines[1:]]

    # ---------------- 修改部分 无id-----------------------
    # jh, jb, jx, ktxm, ztmd, dlwz, yqt, zzbx, hzby, zymdc, kzrq, wzrq, wzjs, wzcw
    # 存储唯一主体操作
    exist_well = list()
    wells = list()
    for well in lines:
        if well[jh_idx] in exist_well:  # 查看已存的主体中有没有新添的主体号，有就跳过去，没有就添加。
            continue
        else:
            wells.append(Well(well[jh_idx],
                              well[jb_idx],
                              well[jx_idx],
                              well[ktxm_idx],
                              well[ztmd_idx],
                              well[dlwz_idx],
                              well[yqt_idx],
                              well[zzbx_idx],
                              well[hzby_idx],
                              well[zymdc_idx],
                              well[kzrq_idx],
                              well[wzrq_idx],
                              well[wzjs_idx],
                              well[wzcw_idx]))
            exist_well.append(well[jh_idx])

    # wells.wzcw = wells.wzcw[0]

    # ---------------------------------------------

    # # ********** 更新主体数据 ***************
    # with open(os.path.join(data_root, "well_cache.pickle"), "rb") as f:  # well.pickle文件里存放序列化文件，源文件是well.xls
    #     cache_data = pickle.load(f)  # 读取pickle类型文件
    # temp = list(cache_data.values())
    # # print("类的属性：", hasattr(temp[0], 'ktxm'))
    # # print("cache主体号：", temp[0].jh)
    # # print("cache 领域:", temp[0])
    # # print("cache长度：", len(temp))
    # for i in range(len(wells), len(temp)):
    #     # print("i = ", i)
    #     temp[i].ktxm = None
    #     # print("领域： ", temp[i].ktxm)
    #     wells.append(temp[i])
    #     # print(f"wells {i} : {wells[i]}")
    #     # print(f"wells 领域 {i} : {wells[i].ktxm}")
    # # ***********************************

    print("数据预处理完成，请确保当前Neo4j数据库的状态适合进行后续操作（数据库是否正确选择，数据库中是否有脏数据等）")
    print("指令说明:\n"
          "data-确认当前Neo4j中数据\n"
          "clear-清空当前Neo4j中数据\n"
          "start-开始向Neo4j中增加实体\n"
          "exit-退出")
    while True:
        command = input("请输入指令:").lower()
        if command == "data":
            cypher = "match (n:Well) return count(n)"
            res = neo4j.run(cypher)[0]["count(n)"]
            print("当前Neo4j中存在{}个主体实体".format(res))
        elif command == "clear":
            if input("确认清空？（此操作将删除所有实体和关系，且不可恢复）[yes/no]").lower() == "yes":
                neo4j.clear()
                print("Neo4j已清空")
        elif command == "start":
            break
        elif command == "exit":
            print("取消增加实体")
            sys.exit()

    print("开始向Neo4j中增加实体，请勿中途退出程序......")
    cur_step = 0
    total_step = 14

    def start_step(msg):
        nonlocal cur_step, total_step
        cur_step += 1
        print("Step {}/{}, {}......".format(cur_step, total_step, msg))

    # 新增试验结果实体
    start_step("新增试验结果实体")
    for syjg in tqdm(syjgs, ascii=True):
        cypher = "merge (n:SYJG {{name:'{0}', level:'{1}'}}) set n+={{name:'{0}', level:'{1}'}}".format(syjg.name, syjg.level)
        neo4j.run(cypher)

    # 新增主体别实体
    start_step("新增主体别实体")
    # neo4j.run('CREATE CONSTRAINT ON(n:JB) ASSERT n.name IS UNIQUE')
    for jb in tqdm(jb_set, ascii=True):
        # cypher = "create (n:JB {{name:'{}'}})".format(jb)
        cypher = "merge (n:JB {{name:'{0}'}}) set n+={{name:'{0}'}}".format(jb)
        neo4j.run(cypher)

    # 新增主体型实体
    start_step("新增主体型实体")
    for jx in tqdm(jx_set, ascii=True):
        cypher = "merge (n:JX {{name:'{0}'}}) set n+={{name:'{0}'}}".format(jx)
        neo4j.run(cypher)

    # 新增产物区实体
    start_step("新增产物区实体")
    for yqt in tqdm(yqt_set, ascii=True):
        cypher = "merge (n:YQT {{name:'{0}'}}) set n+={{name:'{0}'}}".format(yqt)
        neo4j.run(cypher)

    # 新增目的层实体 & 关系
    start_step("新增目的层实体")
    for mdc in tqdm(mdcs, ascii=True):
        cypher = "merge (n:MDC {{name:'{0}'}}) set n+={{name:'{0}', level:'{1}', abbr:'{2}'}}".format(mdc.name,
                                                                                                      mdc.level,
                                                                                                      mdc.abbr)
        neo4j.run(cypher)

    start_step("添加目的层层级关系")
    for s, t in tqdm(mdc_relations, ascii=True):
        cypher = 'match (n:MDC), (m:MDC) where n.name="{}" and m.name="{}" merge (n)<-[:parent]-(m)'.format(s, t)
        neo4j.run(cypher)

    # --------  新增杂质实体 & 关系 ----------
    start_step("新增杂质实体")
    for litho in tqdm(lithos, ascii=True):
        cypher = "merge (n:Litho {{name:'{0}'}}) set n+={{name:'{0}', level:'{1}'}}".format(litho.name, litho.level)
        neo4j.run(cypher)

    start_step("添加杂质关系")
    for s, t in tqdm(litho_relations, ascii=True):
        cypher = 'match (n:Litho), (m:Litho) where n.name="{}" and m.name="{}" merge (n)<-[:parentLitho]-(m)'.format(s,
                                                                                                                     t)
        neo4j.run(cypher)
    # ---------------------------------

    # 新增主体实体
    start_step("新增主体实体")
    for well in tqdm(wells, ascii=True):
        # cypher = 'merge (n:Well {{name:"{1}"}}) set n+= {{id:{0}, name:"{1}", jh:"{1}", ztmd:"{2}", dlwz:"{3}", ' \
        #          'zzbx:{4}, hzby:{5}, ktxm:"{6}", kzrq:"{7}", wzrq:"{8}", wzjs:{9}, wzcw:"{10}"}}'.format(
        #     well.idx,
        #     well.jh,
        #     well.ztmd,
        #     well.dlwz,
        #     well.zzbx,
        #     well.hzby,
        #     well.ktxm,
        #     well.kzrq,
        #     well.wzrq,
        #     well.wzjs,
        #     well.wzcw)

        # --------------------- 修改部分 无id------------------------
        cypher = 'merge (n:Well {{name:"{0}"}}) set n+= {{name:"{0}", jh:"{0}", ztmd:"{1}", dlwz:"{2}", ' \
                 'zzbx:{3}, hzby:{4}, ktxm:"{5}", kzrq:"{6}", wzrq:"{7}", wzjs:{8}, wzcw:"{9}"}}'.format(
            well.jh,
            well.ztmd,
            well.dlwz,
            well.zzbx,
            well.hzby,
            well.ktxm,
            well.kzrq,
            well.wzrq,
            well.wzjs,
            well.wzcw)
        # ---------------------------------------------------

        neo4j.run(cypher)

    # 新增主体->主体别关系
    # wzjb_num = 0  # 统计未知主体别个数
    start_step("新增主体->主体别关系")
    for well in tqdm(wells, ascii=True):
        if len(well.jb) == 1 and well.jb[0] == "未知主体别":
            # wzjb_num += 1
            continue
        for jb in well.jb:
            cypher = 'match (n:Well), (m:JB) where n.name="{}" and m.name="{}" merge (n)-[:jb]->(m)'.format(well.jh, jb)
            neo4j.run(cypher)
    # print(f"未知主体别个数：{wzjb_num}")

    # 新增主体->主体型关系
    start_step("新增主体->主体型关系")
    for well in tqdm(wells, ascii=True):
        if len(well.jx) == 1 and well.jx[0] == "未知主体型":
            continue
        for jx in well.jx:
            cypher = 'match (n:Well), (m:JX) where n.name="{}" and m.name="{}" merge (n)-[:jx]->(m)'.format(well.jh, jx)
            neo4j.run(cypher)

    # 新增主体->产物区关系
    start_step("新增主体->产物区关系")
    for well in tqdm(wells, ascii=True):
        if well.yqt == "未知产物区":
            continue
        cypher = 'match (n:Well), (m:YQT) where n.name="{}" and m.name="{}" merge (n)-[:yqt]->(m)'.format(well.jh,
                                                                                                          well.yqt)
        neo4j.run(cypher)

    # 新增主体->目的层关系
    start_step("新增主体->目的层关系")
    for well in tqdm(wells, ascii=True):
        if len(well.zymdc) == 1 and well.zymdc[0] == "未知目的层":
            continue
        for zymdc in well.zymdc:
            cypher = 'match (n:Well), (m:MDC) where n.name="{}" and m.name="{}" merge (n)-[:zymdc]->(m)'.format(well.jh,
                                                                                                                zymdc)
            neo4j.run(cypher)

    # 新增完成层位->层位关系
    start_step("新增完成层位->层位关系")
    for well in tqdm(wells, ascii=True):
        if len(well.wzcw) == 1 and well.wzcw[0] == "未记录完成层位":
            continue
        for wzcw in well.wzcw:
            cypher = 'match (n:Well), (m:MDC) where n.name="{}" and m.name="{}" merge (n)-[:wzcw]->(m)'.format(well.jh,
                                                                                                               wzcw)
            neo4j.run(cypher)

    print("自动抽取主体实体至Neo4j图谱完成，共抽取主体别实体{}个，主体型实体{}个，产物区实体{}个，目的层实体{}个，主体实体{}个。\n"
          "请前往Neo4j图形界面检查结果".format(len(jb_set), len(jx_set), len(yqt_set), len(mdcs), len(wells)))


if __name__ == "__main__":
    # well_data_extraction(data_root)
    add_wl02_1_cjjs_relations()

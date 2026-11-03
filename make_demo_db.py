"""生成 demo.db：零售银行演示库（14 张表 / 约 4600 行）。

保留 orders / customers 两张最简表（冒烟测试与 README 口径对比示例依赖），
新增 12 张银行零售表：存贷通产品、理财产品、助力链配置/生成/记录、签约、
贷款合同、贷款借据、个人客户、存款账户、FTP收支、理财持有。

表名刻意用行内常见的英文缩写风格（loan_iou_info / dp_acct / ftp_detail），
中文业务含义与口径不进表名——这正是语义层（config/ontology.yaml）要解决的
问题：模型看表名猜不出「不良贷款余额要 five_class 后三类」，必须走
browse_terms / resolve_term。

固定随机种子：每次生成数据一致，冒烟可复现。
运行：python make_demo_db.py
"""
import random
import sqlite3
from datetime import date, timedelta

random.seed(42)

BRANCHES = ["北京分行营业部", "上海静安支行", "深圳福田支行", "成都高新支行", "武汉江岸支行"]
REPAY_TYPES = ["等额本息", "等额本金", "一次性还本付息", "按月付息到期还本"]


def d(start: date, end: date) -> str:
    """[start, end] 内随机一天的 'YYYY-MM-DD' 字符串。"""
    return str(start + timedelta(days=random.randint(0, (end - start).days)))


def main() -> None:
    conn = sqlite3.connect("demo.db")
    conn.executescript("""
    DROP TABLE IF EXISTS orders;
    CREATE TABLE orders(
        id INTEGER PRIMARY KEY, customer TEXT, amount REAL, status TEXT);
    DROP TABLE IF EXISTS customers;
    CREATE TABLE customers(
        id INTEGER PRIMARY KEY, name TEXT, city TEXT, idcard TEXT);

    -- 个人客户基本信息表
    DROP TABLE IF EXISTS cust_info;
    CREATE TABLE cust_info(
        cust_id TEXT PRIMARY KEY, cust_name TEXT, gender TEXT,
        cert_type TEXT, cert_no TEXT, mobile TEXT, cust_level TEXT,
        open_branch TEXT, create_date TEXT);

    -- 存贷通协议贷款产品表
    DROP TABLE IF EXISTS cdt_prod;
    CREATE TABLE cdt_prod(
        prod_id TEXT PRIMARY KEY, prod_name TEXT, prod_type TEXT, currency TEXT,
        min_amount REAL, max_amount REAL, term_min_m INTEGER, term_max_m INTEGER,
        status TEXT);

    -- 协议理财产品表
    DROP TABLE IF EXISTS wm_prod;
    CREATE TABLE wm_prod(
        prod_id TEXT PRIMARY KEY, prod_name TEXT, risk_level TEXT,
        base_yield REAL, min_subscribe REAL, term_days INTEGER, status TEXT);

    -- 助力链配置表
    DROP TABLE IF EXISTS boost_chain_cfg;
    CREATE TABLE boost_chain_cfg(
        chain_id TEXT PRIMARY KEY, activity_name TEXT, boost_target INTEGER,
        reward_type TEXT, reward_amount REAL, start_date TEXT, end_date TEXT,
        status TEXT);

    -- 助力链生成表
    DROP TABLE IF EXISTS boost_chain_gen;
    CREATE TABLE boost_chain_gen(
        gen_id TEXT PRIMARY KEY, chain_id TEXT, initiator_cust_id TEXT,
        gen_time TEXT, joined_cnt INTEGER, gen_status TEXT);

    -- 助力表
    DROP TABLE IF EXISTS boost_record;
    CREATE TABLE boost_record(
        boost_id INTEGER PRIMARY KEY, gen_id TEXT, helper_cust_id TEXT,
        boost_time TEXT, boost_value REAL, is_valid INTEGER);

    -- 签约信息表
    DROP TABLE IF EXISTS sign_info;
    CREATE TABLE sign_info(
        sign_id INTEGER PRIMARY KEY, cust_id TEXT, biz_type TEXT, prod_id TEXT,
        sign_date TEXT, sign_status TEXT, channel TEXT);

    -- 贷款合同基本信息表
    DROP TABLE IF EXISTS loan_contract;
    CREATE TABLE loan_contract(
        contract_no TEXT PRIMARY KEY, cust_id TEXT, prod_id TEXT, currency TEXT,
        contract_amount REAL, start_date TEXT, end_date TEXT, contract_status TEXT);

    -- 贷款借据信息表
    DROP TABLE IF EXISTS loan_iou_info;
    CREATE TABLE loan_iou_info(
        iou_id TEXT PRIMARY KEY, contract_no TEXT, cust_id TEXT, prod_id TEXT,
        issue_date TEXT, due_date TEXT, loan_amount REAL, balance REAL,
        rate REAL, five_class TEXT, overdue_days INTEGER, repay_type TEXT);

    -- 存款账户表
    DROP TABLE IF EXISTS dp_acct;
    CREATE TABLE dp_acct(
        acct_id TEXT PRIMARY KEY, cust_id TEXT, acct_type TEXT, currency TEXT,
        balance REAL, freeze_amount REAL, avail_balance REAL,
        open_date TEXT, acct_status TEXT);

    -- FTP收支信息表
    DROP TABLE IF EXISTS ftp_detail;
    CREATE TABLE ftp_detail(
        ftp_id INTEGER PRIMARY KEY, biz_type TEXT, ref_id TEXT, cust_id TEXT,
        stat_month TEXT, ftp_income REAL, ftp_cost REAL);

    -- 客户理财信息表
    DROP TABLE IF EXISTS cust_wm_hold;
    CREATE TABLE cust_wm_hold(
        hold_id TEXT PRIMARY KEY, cust_id TEXT, prod_id TEXT,
        subscribe_amount REAL, hold_balance REAL, buy_date TEXT, due_date TEXT,
        yield_rate REAL, wm_status TEXT);
    """)

    # ---------- 基础两表（保持原样，冒烟与 README 示例依赖） ----------
    conn.executemany(
        "INSERT INTO orders VALUES(?, ?, ?, ?)",
        [(i, f"客户{i % 30 + 1}", i * 13.5, "PENDING" if i % 9 == 0 else "PAID")
         for i in range(1, 51)])
    conn.executemany(
        "INSERT INTO customers VALUES(?, ?, ?, ?)",
        [(i, f"客户{i}", ("北京", "上海", "深圳")[i % 3], f"110101199001{i:02d}000X")
         for i in range(1, 31)])

    # ---------- 个人客户 200 户 ----------
    surnames = "张王李赵刘陈杨黄周吴徐孙马朱胡郭何高林"
    given = ["伟", "芳", "娜", "敏", "静", "强", "磊", "军", "洋", "勇",
             "艳", "杰", "娟", "涛", "明", "超", "霞", "平", "刚", "丽"]
    cust_ids = [f"C{i:05d}" for i in range(1, 201)]
    conn.executemany(
        "INSERT INTO cust_info VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(cid,
          random.choice(surnames) + random.choice(given),
          random.choice("男女"),
          "IDCARD",
          f"110101{random.randint(1960, 2005):04d}{random.randint(1, 12):02d}"
          f"{random.randint(1, 28):02d}{random.randint(1000, 9999)}X",
          f"1{random.choice('3589')}{random.randint(100000000, 999999999)}",
          "VIP" if random.random() < 0.1 else "普通",
          random.choice(BRANCHES),
          d(date(2018, 1, 1), date(2025, 12, 31)))
         for cid in cust_ids])

    # ---------- 产品：存贷通 5 款 + 理财 6 款 ----------
    conn.executemany("INSERT INTO cdt_prod VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", [
        ("CDT001", "存贷通·房贷抵扣款", "存贷通", "CNY", 50000, 10000000, 36, 360, "在售"),
        ("CDT002", "存贷通·消费贷联动", "存贷通", "CNY", 10000, 500000, 6, 60, "在售"),
        ("CDT003", "存贷通·经营贷联动", "存贷通", "CNY", 50000, 3000000, 12, 120, "在售"),
        ("CDT004", "存贷通·存量专属", "存贷通", "CNY", 20000, 2000000, 24, 240, "停售"),
        ("LN001", "个人住房贷款", "住房贷款", "CNY", 200000, 5000000, 60, 360, "在售"),
    ])
    conn.executemany("INSERT INTO wm_prod VALUES(?, ?, ?, ?, ?, ?, ?)", [
        ("WM001", "添利90天", "R2", 0.026, 10000, 90, "在售"),
        ("WM002", "稳盈180天", "R2", 0.030, 50000, 180, "在售"),
        ("WM003", "季季红", "R3", 0.035, 10000, 90, "在售"),
        ("WM004", "年年利", "R3", 0.038, 50000, 365, "在售"),
        ("WM005", "天天盈", "R1", 0.018, 1000, 1, "在售"),
        ("WM006", "私享尊利", "R4", 0.045, 1000000, 365, "停售"),
    ])

    # ---------- 助力链：配置 6 条 / 生成 300 条 / 助力 1000 次 ----------
    chains = [
        ("CH01", "邀请3人得20元立减金", 3, "CASH", 20, date(2026, 9, 1), date(2026, 10, 31)),
        ("CH02", "助力达标送视频会员", 5, "COUPON", 30, date(2026, 9, 15), date(2026, 11, 15)),
        ("CH03", "拉新有礼·积分翻倍", 2, "POINTS", 500, date(2026, 8, 1), date(2026, 12, 31)),
        ("CH04", "存款助力加息券", 3, "COUPON", 15, date(2026, 10, 1), date(2026, 11, 30)),
        ("CH05", "理财体验金助力", 4, "CASH", 10, date(2026, 10, 10), date(2026, 12, 10)),
        ("CH06", "老带新·话费券", 3, "COUPON", 20, date(2026, 7, 1), date(2026, 9, 30)),
    ]
    conn.executemany(
        "INSERT INTO boost_chain_cfg VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
        [(cid, name, tgt, rtype, ramt, str(s), str(e),
          "进行中" if e >= date(2026, 11, 1) else "已结束") for cid, name, tgt, rtype, ramt, s, e in chains])

    gen_rows = []
    for i in range(1, 301):
        chain = random.choice(chains)
        joined = random.randint(0, 8)
        status = ("已成团" if joined >= chain[2]
                  else "已失效" if random.random() < 0.2 else "进行中")
        gen_rows.append((f"G{i:05d}", chain[0], random.choice(cust_ids),
                         d(chain[5], min(chain[6], date(2026, 11, 1))), joined, status))
    conn.executemany("INSERT INTO boost_chain_gen VALUES(?, ?, ?, ?, ?, ?)", gen_rows)

    conn.executemany(
        "INSERT INTO boost_record VALUES(?, ?, ?, ?, ?, ?)",
        [(i, random.choice(gen_rows)[0], random.choice(cust_ids),
          d(date(2026, 8, 1), date(2026, 11, 1)), round(random.uniform(0.5, 5), 1),
          1 if random.random() < 0.9 else 0) for i in range(1, 1001)])

    # ---------- 签约 400 笔 ----------
    sign_types = ["存贷通", "理财", "代发工资", "短信通知"]
    conn.executemany(
        "INSERT INTO sign_info VALUES(?, ?, ?, ?, ?, ?, ?)",
        [(i, random.choice(cust_ids), btype,
          random.choice(["CDT001", "CDT002", "WM001", "WM002", None]
                        if btype in ("存贷通", "理财") else [None]),
          d(date(2020, 1, 1), date(2026, 10, 31)),
          "有效" if random.random() < 0.85 else "解约",
          random.choice(["柜面", "手机银行", "网上银行", "客户经理"]))
         for i, btype in [(i, random.choice(sign_types)) for i in range(1, 401)]])

    # ---------- 贷款：合同 300 / 借据 500 ----------
    loan_products = ["CDT001", "CDT002", "CDT003", "LN001"]
    contracts = []
    for i in range(1, 301):
        amount = round(random.uniform(50, 3000) * 10000, 2)
        start = date(2022, 1, 1)
        start += timedelta(days=random.randint(0, 1400))
        status = random.choices(["正常", "结清", "终止"], weights=[80, 15, 5])[0]
        contracts.append((f"CT{i:06d}", random.choice(cust_ids),
                          random.choice(loan_products), "CNY", amount, str(start),
                          str(start + timedelta(days=365 * random.randint(1, 20))), status))
    conn.executemany("INSERT INTO loan_contract VALUES(?, ?, ?, ?, ?, ?, ?, ?)", contracts)

    # 五级分类：正常85% 关注8% 次级4% 可疑2% 损失1%；逾期天数与分类联动
    ious = []
    for i in range(1, 501):
        ct = random.choice(contracts)
        amount = round(ct[4] * random.uniform(0.3, 1.0), 2)
        five = random.choices(["正常", "关注", "次级", "可疑", "损失"],
                              weights=[85, 8, 4, 2, 1])[0]
        balance = 0.0 if ct[7] == "结清" else round(amount * random.uniform(0.3, 1.0), 2)
        overdue = {"正常": 0, "关注": random.choice([0, 0, 15]),
                   "次级": random.randint(31, 180),
                   "可疑": random.randint(91, 360),
                   "损失": random.randint(361, 720)}[five]
        issue = date(2023, 1, 1) + timedelta(days=random.randint(0, 900))
        ious.append((f"IOU{i:06d}", ct[0], ct[1], ct[2], str(issue),
                     str(issue + timedelta(days=365 * random.randint(1, 20))),
                     amount, balance, round(random.uniform(3.1, 5.9), 4),
                     five, overdue, random.choice(REPAY_TYPES)))
    conn.executemany("INSERT INTO loan_iou_info VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ious)

    # ---------- 存款账户 400 户 ----------
    accts = []
    for i in range(1, 401):
        atype = random.choices(["活期", "定期", "通知存款"], weights=[60, 30, 10])[0]
        balance = (round(random.uniform(500, 500000), 2) if atype == "活期"
                   else round(random.uniform(10000, 2000000), 2))
        freeze = round(balance * random.uniform(0, 0.2), 2) if random.random() < 0.08 else 0.0
        accts.append((f"62220202{i:010d}", random.choice(cust_ids), atype, "CNY",
                      balance, freeze, round(balance - freeze, 2),
                      d(date(2019, 1, 1), date(2026, 10, 31)),
                      "正常" if random.random() < 0.98 else "销户"))
    conn.executemany("INSERT INTO dp_acct VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", accts)

    # ---------- FTP收支：202609/202610 两月，贷款+存款+理财 ----------
    ftp_rows = []
    fid = 1
    for month in ("202609", "202610"):
        for iou in random.sample(ious, 250):                      # 贷款：资产业务
            income = round(iou[7] * iou[8] / 12, 2)
            ftp_rows.append((fid, "贷款", iou[0], iou[2], month,
                             income, round(income * random.uniform(0.55, 0.8), 2)))
            fid += 1
        for acct in random.sample(accts, 250):                    # 存款：负债业务
            cost = round(acct[4] * random.uniform(0.015, 0.03) / 12, 2)
            ftp_rows.append((fid, "存款", acct[0], acct[1], month,
                             round(cost * random.uniform(0.4, 0.7), 2), cost))
            fid += 1
        for h in range(1, 121):                                   # 理财：中间业务
            ftp_rows.append((fid, "理财", f"H{h:05d}", random.choice(cust_ids), month,
                             round(random.uniform(50, 5000), 2),
                             round(random.uniform(10, 800), 2)))
            fid += 1
    conn.executemany("INSERT INTO ftp_detail VALUES(?, ?, ?, ?, ?, ?, ?)", ftp_rows)

    # ---------- 理财持有 300 笔 ----------
    holds = []
    for i in range(1, 301):
        subscribe = round(random.uniform(10000, 2000000), 2)
        redeemed = random.random() < 0.15
        buy = date(2025, 1, 1) + timedelta(days=random.randint(0, 600))
        term = random.choice([90, 180, 365])
        holds.append((f"H{i:05d}", random.choice(cust_ids),
                      random.choice(["WM001", "WM002", "WM003", "WM004", "WM005"]),
                      subscribe, 0.0 if redeemed else round(subscribe * random.uniform(0.9, 1.05), 2),
                      str(buy), str(buy + timedelta(days=term)),
                      round(random.uniform(0.015, 0.045), 4),
                      "已赎回" if redeemed else "持有中"))
    conn.executemany("INSERT INTO cust_wm_hold VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", holds)

    conn.commit()
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
    total = sum(conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in tables)
    print(f"demo.db 就绪：{len(tables)} 张表 / {total} 行")
    conn.close()


if __name__ == "__main__":
    main()

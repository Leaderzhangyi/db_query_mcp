"""生成 demo.db：orders 表 50 行 + customers 表 30 行，供本地 sqlite 演示。

- orders 行数刻意给大：让「能在 SQL 里 COUNT/SUM 就别取全量明细」的
  工具提示有真实分量
- customers 含身份证号：演示语义约束的 block 拦截（见 config/ontology.yaml）
运行：python make_demo_db.py
"""
import sqlite3

TOTAL_ROWS = 50
CUSTOMERS = 30

conn = sqlite3.connect("demo.db")
conn.executescript("""
DROP TABLE IF EXISTS orders;
CREATE TABLE orders(
    id INTEGER PRIMARY KEY,
    customer TEXT,
    amount REAL,
    status TEXT
);
DROP TABLE IF EXISTS customers;
CREATE TABLE customers(
    id INTEGER PRIMARY KEY,
    name TEXT,
    city TEXT,
    idcard TEXT
);
""")
conn.executemany(
    "INSERT INTO orders VALUES(?, ?, ?, ?)",
    [(i, f"客户{i % CUSTOMERS + 1}", i * 13.5, "PENDING" if i % 400 == 0 else "PAID")
     for i in range(1, TOTAL_ROWS + 1)],
)
conn.executemany(
    "INSERT INTO customers VALUES(?, ?, ?, ?)",
    [(i, f"客户{i}", ("北京", "上海", "深圳")[i % 3], f"110101199001{i:02d}000X")
     for i in range(1, CUSTOMERS + 1)],
)
conn.commit()
conn.close()
print(f"demo.db 就绪：orders {TOTAL_ROWS} 行，customers {CUSTOMERS} 行")

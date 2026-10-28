"""生成 demo.db：一张 orders 表 2 万行，供本地 sqlite-demo 实例演示 MCP 工具。

行数刻意给大：让「能在 SQL 里 COUNT/SUM 就别取全量明细」这个工具提示有真实分量。
运行：python make_demo_db.py
"""
import sqlite3

TOTAL_ROWS = 20000

conn = sqlite3.connect("demo.db")
conn.executescript("""
DROP TABLE IF EXISTS orders;
CREATE TABLE orders(
    id INTEGER PRIMARY KEY,
    customer TEXT,
    amount REAL,
    status TEXT
);
""")
conn.executemany(
    "INSERT INTO orders VALUES(?, ?, ?, ?)",
    [(i, f"客户{i % 30 + 1}", i * 13.5, "PENDING" if i % 400 == 0 else "PAID")
     for i in range(1, TOTAL_ROWS + 1)],
)
conn.commit()
conn.close()
print(f"demo.db 就绪：orders 表 {TOTAL_ROWS} 行")

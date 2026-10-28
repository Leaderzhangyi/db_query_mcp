"""SQL 守卫：只放行「单条只读 SELECT」。解析成语法树再判断，不靠正则猜。"""
import sqlglot
from sqlglot import exp

# 会拖垮服务的危险函数（黑名单按需扩充）
DANGEROUS_FUNCTIONS = {"sleep", "benchmark", "load_file", "get_lock"}


def validate(sql: str, dialect: str = "mysql") -> str:
    """校验并净化 SQL：通过则返回重写后的干净 SQL，不通过抛 ValueError。

    dialect 是 sqlglot 方言名（mysql / sqlite / ...），来自数据源实例配置。
    """
    if not sql or not sql.strip():
        raise ValueError("SQL 不能为空")

    # ① 解析成语法树；解析失败说明语法本身就不合法
    try:
        statements = [s for s in sqlglot.parse(sql, read=dialect) if s]
    except sqlglot.errors.ParseError as e:
        raise ValueError(f"SQL 语法无法解析：{str(e).splitlines()[0]}")

    # ② 只允许一条语句——「SELECT 1; DELETE FROM t」在这里被断成两截
    if len(statements) > 1:
        raise ValueError(f"只允许单条语句，当前解析出 {len(statements)} 条")
    stmt = statements[0]

    # ③ 必须是 SELECT。每种语句一个 AST 类：Insert/Update/Delete/Drop/Truncate...
    #    注意：Union 不是 Select 的子类（这是我移植 Java 版时踩过的真坑），要单独放行
    if not isinstance(stmt, (exp.Select, exp.Union, exp.Intersect, exp.Except)):
        raise ValueError(f"只允许 SELECT 查询，当前是 {type(stmt).__name__}")

    # ④ CTE 里不许藏写语句：WITH d AS (DELETE ...) SELECT 看着是查询，
    #    但数据库会真实执行那个 DELETE
    for cte in stmt.find_all(exp.CTE):
        if not isinstance(cte.this, exp.Select):
            raise ValueError("不允许在 CTE 中写数据（WITH d AS (DELETE ...) 会被真实执行）")

    # ⑤ 危险函数黑名单
    for fn in stmt.find_all(exp.Func):
        name = fn.this if isinstance(fn, exp.Anonymous) and isinstance(fn.this, str) else (fn.sql_name() or "")
        if name.lower() in DANGEROUS_FUNCTIONS:
            raise ValueError(f"不允许使用危险函数 {name}：会拖垮查询或读取服务器文件")

    # ⑥ 不许加锁
    if next(stmt.find_all(exp.Lock), None) is not None:
        raise ValueError("不允许加锁查询（FOR UPDATE / FOR SHARE）")

    # 通过：用语法树重新生成 SQL——注释、结尾分号自动消失
    return stmt.sql(dialect=dialect, comments=False)
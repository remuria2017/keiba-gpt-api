import os
import shutil
import traceback
import re
import unicodedata
from typing import Any, Dict, List, Optional, Literal, Tuple, Union
import sqlparse
from fastapi import Body

import duckdb
from fastapi import FastAPI, Header, HTTPException, UploadFile, File, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def ensure_json_utf8(request: Request, call_next):
    response = await call_next(request)
    content_type = response.headers.get("content-type", "")
    if "application/json" in content_type and "charset=" not in content_type.lower():
        response.headers["content-type"] = "application/json; charset=utf-8"
    return response

# =========================
# 設定
# =========================

API_KEY = os.getenv("BACKTEST_API_KEY", "").strip()


def resolve_db_path() -> str:
    env_path = os.getenv("DB_PATH", "").strip()
    candidates: List[str] = []

    if env_path:
        candidates.append(env_path)

    candidates.extend([
        r"C:\keiba\keiba.duckdb",
        "/app/data/keiba.duckdb",
        os.path.join(os.getcwd(), "keiba.duckdb"),
    ])

    for p in candidates:
        if p and os.path.exists(p):
            return p

    return env_path or r"C:\keiba\keiba.duckdb"


DB_PATH = resolve_db_path()

PREFERRED_TABLES = [
    "backtest_with_training_completed",
    "backtest_with_training_clean",
    "training_latest_one_clean",
    "backtest_with_training",
    "training_latest_one",
    "training_analysis",
    "training_join_ready",
    "backtest_join_ready",
    "training",
    "backtest",
]

# =========================
# Pydantic
# =========================

class FilterItem(BaseModel):
    field: str
    op: Literal["=", "!=", ">", ">=", "<", "<=", "in", "like", "between", "is_null", "is_not_null"]
    value: Any = None


class ColumnFilterItem(BaseModel):
    left: str
    op: Literal["=", "!=", ">", ">=", "<", "<="]
    right: str


class ExprFilterItem(BaseModel):
    expr: str
    op: Literal["=", "!=", ">", ">=", "<", "<="]
    value: Any


class MetricItem(BaseModel):
    name: str
    agg: Literal[
        "count",
        "sum",
        "avg",
        "min",
        "max",
        "median",
        "stddev",
        "win_rate",
        "place_rate",
        "tan_return",
        "fuku_return",
        "hit_count",
        "place_hit_count",
    ]
    field: Optional[str] = None


class HavingItem(BaseModel):
    metric: str
    op: Literal["=", "!=", ">", ">=", "<", "<="]
    value: Any


class OrderByItem(BaseModel):
    target: str
    direction: Literal["asc", "desc"] = "asc"


class WhereCondition(BaseModel):
    field: str
    op: Literal["=", "!=", ">", ">=", "<", "<=", "in", "like", "between", "is_null", "is_not_null"]
    value: Any = None


class WhereColumnCondition(BaseModel):
    type: Literal["column"]
    left: str
    op: Literal["=", "!=", ">", ">=", "<", "<="]
    right: str


class WhereAnd(BaseModel):
    and_: List["WhereNode"] = Field(alias="and")


class WhereOr(BaseModel):
    or_: List["WhereNode"] = Field(alias="or")


class WhereNot(BaseModel):
    not_: "WhereNode" = Field(alias="not")


WhereNode = Union[WhereCondition, WhereColumnCondition, WhereAnd, WhereOr, WhereNot]
WhereAnd.model_rebuild()
WhereOr.model_rebuild()
WhereNot.model_rebuild()


class QueryRequest(BaseModel):
    mode: Literal["summary", "rows", "breakdown", "aggregate"] = "summary"
    filters: List[FilterItem] = Field(default_factory=list)
    column_filters: List[ColumnFilterItem] = Field(default_factory=list)
    where: Optional[WhereNode] = None
    expr_filters: List[ExprFilterItem] = Field(default_factory=list)
    group_by: Optional[Union[str, List[str]]] = None
    select: Optional[List[str]] = None
    metrics: List[MetricItem] = Field(default_factory=list)
    having: List[HavingItem] = Field(default_factory=list)
    order_by: List[OrderByItem] = Field(default_factory=list)
    limit: int = 100
    offset: int = 0

class SQLQueryRequest(BaseModel):
    sql: str
    limit: int = 1000
    offset: int = 0


class ResolveValueRequest(BaseModel):
    field: str
    value: str
    limit: int = Field(default=10, ge=1, le=100)


# =========================
# APIキー
# =========================

def check_api_key(x_api_key: Optional[str]):
    if not API_KEY:
        return
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


# =========================
# DB / スキーマ
# =========================

def ensure_db():
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=500, detail=f"DB not found: {DB_PATH}")


def get_connection(read_only: bool = True):
    ensure_db()
    return duckdb.connect(DB_PATH, read_only=read_only)


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def list_tables(con) -> List[str]:
    rows = con.execute("SHOW TABLES").fetchall()
    return [r[0] for r in rows]


def resolve_source_table(con) -> str:
    tables = set(list_tables(con))
    for t in PREFERRED_TABLES:
        if t in tables:
            return t
    if tables:
        return sorted(tables)[0]
    raise HTTPException(status_code=500, detail="No tables found in DB")


def get_schema_map(con, table_name: str) -> Dict[str, str]:
    rows = con.execute(f"DESCRIBE {quote_ident(table_name)}").fetchall()
    return {r[0]: r[1] for r in rows}


def is_numeric_type(type_name: str) -> bool:
    t = (type_name or "").upper()
    numeric_keywords = [
        "BIGINT", "INTEGER", "SMALLINT", "TINYINT",
        "HUGEINT", "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT",
        "DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC"
    ]
    return any(k in t for k in numeric_keywords)


# =========================
# フィールド別名マップ
# =========================

FIELD_CANDIDATES: Dict[str, List[str]] = {
    "race_id": ["race_id", "入ID(新)"],
    "horse_id": ["horse_id", "血統登録番号"],
    "horse_no": ["horse_no", "馬番"],
    "horse_name": ["horse_name", "馬名"],
    "training_horse_name": ["training_horse_name"],

    "人気": ["人気"],
    "確定着順": ["確定着順"],
    "単勝オッズ": ["単勝オッズ"],
    "クラスコード": ["クラスコード"],
    "芝・ダ": ["芝・ダ"],
    "距離": ["距離"],
    "馬場状態": ["馬場状態"],
    "性別": ["性別"],
    "年齢": ["年齢"],
    "頭数": ["頭数"],
    "馬番": ["馬番"],
    "騎手": ["騎手"],
    "調教師": ["調教師"],
    "所属": ["所属"],
    "場所": ["場所"],
    "年": ["年"],
    "月": ["月"],
    "日": ["日"],
    "斤量": ["斤量"],
    "上がり3Fタイム": ["上がり3Fタイム"],
    "PCI": ["PCI"],
    "race_date": ["race_date"],
    "work_date_full": ["work_date_full"],
    "調教師コード": ["調教師コード"],
    "騎手コード": ["騎手コード"],

    # 英字別名
    "class_code": ["クラスコード"],
    "surface": ["芝・ダ"],
    "distance": ["距離"],
    "going": ["馬場状態"],
    "sex": ["性別"],
    "age": ["年齢"],
    "field_size": ["頭数"],
    "jockey": ["騎手"],
    "trainer": ["調教師"],
    "stable": ["所属"],
    "place": ["場所"],
    "odds": ["単勝オッズ"],
    "finish": ["確定着順"],
    "agari3f": ["上がり3Fタイム"],
    "weight": ["斤量"],
    "rank": ["人気"],
    "trainer_code": ["調教師コード"],
    "jockey_code": ["騎手コード"],

    # 調教系
    "course": ["course", "コース", "調教コース"],
    "work_type": ["work_type", "追い種類", "調教種類"],
    "track_condition": ["track_condition", "馬場", "調教馬場状態"],
    "rider_type": ["rider_type", "騎乗者区分"],
    "lap_1": ["lap_1"],
    "lap_2": ["lap_2"],
    "lap_3": ["lap_3"],
    "lap_4": ["lap_4"],
    "lap_last": ["lap_last"],
    "comment": ["comment"],
    "trend": ["trend"],
    "is_previous": ["is_previous"],
    "memo_only": ["memo_only"],
    "work_date": ["work_date"],
    "lap_key_text": ["lap_key_text"],
    "lap_raw_1": ["lap_raw_1"],
    "lap_raw_2": ["lap_raw_2"],
    "lap_raw_3": ["lap_raw_3"],
    "lap_raw_4": ["lap_raw_4"],
    "lap_raw_5": ["lap_raw_5"],
    "position_text": ["position_text"],
    "memo_text": ["memo_text"],
    "with_horse": ["with_horse"],
    "with_horse_class": ["with_horse_class"],
    "with_result": ["with_result"],
    "with_diff": ["with_diff"],
    "with_chase_diff": ["with_chase_diff"],
    "with_side": ["with_side"],
    "with_text": ["with_text"],
    "race_date_text": ["race_date_text"],
    "race_date_iso": ["race_date_iso"],
    "race_name": ["race_name"],
    "conditions": ["conditions"],
    "training_distance": ["training_distance"],
    "training_surface": ["training_surface"],
    "training_turn": ["training_turn"],
    "training_weather": ["training_weather"],
    "training_going": ["training_going"],
    "race_note": ["race_note"],
}

NUMERIC_FIELD_NAMES = {
    "horse_no", "人気", "確定着順", "単勝オッズ", "クラスコード", "距離",
    "年齢", "頭数", "馬番", "年", "月", "日", "斤量", "上がり3Fタイム", "PCI",
    "class_code", "distance", "age", "field_size", "odds", "finish", "agari3f", "weight",
    "rank", "lap_1", "lap_2", "lap_3", "lap_4", "lap_last",
    "trainer_code", "jockey_code", "調教師コード", "騎手コード",
}

ALLOWED_OPS = {"=", "!=", ">", ">=", "<", "<=", "in", "like", "between", "is_null", "is_not_null"}
ALLOWED_COLUMN_OPS = {"=", "!=", ">", ">=", "<", "<="}
ALLOWED_EXPR_FILTER_OPS = {"=", "!=", ">", ">=", "<", "<="}
ALLOWED_METRIC_AGGS = {
    "count", "sum", "avg", "min", "max", "median", "stddev",
    "win_rate", "place_rate", "tan_return", "fuku_return",
    "hit_count", "place_hit_count",
}
ALLOWED_HAVING_OPS = {"=", "!=", ">", ">=", "<", "<="}

DERIVED_EXPR_MAP = {
    "4F合計": "lap_2",
    "加速ラップ": "lap_last < (lap_4 - lap_last)",
    "減速ラップ": "lap_last > (lap_4 - lap_last)",
    "同ラップ": "lap_last = (lap_4 - lap_last)",
}

_ALLOWED_EXPR_TOKENS = re.compile(r'[A-Za-z0-9_\u4e00-\u9fff\u3040-\u30ff・() +\-*/.<>=!]+$')
_EXPR_FIELD_TOKEN = re.compile(r'[A-Za-z_][A-Za-z0-9_]*|[\u4e00-\u9fff\u3040-\u30ff・]+')


def resolve_actual_column(field: str, schema_map: Dict[str, str]) -> str:
    if field in schema_map:
        return field

    candidates = FIELD_CANDIDATES.get(field, [])
    for c in candidates:
        if c in schema_map:
            return c

    raise HTTPException(status_code=400, detail=f"Field not allowed or not found in source table: {field}")


def field_expr(field: str, schema_map: Dict[str, str]) -> str:
    actual = resolve_actual_column(field, schema_map)
    actual_type = schema_map[actual]
    q = quote_ident(actual)

    if field in NUMERIC_FIELD_NAMES and not is_numeric_type(actual_type):
        return f"TRY_CAST({q} AS DOUBLE)"

    return q


def field_label(field: str, schema_map: Dict[str, str]) -> str:
    return resolve_actual_column(field, schema_map)


def normalize_group_by(group_by: Optional[Union[str, List[str]]]) -> List[str]:
    if group_by is None:
        return []
    if isinstance(group_by, str):
        return [group_by]
    if isinstance(group_by, list):
        return [g for g in group_by if g]
    raise HTTPException(status_code=400, detail="group_by must be string or string[]")


def normalize_select(select: Optional[List[str]]) -> List[str]:
    if select is None:
        return []
    if not isinstance(select, list):
        raise HTTPException(status_code=400, detail="select must be string[]")
    return select


def build_safe_expr(expr: str, schema_map: Dict[str, str]) -> str:
    expr = (expr or "").strip()
    if not expr:
        raise HTTPException(status_code=400, detail="expr must not be empty")

    expr = DERIVED_EXPR_MAP.get(expr, expr)

    if not _ALLOWED_EXPR_TOKENS.fullmatch(expr):
        raise HTTPException(status_code=400, detail=f"Unsafe expr: {expr}")

    def repl(m: re.Match) -> str:
        token = m.group(0)
        lowered = token.lower()

        if lowered in {"and", "or", "not", "null", "true", "false"}:
            return token

        if token in FIELD_CANDIDATES or token in schema_map:
            return field_expr(token, schema_map)

        if re.fullmatch(r"\d+", token):
            return token

        raise HTTPException(status_code=400, detail=f"Unknown token in expr: {token}")

    return _EXPR_FIELD_TOKEN.sub(repl, expr)


# =========================
# where / filters
# =========================

def build_filter_clause(f: FilterItem, schema_map: Dict[str, str]) -> Tuple[str, List[Any]]:
    if f.op not in ALLOWED_OPS:
        raise HTTPException(status_code=400, detail=f"Operator not allowed: {f.op}")

    expr = field_expr(f.field, schema_map)

    if f.op in {"=", "!=", ">", ">=", "<", "<="}:
        return f"{expr} {f.op} ?", [f.value]

    if f.op == "like":
        return f"{expr} LIKE ?", [f.value]

    if f.op == "in":
        if not isinstance(f.value, list) or len(f.value) == 0:
            raise HTTPException(status_code=400, detail=f"IN value must be a non-empty list: {f.field}")
        placeholders = ",".join(["?"] * len(f.value))
        return f"{expr} IN ({placeholders})", list(f.value)

    if f.op == "between":
        if not isinstance(f.value, list) or len(f.value) != 2:
            raise HTTPException(status_code=400, detail=f"BETWEEN value must be a list of length 2: {f.field}")
        return f"{expr} BETWEEN ? AND ?", [f.value[0], f.value[1]]

    if f.op == "is_null":
        return f"{expr} IS NULL", []

    if f.op == "is_not_null":
        return f"{expr} IS NOT NULL", []

    raise HTTPException(status_code=400, detail=f"Unsupported operator: {f.op}")


def build_column_filter_clause(cf: ColumnFilterItem, schema_map: Dict[str, str]) -> Tuple[str, List[Any]]:
    if cf.op not in ALLOWED_COLUMN_OPS:
        raise HTTPException(status_code=400, detail=f"Column operator not allowed: {cf.op}")
    left_expr = field_expr(cf.left, schema_map)
    right_expr = field_expr(cf.right, schema_map)
    return f"{left_expr} {cf.op} {right_expr}", []


def build_expr_filter_clause(ef: ExprFilterItem, schema_map: Dict[str, str]) -> Tuple[str, List[Any]]:
    if ef.op not in ALLOWED_EXPR_FILTER_OPS:
        raise HTTPException(status_code=400, detail=f"Expr operator not allowed: {ef.op}")
    expr_sql = build_safe_expr(ef.expr, schema_map)
    return f"({expr_sql}) {ef.op} ?", [ef.value]


def build_where_node(node: WhereNode, schema_map: Dict[str, str]) -> Tuple[str, List[Any]]:
    if isinstance(node, WhereCondition):
        return build_filter_clause(FilterItem(field=node.field, op=node.op, value=node.value), schema_map)

    if isinstance(node, WhereColumnCondition):
        return build_column_filter_clause(ColumnFilterItem(left=node.left, op=node.op, right=node.right), schema_map)

    if isinstance(node, WhereAnd):
        clauses: List[str] = []
        params: List[Any] = []
        for child in node.and_:
            child_sql, child_params = build_where_node(child, schema_map)
            clauses.append(f"({child_sql})")
            params.extend(child_params)
        return (" AND ".join(clauses) if clauses else "1=1"), params

    if isinstance(node, WhereOr):
        clauses: List[str] = []
        params: List[Any] = []
        for child in node.or_:
            child_sql, child_params = build_where_node(child, schema_map)
            clauses.append(f"({child_sql})")
            params.extend(child_params)
        return (" OR ".join(clauses) if clauses else "1=0"), params

    if isinstance(node, WhereNot):
        child_sql, child_params = build_where_node(node.not_, schema_map)
        return f"NOT ({child_sql})", child_params

    raise HTTPException(status_code=400, detail="Invalid where node")


def build_where_legacy(
    filters: List[FilterItem],
    column_filters: List[ColumnFilterItem],
    expr_filters: List[ExprFilterItem],
    schema_map: Dict[str, str]
) -> Tuple[str, List[Any]]:
    clauses = ["1=1"]
    params: List[Any] = []

    for f in filters:
        sql, p = build_filter_clause(f, schema_map)
        clauses.append(sql)
        params.extend(p)

    for cf in column_filters:
        sql, p = build_column_filter_clause(cf, schema_map)
        clauses.append(sql)
        params.extend(p)

    for ef in expr_filters:
        sql, p = build_expr_filter_clause(ef, schema_map)
        clauses.append(sql)
        params.extend(p)

    return " AND ".join(clauses), params


def build_where(req: QueryRequest, schema_map: Dict[str, str]) -> Tuple[str, List[Any], str]:
    if req.where is not None:
        base_sql, base_params = build_where_node(req.where, schema_map)
        clauses = [f"({base_sql})"]
        params = list(base_params)

        for ef in req.expr_filters:
            sql, p = build_expr_filter_clause(ef, schema_map)
            clauses.append(sql)
            params.extend(p)

        return " AND ".join(clauses), params, "where"

    where_sql, params = build_where_legacy(req.filters, req.column_filters, req.expr_filters, schema_map)
    return where_sql, params, "legacy"


# =========================
# サマリー / 指標
# =========================

def resolve_summary_columns(schema_map: Dict[str, str]) -> Tuple[str, str]:
    finish_expr = field_expr("確定着順", schema_map)
    odds_expr = field_expr("単勝オッズ", schema_map)
    return finish_expr, odds_expr


def metric_sql(item: MetricItem, schema_map: Dict[str, str]) -> str:
    if item.agg not in ALLOWED_METRIC_AGGS:
        raise HTTPException(status_code=400, detail=f"Metric agg not allowed: {item.agg}")

    alias = quote_ident(item.name)
    finish_expr, odds_expr = resolve_summary_columns(schema_map)

    if item.agg == "count":
        return f"COUNT(*) AS {alias}"

    if item.agg == "win_rate":
        return f"ROUND(AVG(CASE WHEN {finish_expr} = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS {alias}"

    if item.agg == "place_rate":
        return f"ROUND(AVG(CASE WHEN {finish_expr} <= 3 THEN 1.0 ELSE 0.0 END) * 100, 2) AS {alias}"

    if item.agg == "tan_return":
        return f"ROUND(AVG(CASE WHEN {finish_expr} = 1 THEN {odds_expr} ELSE 0 END) * 100, 2) AS {alias}"

    if item.agg == "hit_count":
        return f"SUM(CASE WHEN {finish_expr} = 1 THEN 1 ELSE 0 END) AS {alias}"

    if item.agg == "place_hit_count":
        return f"SUM(CASE WHEN {finish_expr} <= 3 THEN 1 ELSE 0 END) AS {alias}"

    if item.agg == "fuku_return":
        raise HTTPException(status_code=400, detail="fuku_return is not supported because payout column is not available in current source table")

    if not item.field:
        raise HTTPException(status_code=400, detail=f"field is required for metric agg: {item.agg}")

    expr = field_expr(item.field, schema_map)

    if item.agg == "sum":
        return f"ROUND(SUM({expr}), 6) AS {alias}"
    if item.agg == "avg":
        return f"ROUND(AVG({expr}), 6) AS {alias}"
    if item.agg == "min":
        return f"MIN({expr}) AS {alias}"
    if item.agg == "max":
        return f"MAX({expr}) AS {alias}"
    if item.agg == "median":
        return f"ROUND(MEDIAN({expr}), 6) AS {alias}"
    if item.agg == "stddev":
        return f"ROUND(STDDEV_SAMP({expr}), 6) AS {alias}"

    raise HTTPException(status_code=400, detail=f"Unsupported metric agg: {item.agg}")


# =========================
# SQL
# =========================

def build_summary_sql(source_table: str, schema_map: Dict[str, str], where_sql: str) -> str:
    finish_expr, odds_expr = resolve_summary_columns(schema_map)
    src = quote_ident(source_table)

    return f"""
    SELECT
        COUNT(*) AS n,
        ROUND(AVG(CASE WHEN {finish_expr} = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS win_rate,
        ROUND(AVG(CASE WHEN {finish_expr} <= 3 THEN 1.0 ELSE 0.0 END) * 100, 2) AS place_rate,
        ROUND(AVG(CASE WHEN {finish_expr} = 1 THEN {odds_expr} ELSE 0 END) * 100, 2) AS tan_return
    FROM {src}
    WHERE {where_sql}
    """


def build_breakdown_sql(
    source_table: str,
    schema_map: Dict[str, str],
    where_sql: str,
    group_by: Union[str, List[str]],
    limit: int,
    offset: int,
) -> str:
    finish_expr, odds_expr = resolve_summary_columns(schema_map)
    group_fields = normalize_group_by(group_by)
    if not group_fields:
        raise HTTPException(status_code=400, detail="group_by is required for breakdown mode")

    group_exprs = [field_expr(g, schema_map) for g in group_fields]
    select_parts = [f"{expr} AS {quote_ident(g)}" for expr, g in zip(group_exprs, group_fields)]
    group_select_sql = ",\n        ".join(select_parts)
    group_by_sql = ", ".join(group_exprs)
    order_cols = ", ".join([quote_ident(g) for g in group_fields])
    src = quote_ident(source_table)

    return f"""
    SELECT
        {group_select_sql},
        COUNT(*) AS n,
        ROUND(AVG(CASE WHEN {finish_expr} = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS win_rate,
        ROUND(AVG(CASE WHEN {finish_expr} <= 3 THEN 1.0 ELSE 0.0 END) * 100, 2) AS place_rate,
        ROUND(AVG(CASE WHEN {finish_expr} = 1 THEN {odds_expr} ELSE 0 END) * 100, 2) AS tan_return
    FROM {src}
    WHERE {where_sql}
    GROUP BY {group_by_sql}
    ORDER BY n DESC, {order_cols}
    LIMIT {limit}
    OFFSET {offset}
    """


def build_rows_sql(
    source_table: str,
    schema_map: Dict[str, str],
    where_sql: str,
    select: Optional[List[str]],
    order_by: List[OrderByItem],
    limit: int,
    offset: int,
) -> str:
    src = quote_ident(source_table)
    select_fields = normalize_select(select)

    if select_fields:
        select_exprs = [f"{field_expr(f, schema_map)} AS {quote_ident(field_label(f, schema_map))}" for f in select_fields]
        select_sql = ", ".join(select_exprs)
        allowed_order_targets = set(select_fields) | set(field_label(f, schema_map) for f in select_fields)
    else:
        select_sql = "*"
        allowed_order_targets = set(FIELD_CANDIDATES.keys()) | set(schema_map.keys())

    order_parts: List[str] = []
    for item in order_by:
        if item.target not in allowed_order_targets:
            raise HTTPException(status_code=400, detail=f"order_by target not allowed in rows mode: {item.target}")
        order_parts.append(f"{field_expr(item.target, schema_map)} {item.direction.upper()}")

    order_sql = f" ORDER BY {', '.join(order_parts)}" if order_parts else ""

    return f"""
    SELECT {select_sql}
    FROM {src}
    WHERE {where_sql}
    {order_sql}
    LIMIT {limit}
    OFFSET {offset}
    """


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    s = str(value).replace("'", "''")
    return f"'{s}'"


def build_aggregate_sql(
    source_table: str,
    schema_map: Dict[str, str],
    where_sql: str,
    group_by: Optional[Union[str, List[str]]],
    metrics: List[MetricItem],
    having: List[HavingItem],
    order_by: List[OrderByItem],
    limit: int,
    offset: int,
) -> str:
    if not metrics:
        raise HTTPException(status_code=400, detail="metrics is required for aggregate mode")

    src = quote_ident(source_table)
    group_fields = normalize_group_by(group_by)

    select_parts: List[str] = []
    group_sql_parts: List[str] = []

    for g in group_fields:
        expr = field_expr(g, schema_map)
        alias = quote_ident(field_label(g, schema_map))
        select_parts.append(f"{expr} AS {alias}")
        group_sql_parts.append(expr)

    metric_names = set()
    for m in metrics:
        if m.name in metric_names:
            raise HTTPException(status_code=400, detail=f"Duplicate metric name: {m.name}")
        metric_names.add(m.name)
        select_parts.append(metric_sql(m, schema_map))

    select_sql = ",\n        ".join(select_parts)

    group_by_sql = ""
    if group_sql_parts:
        group_by_sql = f"\n    GROUP BY {', '.join(group_sql_parts)}"

    having_sql = ""
    if having:
        clauses: List[str] = []
        for h in having:
            if h.op not in ALLOWED_HAVING_OPS:
                raise HTTPException(status_code=400, detail=f"Having operator not allowed: {h.op}")
            if h.metric not in metric_names:
                raise HTTPException(status_code=400, detail=f"Unknown having metric: {h.metric}")
            clauses.append(f"{quote_ident(h.metric)} {h.op} {sql_literal(h.value)}")
        having_sql = f"\n    HAVING {' AND '.join(clauses)}"

    allowed_order_targets = set(metric_names) | set(field_label(g, schema_map) for g in group_fields)
    order_parts: List[str] = []
    for o in order_by:
        if o.target not in allowed_order_targets:
            raise HTTPException(status_code=400, detail=f"Unknown order_by target: {o.target}")
        order_parts.append(f"{quote_ident(o.target)} {o.direction.upper()}")

    if not order_parts:
        if "n" in metric_names:
            order_parts.append('"n" DESC')
        elif metrics:
            order_parts.append(f'{quote_ident(metrics[0].name)} DESC')

    order_sql = f"\n    ORDER BY {', '.join(order_parts)}"

    return f"""
    SELECT
        {select_sql}
    FROM {src}
    WHERE {where_sql}{group_by_sql}{having_sql}{order_sql}
    LIMIT {limit}
    OFFSET {offset}
    """


# =========================
# 値解決
# =========================

def normalize_jp_text(v: Any) -> str:
    if v is None:
        return ""

    if isinstance(v, bytes):
        for enc in ("utf-8", "cp932", "shift_jis", "utf-8-sig", "euc_jp"):
            try:
                v = v.decode(enc)
                break
            except Exception:
                continue
        else:
            v = v.decode("utf-8", errors="ignore")

    v = str(v)
    v = unicodedata.normalize("NFKC", v)
    v = v.replace("\u3000", " ")
    v = " ".join(v.split())
    return v.strip()


def normalize_for_match(v: Any) -> str:
    return normalize_jp_text(v).replace(" ", "")


RESOLVABLE_FIELDS = {
    "trainer": ["調教師", "調教師コード"],
    "調教師": ["調教師", "調教師コード"],
    "trainer_code": ["調教師", "調教師コード"],
    "調教師コード": ["調教師", "調教師コード"],

    "jockey": ["騎手", "騎手コード"],
    "騎手": ["騎手", "騎手コード"],
    "jockey_code": ["騎手", "騎手コード"],
    "騎手コード": ["騎手", "騎手コード"],

    "course": ["course"],
    "horse_name": ["馬名"],
    "horse_id": ["horse_id", "血統登録番号"],
}


def resolve_value_columns(field: str, schema_map: Dict[str, str]) -> Tuple[str, Optional[str]]:
    keys = RESOLVABLE_FIELDS.get(field)
    if not keys:
        raise HTTPException(status_code=400, detail=f"resolve_value unsupported field: {field}")

    label_col = None
    code_col = None

    for candidate in keys:
        actual = resolve_actual_column(candidate, schema_map)
        if candidate in {"調教師", "騎手", "course", "馬名"}:
            label_col = actual
        elif candidate in {"調教師コード", "騎手コード", "horse_id", "血統登録番号"}:
            code_col = actual

    if label_col is None and code_col is None:
        raise HTTPException(status_code=400, detail=f"resolve_value could not map field: {field}")

    if label_col is None and code_col is not None:
        label_col = code_col

    return label_col, code_col


def _resolve_value_impl(field: str, query_text: str, limit: int) -> Dict[str, Any]:
    ensure_db()

    field = normalize_jp_text(field)
    query_text = normalize_jp_text(query_text)
    query_key = normalize_for_match(query_text)

    if not field:
        raise HTTPException(status_code=400, detail="field is required")
    if not query_key:
        raise HTTPException(status_code=400, detail="q/value is empty")

    con = get_connection(read_only=True)
    try:
        source_table = resolve_source_table(con)
        schema_map = get_schema_map(con, source_table)

        label_col, code_col = resolve_value_columns(field, schema_map)
        src = quote_ident(source_table)
        label_expr = quote_ident(label_col)

        if code_col and code_col != label_col:
            code_expr = quote_ident(code_col)
            sql = f"""
            SELECT DISTINCT
                {label_expr} AS label,
                {code_expr} AS code
            FROM {src}
            WHERE {label_expr} IS NOT NULL
            ORDER BY label
            """
            rows = con.execute(sql).fetchall()
        else:
            sql = f"""
            SELECT DISTINCT
                {label_expr} AS label,
                NULL AS code
            FROM {src}
            WHERE {label_expr} IS NOT NULL
            ORDER BY label
            """
            rows = con.execute(sql).fetchall()

        exact_matches: List[Dict[str, Any]] = []
        partial_matches: List[Dict[str, Any]] = []

        for raw_label, raw_code in rows:
            label = normalize_jp_text(raw_label)
            label_key = normalize_for_match(raw_label)
            if not label_key:
                continue

            item = {
                "label": raw_label,
                "value": raw_label,
                "code_field": code_col if (code_col and code_col != label_col) else None,
                "code": raw_code if (code_col and code_col != label_col) else None,
            }

            if label_key == query_key:
                exact_matches.append(item)
            elif query_key in label_key:
                partial_matches.append(item)

        matches = exact_matches[:limit] if exact_matches else partial_matches[:limit]

        return {
            "ok": True,
            "field": field,
            "q": query_text,
            "source_table": source_table,
            "match_type": "exact" if exact_matches else "partial",
            "matches": matches,
        }

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()


@app.get("/resolve_value")
def resolve_value(
    field: str = Query(...),
    q: str = Query(...),
    limit: int = Query(10, ge=1, le=100),
    x_api_key: Optional[str] = Header(default=None),
):
    check_api_key(x_api_key)
    return _resolve_value_impl(field=field, query_text=q, limit=limit)


@app.post("/resolve_value")
def resolve_value_post(
    req: ResolveValueRequest,
    x_api_key: Optional[str] = Header(default=None),
):
    check_api_key(x_api_key)
    return _resolve_value_impl(field=req.field, query_text=req.value, limit=req.limit)

# =========================
# SQL mode
# =========================

FORBIDDEN_SQL_KEYWORDS = {
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
    "TRUNCATE", "ATTACH", "DETACH", "COPY", "EXPORT", "IMPORT",
    "PRAGMA", "INSTALL", "LOAD", "REPLACE", "MERGE", "VACUUM",
    "CALL"
}


def strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return sql


def validate_sql_readonly(sql: str) -> str:
    if not sql or not sql.strip():
        raise HTTPException(status_code=400, detail="sql is empty")

    cleaned = strip_sql_comments(sql).strip()

    # 複文禁止
    if ";" in cleaned.rstrip(";"):
        raise HTTPException(status_code=400, detail="multiple SQL statements are not allowed")

    upper_sql = cleaned.upper()

    # SELECT / WITH のみ許可
    if not (upper_sql.startswith("SELECT") or upper_sql.startswith("WITH")):
        raise HTTPException(status_code=400, detail="only SELECT or WITH queries are allowed")

    # 危険キーワード禁止
    for kw in FORBIDDEN_SQL_KEYWORDS:
        if re.search(rf"\b{re.escape(kw)}\b", upper_sql):
            raise HTTPException(status_code=400, detail=f"forbidden SQL keyword detected: {kw}")

    return cleaned


def append_limit_offset(sql: str, limit: int, offset: int) -> str:
    sql = sql.strip().rstrip(";")
    upper_sql = sql.upper()

    # すでに LIMIT があればそのまま
    if re.search(r"\bLIMIT\b", upper_sql):
        return sql

    safe_limit = max(1, min(limit, 5000))
    safe_offset = max(0, offset)

    if safe_offset > 0:
        return f"{sql}\nLIMIT {safe_limit} OFFSET {safe_offset}"
    return f"{sql}\nLIMIT {safe_limit}"
# =========================
# API
# =========================

@app.get("/health")
def health():
    db_exists = os.path.exists(DB_PATH)

    source_table = None
    schema_map: Dict[str, str] = {}
    tables: List[str] = []

    if db_exists:
        con = get_connection(read_only=True)
        try:
            tables = list_tables(con)
            source_table = resolve_source_table(con)
            schema_map = get_schema_map(con, source_table)
        finally:
            con.close()

    return {
        "ok": True,
        "db_path": DB_PATH,
        "db_exists": db_exists,
        "api_key_required": bool(API_KEY),
        "tables": tables,
        "source_table": source_table,
        "columns": list(schema_map.keys()),
        "allowed_filter_names": sorted(list(dict.fromkeys(list(FIELD_CANDIDATES.keys()) + list(schema_map.keys())))),
        "allowed_column_filter_ops": sorted(list(ALLOWED_COLUMN_OPS)),
        "allowed_expr_filter_ops": sorted(list(ALLOWED_EXPR_FILTER_OPS)),
        "allowed_metric_aggs": sorted(list(ALLOWED_METRIC_AGGS)),
        "supported_modes": ["summary", "rows", "breakdown", "aggregate"],
        "supports_where_tree": True,
        "supports_select": True,
        "supports_order_by": True,
        "supports_having": True,
        "supports_offset": True,
        "supports_expr_filters": True,
        "supports_resolve_value": True,
        "resolve_value_methods": ["GET", "POST"],
        "derived_expr_names": sorted(list(DERIVED_EXPR_MAP.keys())),
        "resolvable_fields": sorted(list(RESOLVABLE_FIELDS.keys())),
        "supports_sql_query": True,
        "sql_query_readonly": True,
        "sql_query_max_limit": 5000,
        "allowed_ops": sorted(list(ALLOWED_OPS)),
    }


@app.post("/upload-db")
async def upload_db(file: UploadFile = File(...), x_api_key: Optional[str] = Header(default=None)):
    check_api_key(x_api_key)

    save_dir = os.path.dirname(DB_PATH) or os.getcwd()
    os.makedirs(save_dir, exist_ok=True)

    with open(DB_PATH, "wb") as f:
        shutil.copyfileobj(file.file, f)

    return {"ok": True, "path": DB_PATH, "size": os.path.getsize(DB_PATH)}

@app.post("/sql_query")
def sql_query(req: SQLQueryRequest, x_api_key: Optional[str] = Header(default=None)):
    check_api_key(x_api_key)
    ensure_db()

    con = get_connection(read_only=True)
    try:
        validated_sql = validate_sql_readonly(req.sql)
        final_sql = append_limit_offset(validated_sql, req.limit, req.offset)

        cur = con.execute(final_sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
        data = [dict(zip(cols, r)) for r in rows]

        applied_limit = max(1, min(req.limit, 5000))

        return {
            "ok": True,
            "sql": final_sql,
            "columns": cols,
            "row_count": len(data),
            "rows": data,
            "truncated": len(data) >= applied_limit and "LIMIT" not in validated_sql.upper(),
        }

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()


@app.post("/query")
def query_backtest(req: QueryRequest, x_api_key: Optional[str] = Header(default=None)):
    check_api_key(x_api_key)
    ensure_db()

    con = get_connection(read_only=True)
    try:
        source_table = resolve_source_table(con)
        schema_map = get_schema_map(con, source_table)

        where_sql, params, where_mode = build_where(req, schema_map)

        if req.mode == "summary":
            sql = build_summary_sql(source_table, schema_map, where_sql)

        elif req.mode == "rows":
            safe_limit = max(1, min(req.limit, 1000))
            safe_offset = max(0, req.offset)
            sql = build_rows_sql(
                source_table=source_table,
                schema_map=schema_map,
                where_sql=where_sql,
                select=req.select,
                order_by=req.order_by,
                limit=safe_limit,
                offset=safe_offset,
            )

        elif req.mode == "breakdown":
            if not req.group_by:
                raise HTTPException(status_code=400, detail="group_by is required for breakdown mode")
            safe_limit = max(1, min(req.limit, 1000))
            safe_offset = max(0, req.offset)
            sql = build_breakdown_sql(
                source_table=source_table,
                schema_map=schema_map,
                where_sql=where_sql,
                group_by=req.group_by,
                limit=safe_limit,
                offset=safe_offset,
            )

        elif req.mode == "aggregate":
            safe_limit = max(1, min(req.limit, 1000))
            safe_offset = max(0, req.offset)
            sql = build_aggregate_sql(
                source_table=source_table,
                schema_map=schema_map,
                where_sql=where_sql,
                group_by=req.group_by,
                metrics=req.metrics,
                having=req.having,
                order_by=req.order_by,
                limit=safe_limit,
                offset=safe_offset,
            )
        else:
            raise HTTPException(status_code=400, detail=f"Unknown mode: {req.mode}")

        cur = con.execute(sql, params)

        if req.mode == "summary":
            row = cur.fetchone()
            data = {
                "n": int(row[0] or 0),
                "win_rate": float(row[1] or 0),
                "place_rate": float(row[2] or 0),
                "tan_return": float(row[3] or 0),
            }
        else:
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            data = [dict(zip(cols, r)) for r in rows]

        return {
            "ok": True,
            "mode": req.mode,
            "where_mode": where_mode,
            "db_path": DB_PATH,
            "source_table": source_table,
            "filters": [f.model_dump() for f in req.filters],
            "column_filters": [f.model_dump() for f in req.column_filters],
            "where": req.where.model_dump(by_alias=True) if req.where is not None else None,
            "expr_filters": [f.model_dump() for f in req.expr_filters],
            "group_by": req.group_by,
            "select": req.select,
            "metrics": [m.model_dump() for m in req.metrics],
            "having": [h.model_dump() for h in req.having],
            "order_by": [o.model_dump() for o in req.order_by],
            "limit": req.limit,
            "offset": req.offset,
            "result": data,
        }

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()
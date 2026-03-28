
import os
import shutil
import traceback
from typing import Any, Dict, List, Optional, Literal, Tuple, Union

import duckdb
from fastapi import FastAPI, Header, HTTPException, UploadFile, File
from pydantic import BaseModel, Field

app = FastAPI()


# =========================
# 設定
# =========================

API_KEY = os.getenv("BACKTEST_API_KEY", "").strip()


def resolve_db_path() -> str:
    env_path = os.getenv("DB_PATH", "").strip()
    candidates = []

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


class ConditionNode(BaseModel):
    and_: Optional[List["Condition"]] = Field(default=None, alias="and")
    or_: Optional[List["Condition"]] = Field(default=None, alias="or")
    not_: Optional["Condition"] = Field(default=None, alias="not")
    field: Optional[str] = None
    op: Optional[Literal["=", "!=", ">", ">=", "<", "<=", "in", "like", "between", "is_null", "is_not_null"]] = None
    value: Any = None
    left: Optional[str] = None
    right: Optional[str] = None
    type: Optional[Literal["filter", "column"]] = None


Condition = Union[ConditionNode, FilterItem, ColumnFilterItem]


class MetricItem(BaseModel):
    name: str
    agg: Literal[
        "count", "sum", "avg", "min", "max", "median", "stddev",
        "win_rate", "place_rate", "tan_return", "fuku_return",
        "hit_count", "place_hit_count"
    ]
    field: Optional[str] = None


class HavingItem(BaseModel):
    metric: str
    op: Literal["=", "!=", ">", ">=", "<", "<="]
    value: Any


class OrderByItem(BaseModel):
    target: str
    direction: Literal["asc", "desc"] = "asc"


class QueryRequest(BaseModel):
    # 旧仕様
    mode: Literal["summary", "rows", "breakdown", "aggregate"] = "summary"
    filters: List[FilterItem] = Field(default_factory=list)
    column_filters: List[ColumnFilterItem] = Field(default_factory=list)
    group_by: Optional[Union[str, List[str]]] = None
    limit: int = 100

    # 新仕様
    where: Optional[ConditionNode] = None
    metrics: List[MetricItem] = Field(default_factory=list)
    having: List[HavingItem] = Field(default_factory=list)
    order_by: List[OrderByItem] = Field(default_factory=list)
    select: Optional[List[str]] = None
    offset: int = 0


ConditionNode.model_rebuild()


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
    # レース系
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
}


NUMERIC_FIELD_NAMES = {
    "horse_no", "人気", "確定着順", "単勝オッズ", "クラスコード", "距離",
    "年齢", "頭数", "馬番", "年", "月", "日", "斤量", "上がり3Fタイム", "PCI",
    "class_code", "distance", "age", "field_size", "odds", "finish", "agari3f", "weight",
    "rank", "lap_1", "lap_2", "lap_3", "lap_4", "lap_last",
}


def resolve_actual_column(field: str, schema_map: Dict[str, str]) -> str:
    if field in schema_map:
        return field

    candidates = FIELD_CANDIDATES.get(field, [])
    for c in candidates:
        if c in schema_map:
            return c

    raise HTTPException(
        status_code=400,
        detail=f"Field not allowed or not found in source table: {field}"
    )


def field_expr(field: str, schema_map: Dict[str, str]) -> str:
    actual = resolve_actual_column(field, schema_map)
    actual_type = schema_map[actual]
    q = quote_ident(actual)

    if field in NUMERIC_FIELD_NAMES and not is_numeric_type(actual_type):
        return f"TRY_CAST({q} AS DOUBLE)"

    return q


ALLOWED_OPS = {"=", "!=", ">", ">=", "<", "<=", "in", "like", "between", "is_null", "is_not_null"}
ALLOWED_COLUMN_OPS = {"=", "!=", ">", ">=", "<", "<="}
ALLOWED_HAVING_OPS = {"=", "!=", ">", ">=", "<", "<="}
ALLOWED_METRICS = {
    "count", "sum", "avg", "min", "max", "median", "stddev",
    "win_rate", "place_rate", "tan_return", "fuku_return",
    "hit_count", "place_hit_count"
}


def normalize_group_by(group_by: Optional[Union[str, List[str]]]) -> List[str]:
    if group_by is None:
        return []
    if isinstance(group_by, str):
        return [group_by]
    if isinstance(group_by, list):
        if not group_by:
            return []
        return group_by
    raise HTTPException(status_code=400, detail="group_by must be string or string[]")


def normalize_select(select_cols: Optional[List[str]]) -> List[str]:
    if not select_cols:
        return []
    uniq: List[str] = []
    seen = set()
    for c in select_cols:
        if c not in seen:
            uniq.append(c)
            seen.add(c)
    return uniq


# =========================
# WHERE 構築
# =========================

def build_simple_filter_clause(f: FilterItem, schema_map: Dict[str, str]) -> Tuple[str, List[Any]]:
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
            raise HTTPException(status_code=400, detail=f"BETWEEN value must be [low, high]: {f.field}")
        return f"{expr} BETWEEN ? AND ?", [f.value[0], f.value[1]]

    if f.op == "is_null":
        return f"{expr} IS NULL", []

    if f.op == "is_not_null":
        return f"{expr} IS NOT NULL", []

    raise HTTPException(status_code=400, detail=f"Unsupported operator: {f.op}")


def build_simple_column_clause(cf: ColumnFilterItem, schema_map: Dict[str, str]) -> Tuple[str, List[Any]]:
    if cf.op not in ALLOWED_COLUMN_OPS:
        raise HTTPException(status_code=400, detail=f"Column operator not allowed: {cf.op}")

    left_expr = field_expr(cf.left, schema_map)
    right_expr = field_expr(cf.right, schema_map)
    return f"{left_expr} {cf.op} {right_expr}", []


def build_condition_node(node: ConditionNode, schema_map: Dict[str, str]) -> Tuple[str, List[Any]]:
    parts: List[str] = []
    params: List[Any] = []

    if node.and_ is not None:
        if not node.and_:
            raise HTTPException(status_code=400, detail="and must be non-empty")
        child_sqls = []
        for child in node.and_:
            child_sql, child_params = build_condition(child, schema_map)
            child_sqls.append(f"({child_sql})")
            params.extend(child_params)
        return " AND ".join(child_sqls), params

    if node.or_ is not None:
        if not node.or_:
            raise HTTPException(status_code=400, detail="or must be non-empty")
        child_sqls = []
        for child in node.or_:
            child_sql, child_params = build_condition(child, schema_map)
            child_sqls.append(f"({child_sql})")
            params.extend(child_params)
        return " OR ".join(child_sqls), params

    if node.not_ is not None:
        child_sql, child_params = build_condition(node.not_, schema_map)
        return f"NOT ({child_sql})", child_params

    if node.type == "column" or (node.left and node.right):
        if not node.left or not node.right or not node.op:
            raise HTTPException(status_code=400, detail="column condition requires left, op, right")
        return build_simple_column_clause(
            ColumnFilterItem(left=node.left, op=node.op, right=node.right),
            schema_map
        )

    if node.field and node.op:
        return build_simple_filter_clause(
            FilterItem(field=node.field, op=node.op, value=node.value),
            schema_map
        )

    raise HTTPException(status_code=400, detail="Invalid where condition node")


def build_condition(cond: Condition, schema_map: Dict[str, str]) -> Tuple[str, List[Any]]:
    if isinstance(cond, FilterItem):
        return build_simple_filter_clause(cond, schema_map)
    if isinstance(cond, ColumnFilterItem):
        return build_simple_column_clause(cond, schema_map)
    if isinstance(cond, ConditionNode):
        return build_condition_node(cond, schema_map)
    raise HTTPException(status_code=400, detail="Invalid condition type")


def build_where_from_legacy(
    filters: List[FilterItem],
    column_filters: List[ColumnFilterItem],
    schema_map: Dict[str, str]
) -> Tuple[str, List[Any]]:
    clauses = ["1=1"]
    params: List[Any] = []

    for f in filters:
        sql, p = build_simple_filter_clause(f, schema_map)
        clauses.append(sql)
        params.extend(p)

    for cf in column_filters:
        sql, p = build_simple_column_clause(cf, schema_map)
        clauses.append(sql)
        params.extend(p)

    return " AND ".join(clauses), params


def build_where(
    req: QueryRequest,
    schema_map: Dict[str, str]
) -> Tuple[str, List[Any], str]:
    if req.where is not None:
        sql, params = build_condition(req.where, schema_map)
        return sql, params, "where"

    sql, params = build_where_from_legacy(req.filters, req.column_filters, schema_map)
    return sql, params, "legacy"


# =========================
# 集計用
# =========================

def resolve_summary_columns(schema_map: Dict[str, str]) -> Tuple[str, str]:
    finish_expr = field_expr("確定着順", schema_map)
    odds_expr = field_expr("単勝オッズ", schema_map)
    return finish_expr, odds_expr


def build_metric_sql(metric: MetricItem, schema_map: Dict[str, str]) -> str:
    if metric.agg not in ALLOWED_METRICS:
        raise HTTPException(status_code=400, detail=f"Metric agg not allowed: {metric.agg}")

    finish_expr, odds_expr = resolve_summary_columns(schema_map)

    if metric.agg == "count":
        return "COUNT(*)"

    if metric.agg in {"sum", "avg", "min", "max", "median", "stddev"}:
        if not metric.field:
            raise HTTPException(status_code=400, detail=f"field is required for metric agg={metric.agg}")
        expr = field_expr(metric.field, schema_map)
        if metric.agg == "sum":
            return f"SUM({expr})"
        if metric.agg == "avg":
            return f"ROUND(AVG({expr}), 4)"
        if metric.agg == "min":
            return f"MIN({expr})"
        if metric.agg == "max":
            return f"MAX({expr})"
        if metric.agg == "median":
            return f"MEDIAN({expr})"
        if metric.agg == "stddev":
            return f"ROUND(STDDEV_SAMP({expr}), 4)"

    if metric.agg == "hit_count":
        return f"SUM(CASE WHEN {finish_expr} = 1 THEN 1 ELSE 0 END)"

    if metric.agg == "place_hit_count":
        return f"SUM(CASE WHEN {finish_expr} <= 3 THEN 1 ELSE 0 END)"

    if metric.agg == "win_rate":
        return f"ROUND(AVG(CASE WHEN {finish_expr} = 1 THEN 1.0 ELSE 0.0 END) * 100, 2)"

    if metric.agg == "place_rate":
        return f"ROUND(AVG(CASE WHEN {finish_expr} <= 3 THEN 1.0 ELSE 0.0 END) * 100, 2)"

    if metric.agg == "tan_return":
        return f"ROUND(AVG(CASE WHEN {finish_expr} = 1 THEN {odds_expr} ELSE 0 END) * 100, 2)"

    if metric.agg == "fuku_return":
        raise HTTPException(status_code=400, detail="fuku_return is not available yet because payout column does not exist")

    raise HTTPException(status_code=400, detail=f"Unsupported metric agg: {metric.agg}")


def default_summary_metrics() -> List[MetricItem]:
    return [
        MetricItem(name="n", agg="count"),
        MetricItem(name="win_rate", agg="win_rate"),
        MetricItem(name="place_rate", agg="place_rate"),
        MetricItem(name="tan_return", agg="tan_return"),
    ]


def normalize_metrics(req: QueryRequest) -> List[MetricItem]:
    if req.metrics:
        return req.metrics

    if req.mode in {"summary", "breakdown"}:
        return default_summary_metrics()

    return []


def build_having_sql(having: List[HavingItem], metric_alias_map: Dict[str, str]) -> str:
    if not having:
        return ""

    clauses = []
    for h in having:
        if h.op not in ALLOWED_HAVING_OPS:
            raise HTTPException(status_code=400, detail=f"Having operator not allowed: {h.op}")
        alias = metric_alias_map.get(h.metric)
        if not alias:
            raise HTTPException(status_code=400, detail=f"Unknown having metric: {h.metric}")
        clauses.append(f'{quote_ident(alias)} {h.op} ?')
    return " HAVING " + " AND ".join(clauses)


def build_having_params(having: List[HavingItem]) -> List[Any]:
    return [h.value for h in having]


def build_order_by_sql(
    order_by: List[OrderByItem],
    allowed_targets: Dict[str, str],
    default_order: Optional[str] = None
) -> str:
    if not order_by:
        return f" ORDER BY {default_order}" if default_order else ""

    parts = []
    for item in order_by:
        target = allowed_targets.get(item.target)
        if not target:
            raise HTTPException(status_code=400, detail=f"Unknown order_by target: {item.target}")
        direction = item.direction.upper()
        parts.append(f"{target} {direction}")
    return " ORDER BY " + ", ".join(parts)


def build_aggregate_sql(
    source_table: str,
    schema_map: Dict[str, str],
    where_sql: str,
    req: QueryRequest
) -> Tuple[str, List[Any], List[str], List[MetricItem]]:
    group_fields = normalize_group_by(req.group_by)
    metrics = normalize_metrics(req)

    if not metrics:
        raise HTTPException(status_code=400, detail="metrics is required for aggregate mode")

    group_select_parts: List[str] = []
    group_alias_map: Dict[str, str] = {}
    for i, g in enumerate(group_fields, start=1):
        expr = field_expr(g, schema_map)
        alias = f"group_{i}"
        group_select_parts.append(f"{expr} AS {quote_ident(alias)}")
        group_alias_map[g] = quote_ident(alias)

    metric_select_parts: List[str] = []
    metric_alias_map: Dict[str, str] = {}
    for m in metrics:
        alias = m.name
        if alias in metric_alias_map:
            raise HTTPException(status_code=400, detail=f"Duplicate metric name: {alias}")
        metric_sql = build_metric_sql(m, schema_map)
        metric_select_parts.append(f"{metric_sql} AS {quote_ident(alias)}")
        metric_alias_map[alias] = alias

    select_parts = group_select_parts + metric_select_parts
    if not select_parts:
        raise HTTPException(status_code=400, detail="Nothing to select")

    select_sql = ",\n        ".join(select_parts)
    src = quote_ident(source_table)

    sql = f"""
    SELECT
        {select_sql}
    FROM {src}
    WHERE {where_sql}
    """

    if group_fields:
        group_by_sql = ", ".join([field_expr(g, schema_map) for g in group_fields])
        sql += f"\nGROUP BY {group_by_sql}"

    having_sql = build_having_sql(req.having, metric_alias_map)
    sql += having_sql

    allowed_order_targets: Dict[str, str] = {}
    for g, alias in group_alias_map.items():
        allowed_order_targets[g] = alias
    for metric_alias in metric_alias_map.keys():
        allowed_order_targets[metric_alias] = quote_ident(metric_alias)

    default_order = None
    if "n" in metric_alias_map:
        default_order = f'{quote_ident("n")} DESC'
        if group_fields:
            default_order += ", " + ", ".join(group_alias_map[g] for g in group_fields)

    sql += build_order_by_sql(req.order_by, allowed_order_targets, default_order=default_order)

    safe_limit = max(1, min(req.limit, 10000))
    safe_offset = max(0, req.offset)
    sql += f"\nLIMIT {safe_limit}\nOFFSET {safe_offset}"

    params = build_having_params(req.having)
    result_columns = [m.name for m in metrics]
    return sql, params, group_fields + result_columns, metrics


def build_rows_sql(
    source_table: str,
    schema_map: Dict[str, str],
    where_sql: str,
    req: QueryRequest
) -> str:
    src = quote_ident(source_table)
    select_cols = normalize_select(req.select)

    if select_cols:
        select_sql = ", ".join([
            f"{field_expr(c, schema_map)} AS {quote_ident(resolve_actual_column(c, schema_map))}"
            for c in select_cols
        ])
        allowed_order_targets = {
            c: quote_ident(resolve_actual_column(c, schema_map))
            for c in select_cols
        }
    else:
        select_sql = "*"
        allowed_order_targets = {
            c: field_expr(c, schema_map)
            for c in FIELD_CANDIDATES.keys()
            if c in FIELD_CANDIDATES
        }
        for c in schema_map.keys():
            allowed_order_targets[c] = quote_ident(c)

    sql = f"""
    SELECT {select_sql}
    FROM {src}
    WHERE {where_sql}
    """

    default_order = None
    if "race_date" in schema_map:
        default_order = f'{quote_ident("race_date")} DESC'
    elif "年" in schema_map and "月" in schema_map and "日" in schema_map:
        default_order = f'{quote_ident("年")} DESC, {quote_ident("月")} DESC, {quote_ident("日")} DESC'

    sql += build_order_by_sql(req.order_by, allowed_order_targets, default_order=default_order)

    safe_limit = max(1, min(req.limit, 10000))
    safe_offset = max(0, req.offset)
    sql += f"\nLIMIT {safe_limit}\nOFFSET {safe_offset}"
    return sql


def build_summary_sql(source_table: str, schema_map: Dict[str, str], where_sql: str, req: QueryRequest):
    cloned = req.model_copy(deep=True)
    cloned.mode = "aggregate"
    cloned.metrics = default_summary_metrics()
    cloned.group_by = None
    cloned.having = []
    if not cloned.order_by:
        cloned.order_by = []
    return build_aggregate_sql(source_table, schema_map, where_sql, cloned)


def build_breakdown_sql(source_table: str, schema_map: Dict[str, str], where_sql: str, req: QueryRequest):
    group_fields = normalize_group_by(req.group_by)
    if not group_fields:
        raise HTTPException(status_code=400, detail="group_by is required for breakdown mode")

    cloned = req.model_copy(deep=True)
    cloned.mode = "aggregate"
    cloned.metrics = default_summary_metrics()
    cloned.group_by = group_fields
    cloned.having = []
    return build_aggregate_sql(source_table, schema_map, where_sql, cloned)


# =========================
# API
# =========================

@app.get("/health")
def health():
    db_exists = os.path.exists(DB_PATH)

    source_table = None
    schema_map = {}
    tables = []

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
        "allowed_filter_names": sorted(list(FIELD_CANDIDATES.keys()) + list(schema_map.keys())),
        "allowed_ops": sorted(list(ALLOWED_OPS)),
        "allowed_column_filter_ops": sorted(list(ALLOWED_COLUMN_OPS)),
        "allowed_metric_aggs": sorted(list(ALLOWED_METRICS)),
        "supported_modes": ["summary", "rows", "breakdown", "aggregate"],
        "supports_where_tree": True,
        "supports_select": True,
        "supports_order_by": True,
        "supports_having": True,
        "supports_offset": True,
    }


@app.post("/upload-db")
async def upload_db(file: UploadFile = File(...), x_api_key: Optional[str] = Header(default=None)):
    check_api_key(x_api_key)

    save_dir = os.path.dirname(DB_PATH) or os.getcwd()
    os.makedirs(save_dir, exist_ok=True)

    with open(DB_PATH, "wb") as f:
        shutil.copyfileobj(file.file, f)

    return {"ok": True, "path": DB_PATH, "size": os.path.getsize(DB_PATH)}


@app.post("/query")
def query_backtest(req: QueryRequest, x_api_key: Optional[str] = Header(default=None)):
    check_api_key(x_api_key)
    ensure_db()

    con = get_connection(read_only=True)
    try:
        source_table = resolve_source_table(con)
        schema_map = get_schema_map(con, source_table)

        where_sql, where_params, where_mode = build_where(req, schema_map)

        if req.mode == "summary":
            sql, extra_params, _, _ = build_summary_sql(source_table, schema_map, where_sql, req)

        elif req.mode == "rows":
            sql = build_rows_sql(source_table, schema_map, where_sql, req)
            extra_params = []

        elif req.mode == "breakdown":
            sql, extra_params, _, _ = build_breakdown_sql(source_table, schema_map, where_sql, req)

        elif req.mode == "aggregate":
            sql, extra_params, _, _ = build_aggregate_sql(source_table, schema_map, where_sql, req)

        else:
            raise HTTPException(status_code=400, detail=f"Unknown mode: {req.mode}")

        all_params = where_params + extra_params
        cur = con.execute(sql, all_params)

        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

        if req.mode == "summary":
            row = rows[0] if rows else (0, 0, 0, 0)
            data = {
                "n": int(row[0] or 0),
                "win_rate": float(row[1] or 0),
                "place_rate": float(row[2] or 0),
                "tan_return": float(row[3] or 0),
            }
        else:
            data = [dict(zip(cols, r)) for r in rows]

        return {
            "ok": True,
            "mode": req.mode,
            "where_mode": where_mode,
            "db_path": DB_PATH,
            "source_table": source_table,
            "filters": [f.model_dump() for f in req.filters],
            "column_filters": [f.model_dump() for f in req.column_filters],
            "where": req.where.model_dump(by_alias=True, exclude_none=True) if req.where else None,
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

import os
import shutil
import traceback
from typing import Any, Dict, List, Optional, Literal, Tuple

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

    # ローカル優先
    candidates.extend([
        r"C:\keiba\keiba.duckdb",
        "/app/data/keiba.duckdb",
        os.path.join(os.getcwd(), "keiba.duckdb"),
    ])

    for p in candidates:
        if p and os.path.exists(p):
            return p

    # 存在しなくても、最優先候補を返す
    return env_path or r"C:\keiba\keiba.duckdb"


DB_PATH = resolve_db_path()

# 優先利用するテーブル
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
    op: Literal["=", "!=", ">", ">=", "<", "<=", "in", "like"]
    value: Any


class QueryRequest(BaseModel):
    mode: Literal["summary", "rows", "breakdown"] = "summary"
    filters: List[FilterItem] = Field(default_factory=list)
    group_by: Optional[str] = None
    limit: int = 100


# =========================
# APIキー
# =========================

def check_api_key(x_api_key: Optional[str]):
    # APIキー未設定ならローカル利用前提でスキップ
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
    # {列名: 型名}
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
# 英字でも日本語でも通す
# =========================

FIELD_CANDIDATES: Dict[str, List[str]] = {
    # レース系
    "race_id": ["race_id", "入ID(新)"],
    "horse_id": ["horse_id", "血統登録番号"],
    "horse_no": ["horse_no", "馬番"],
    "horse_name": ["horse_name", "馬名"],
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
    # まず列名そのものが存在する場合
    if field in schema_map:
        return field

    # 別名候補
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

    # 数値扱いしたい列は、文字列型でも TRY_CAST で落ちにくくする
    if field in NUMERIC_FIELD_NAMES and not is_numeric_type(actual_type):
        return f"TRY_CAST({q} AS DOUBLE)"

    return q


def field_label(field: str, schema_map: Dict[str, str]) -> str:
    # 結果表示用のグループ名ラベル
    return resolve_actual_column(field, schema_map)


ALLOWED_OPS = {"=", "!=", ">", ">=", "<", "<=", "in", "like"}


def build_where(filters: List[FilterItem], schema_map: Dict[str, str]) -> Tuple[str, List[Any]]:
    clauses = ["1=1"]
    params: List[Any] = []

    for f in filters:
        if f.op not in ALLOWED_OPS:
            raise HTTPException(status_code=400, detail=f"Operator not allowed: {f.op}")

        expr = field_expr(f.field, schema_map)

        if f.op in {"=", "!=", ">", ">=", "<", "<="}:
            clauses.append(f"{expr} {f.op} ?")
            params.append(f.value)

        elif f.op == "like":
            clauses.append(f"{expr} LIKE ?")
            params.append(f.value)

        elif f.op == "in":
            if not isinstance(f.value, list) or len(f.value) == 0:
                raise HTTPException(
                    status_code=400,
                    detail=f"IN value must be a non-empty list: {f.field}"
                )
            placeholders = ",".join(["?"] * len(f.value))
            clauses.append(f"{expr} IN ({placeholders})")
            params.extend(f.value)

    return " AND ".join(clauses), params


# =========================
# サマリー用の列解決
# =========================

def resolve_summary_columns(schema_map: Dict[str, str]) -> Tuple[str, str]:
    finish_expr = field_expr("確定着順", schema_map)
    odds_expr = field_expr("単勝オッズ", schema_map)
    return finish_expr, odds_expr


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
    group_by: str,
    limit: int
) -> str:
    finish_expr, odds_expr = resolve_summary_columns(schema_map)
    group_expr = field_expr(group_by, schema_map)
    src = quote_ident(source_table)

    return f"""
    SELECT
        {group_expr} AS group_value,
        COUNT(*) AS n,
        ROUND(AVG(CASE WHEN {finish_expr} = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS win_rate,
        ROUND(AVG(CASE WHEN {finish_expr} <= 3 THEN 1.0 ELSE 0.0 END) * 100, 2) AS place_rate,
        ROUND(AVG(CASE WHEN {finish_expr} = 1 THEN {odds_expr} ELSE 0 END) * 100, 2) AS tan_return
    FROM {src}
    WHERE {where_sql}
    GROUP BY 1
    ORDER BY n DESC
    LIMIT {limit}
    """


def build_rows_sql(source_table: str, where_sql: str, limit: int) -> str:
    src = quote_ident(source_table)
    return f"""
    SELECT *
    FROM {src}
    WHERE {where_sql}
    LIMIT {limit}
    """


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

        where_sql, params = build_where(req.filters, schema_map)

        if req.mode == "summary":
            sql = build_summary_sql(source_table, schema_map, where_sql)

        elif req.mode == "rows":
            safe_limit = max(1, min(req.limit, 1000))
            sql = build_rows_sql(source_table, where_sql, safe_limit)

        elif req.mode == "breakdown":
            if not req.group_by:
                raise HTTPException(status_code=400, detail="group_by is required for breakdown mode")
            safe_limit = max(1, min(req.limit, 1000))
            sql = build_breakdown_sql(
                source_table=source_table,
                schema_map=schema_map,
                where_sql=where_sql,
                group_by=req.group_by,
                limit=safe_limit,
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
            "db_path": DB_PATH,
            "source_table": source_table,
            "filters": [f.model_dump() for f in req.filters],
            "group_by": req.group_by,
            "result": data,
        }

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()
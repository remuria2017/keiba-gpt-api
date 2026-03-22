import os
import shutil
from typing import Any, List, Optional, Literal

import duckdb
from fastapi import FastAPI, Header, HTTPException, UploadFile, File
from pydantic import BaseModel, Field

app = FastAPI()

API_KEY = os.getenv("BACKTEST_API_KEY", "")
DB_PATH = os.getenv("DB_PATH", "/app/data/keiba.duckdb")


# DBの列名に対応する許可フィールド
ALLOWED_FIELDS = {
    "race_id": "race_id",
    "horse_id": "horse_id",
    "horse_no": "CAST(horse_no AS INTEGER)",
    "horse_name": "horse_name",
    "course": "course",
    "work_type": "work_type",
    "track_condition": "track_condition",
    "rider_type": "rider_type",
    "lap_1": "CAST(lap_1 AS DOUBLE)",
    "lap_2": "CAST(lap_2 AS DOUBLE)",
    "lap_3": "CAST(lap_3 AS DOUBLE)",
    "lap_4": "CAST(lap_4 AS DOUBLE)",
    "lap_last": "CAST(lap_last AS DOUBLE)",
    "人気": "CAST(人気 AS INTEGER)",
    "確定着順": "CAST(確定着順 AS INTEGER)",
    "単勝オッズ": "CAST(単勝オッズ AS DOUBLE)",
    "クラスコード": "クラスコード",
    "芝・ダ": "芝・ダ",
    "距離": "CAST(距離 AS INTEGER)",
    "馬場状態": "馬場状態",
    "性別": "性別",
    "年齢": "CAST(年齢 AS INTEGER)",
    "頭数": "CAST(頭数 AS INTEGER)",
    "馬番": "CAST(馬番 AS INTEGER)",
    "騎手": "騎手",
    "調教師": "調教師",
    "所属": "所属",
    "場所": "場所",
    "年": "CAST(年 AS INTEGER)",
    "月": "CAST(月 AS INTEGER)",
    "日": "CAST(日 AS INTEGER)",
}

ALLOWED_OPS = {"=", "!=", ">", ">=", "<", "<=", "in", "like"}

SUMMARY_SELECT = """
SELECT
    COUNT(*) AS n,
    ROUND(AVG(CASE WHEN CAST(確定着順 AS INTEGER) = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS win_rate,
    ROUND(AVG(CASE WHEN CAST(確定着順 AS INTEGER) <= 3 THEN 1.0 ELSE 0.0 END) * 100, 2) AS place_rate,
    ROUND(AVG(CASE WHEN CAST(確定着順 AS INTEGER) = 1 THEN CAST(単勝オッズ AS DOUBLE) ELSE 0 END) * 100, 2) AS tan_return
FROM training_analysis
"""

ROW_COLUMNS = """
race_id,
horse_id,
horse_no,
horse_name,
course,
work_type,
lap_1,
lap_2,
lap_3,
lap_4,
lap_last,
人気,
確定着順,
単勝オッズ,
クラスコード,
芝・ダ,
距離,
馬場状態,
性別,
年齢,
頭数,
馬番,
騎手,
調教師,
所属,
場所,
年,
月,
日
"""


class FilterItem(BaseModel):
    field: str
    op: Literal["=", "!=", ">", ">=", "<", "<=", "in", "like"]
    value: Any


class QueryRequest(BaseModel):
    mode: Literal["summary", "rows", "breakdown"] = "summary"
    filters: List[FilterItem] = Field(default_factory=list)
    group_by: Optional[str] = None
    limit: int = 100


def check_api_key(x_api_key: Optional[str]):
    if not API_KEY:
        raise HTTPException(status_code=500, detail="BACKTEST_API_KEY not configured")
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


def ensure_db():
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=500, detail=f"DB not found: {DB_PATH}")


def field_expr(field: str) -> str:
    if field not in ALLOWED_FIELDS:
        raise HTTPException(status_code=400, detail=f"Field not allowed: {field}")
    return ALLOWED_FIELDS[field]


def build_where(filters: List[FilterItem]):
    clauses = ["1=1"]
    params: List[Any] = []

    for f in filters:
        if f.op not in ALLOWED_OPS:
            raise HTTPException(status_code=400, detail=f"Operator not allowed: {f.op}")

        expr = field_expr(f.field)

        if f.op in {"=", "!=", ">", ">=", "<", "<="}:
            clauses.append(f"{expr} {f.op} ?")
            params.append(f.value)

        elif f.op == "like":
            clauses.append(f"{expr} LIKE ?")
            params.append(f.value)

        elif f.op == "in":
            if not isinstance(f.value, list) or len(f.value) == 0:
                raise HTTPException(status_code=400, detail=f"IN value must be a non-empty list: {f.field}")
            placeholders = ",".join(["?"] * len(f.value))
            clauses.append(f"{expr} IN ({placeholders})")
            params.extend(f.value)

    return " AND ".join(clauses), params


@app.get("/health")
def health():
    return {
        "ok": True,
        "db_path": DB_PATH,
        "db_exists": os.path.exists(DB_PATH),
        "allowed_fields": list(ALLOWED_FIELDS.keys()),
        "allowed_ops": sorted(list(ALLOWED_OPS)),
    }


@app.post("/upload-db")
async def upload_db(file: UploadFile = File(...), x_api_key: Optional[str] = Header(default=None)):
    check_api_key(x_api_key)

    os.makedirs("/app/data", exist_ok=True)
    path = "/app/data/keiba.duckdb"

    with open(path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    return {"ok": True, "path": path, "size": os.path.getsize(path)}


@app.post("/query")
def query_backtest(req: QueryRequest, x_api_key: Optional[str] = Header(default=None)):
    check_api_key(x_api_key)
    ensure_db()

    where_sql, params = build_where(req.filters)

    if req.mode == "summary":
        sql = f"""
        {SUMMARY_SELECT}
        WHERE {where_sql}
        """
    elif req.mode == "rows":
        safe_limit = max(1, min(req.limit, 1000))
        sql = f"""
        SELECT {ROW_COLUMNS}
        FROM training_analysis
        WHERE {where_sql}
        LIMIT {safe_limit}
        """
    elif req.mode == "breakdown":
        if not req.group_by:
            raise HTTPException(status_code=400, detail="group_by is required for breakdown mode")
        group_expr = field_expr(req.group_by)
        safe_limit = max(1, min(req.limit, 1000))
        sql = f"""
        SELECT
            {group_expr} AS group_value,
            COUNT(*) AS n,
            ROUND(AVG(CASE WHEN CAST(確定着順 AS INTEGER) = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS win_rate,
            ROUND(AVG(CASE WHEN CAST(確定着順 AS INTEGER) <= 3 THEN 1.0 ELSE 0.0 END) * 100, 2) AS place_rate,
            ROUND(AVG(CASE WHEN CAST(確定着順 AS INTEGER) = 1 THEN CAST(単勝オッズ AS DOUBLE) ELSE 0 END) * 100, 2) AS tan_return
        FROM training_analysis
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY n DESC
        LIMIT {safe_limit}
        """
    else:
        raise HTTPException(status_code=400, detail=f"Unknown mode: {req.mode}")

    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        if req.mode == "summary":
            row = con.execute(sql, params).fetchone()
            data = {
                "n": int(row[0] or 0),
                "win_rate": float(row[1] or 0),
                "place_rate": float(row[2] or 0),
                "tan_return": float(row[3] or 0),
            }
        else:
            cur = con.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            data = [dict(zip(cols, r)) for r in rows]
    finally:
        con.close()

    return {
        "ok": True,
        "mode": req.mode,
        "filters": [f.model_dump() for f in req.filters],
        "group_by": req.group_by,
        "result": data,
    }
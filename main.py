import os
import shutil
from typing import List, Optional, Any

import duckdb
from fastapi import FastAPI, Header, HTTPException, UploadFile, File
from pydantic import BaseModel

app = FastAPI()

API_KEY = os.getenv("BACKTEST_API_KEY", "")
DB_PATH = os.getenv("DB_PATH", r"C:\keiba\keiba.duckdb")


class BacktestRequest(BaseModel):
    course_in: Optional[List[str]] = None
    work_type_like: Optional[str] = None
    lap_last_min: Optional[float] = None
    lap_last_max: Optional[float] = None
    popularity_min: Optional[int] = None
    popularity_max: Optional[int] = None


def check_api_key(x_api_key: str | None):
    if not API_KEY:
        raise HTTPException(status_code=500, detail="BACKTEST_API_KEY not configured")
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


def build_where(req: BacktestRequest):
    clauses = ["1=1"]
    params: List[Any] = []

    if req.course_in:
        placeholders = ",".join(["?"] * len(req.course_in))
        clauses.append(f"course IN ({placeholders})")
        params.extend(req.course_in)

    if req.work_type_like:
        clauses.append("work_type LIKE ?")
        params.append(req.work_type_like)

    if req.lap_last_min is not None:
        clauses.append("CAST(lap_last AS DOUBLE) >= ?")
        params.append(req.lap_last_min)

    if req.lap_last_max is not None:
        clauses.append("CAST(lap_last AS DOUBLE) <= ?")
        params.append(req.lap_last_max)

    if req.popularity_min is not None:
        clauses.append("CAST(人気 AS INTEGER) >= ?")
        params.append(req.popularity_min)

    if req.popularity_max is not None:
        clauses.append("CAST(人気 AS INTEGER) <= ?")
        params.append(req.popularity_max)

    return " AND ".join(clauses), params


@app.get("/health")
def health():
    return {
        "ok": True,
        "db_path": DB_PATH,
        "db_exists": os.path.exists(DB_PATH),
    }


@app.post("/backtest/run")
def run_backtest(req: BacktestRequest, x_api_key: str | None = Header(default=None)):
    check_api_key(x_api_key)

    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=500, detail=f"DB not found: {DB_PATH}")

    where_sql, params = build_where(req)

    sql = f"""
    SELECT
        COUNT(*) AS n,
        ROUND(AVG(CASE WHEN CAST(確定着順 AS INTEGER) = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS win_rate,
        ROUND(AVG(CASE WHEN CAST(確定着順 AS INTEGER) <= 3 THEN 1.0 ELSE 0.0 END) * 100, 2) AS place_rate,
        ROUND(AVG(CASE WHEN CAST(確定着順 AS INTEGER) = 1 THEN CAST(単勝オッズ AS DOUBLE) ELSE 0 END) * 100, 2) AS tan_return
    FROM training_analysis
    WHERE {where_sql}
    """

    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        row = con.execute(sql, params).fetchone()
    finally:
        con.close()

    return {
        "ok": True,
        "filters": req.model_dump(),
        "result": {
            "n": int(row[0] or 0),
            "win_rate": float(row[1] or 0),
            "place_rate": float(row[2] or 0),
            "tan_return": float(row[3] or 0),
        }
    }

@app.post("/upload-db")
async def upload_db(file: UploadFile = File(...), x_api_key: str | None = Header(default=None)):
    check_api_key(x_api_key)

    os.makedirs("/app/data", exist_ok=True)
    path = "/app/data/keiba.duckdb"

    with open(path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    return {"ok": True, "path": path, "size": os.path.getsize(path)}
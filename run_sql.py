import duckdb
from pathlib import Path

# ←ここは自分の環境に合わせて
db_path = r"C:\keiba\作業中\keiba.duckdb"
sql_path = r"C:\keiba\作業中\training_latest_one_supplement.sql"

# SQL読み込み
sql = Path(sql_path).read_text(encoding="utf-8")

# DB接続
con = duckdb.connect(db_path)

# 実行
con.execute(sql)

print("SQL実行完了")

# 確認
print("元テーブル:")
print(con.execute("SELECT COUNT(*) FROM backtest_with_training").fetchone())

print("補完後:")
print(con.execute("SELECT COUNT(*) FROM backtest_with_training_completed").fetchone())

# サンプル
rows = con.execute("""
SELECT
  race_id,
  horse_no,
  work_date_full,
  comment,
  with_horse,
  lap_raw_1,
  lap_raw_2,
  lap_raw_3,
  lap_raw_4,
  lap_raw_5
FROM backtest_with_training_completed
LIMIT 5
""").fetchall()

print("サンプル:")
for r in rows:
    print(r)

con.close()
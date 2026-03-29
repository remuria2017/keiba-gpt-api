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
# ===== cleanテーブルを短評込みで作り直す =====
con.execute("""
CREATE OR REPLACE TABLE backtest_with_training_clean AS
SELECT
  b.*,
  s.comment,
  s.trend,
  s.is_previous,
  s.memo_only,
  s.work_date,
  s.lap_key_text,
  s.lap_raw_1,
  s.lap_raw_2,
  s.lap_raw_3,
  s.lap_raw_4,
  s.lap_raw_5,
  s.position_text,
  s.memo_text,
  s.with_horse,
  s.with_horse_class,
  s.with_result,
  s.with_diff,
  s.with_chase_diff,
  s.with_side,
  s.with_text,
  s.race_date_text,
  s.race_date_iso,
  s.race_name,
  s.conditions,
  s.distance AS training_distance,
  s.surface AS training_surface,
  s.turn AS training_turn,
  s.weather AS training_weather,
  s.going AS training_going,
  s.race_note
FROM backtest_with_training b
LEFT JOIN training_latest_one_supplement s
  ON CAST(b.race_id AS VARCHAR) = s.race_id
 AND LPAD(CAST(b.horse_no AS VARCHAR), 2, '0') = s.horse_no
 AND b.work_date_full = s.work_date_full;
""")

print("cleanテーブル再作成完了")

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

    # ===== 列確認 =====
cols = con.execute("PRAGMA table_info('backtest_with_training_clean')").fetchall()
print("columns:")
for c in cols:
    print(c[1])

# ===== 短評確認 =====
rows = con.execute("""
SELECT comment, trend, memo_text
FROM backtest_with_training_clean
WHERE comment IS NOT NULL
LIMIT 5
""").fetchall()

print("短評サンプル:")
for r in rows:
    print(r)

con.close()
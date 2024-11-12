import duckdb

from utils import data_dir, db_path

con = duckdb.connect(db_path)
for csv in data_dir.glob("*.csv"):
    rel = con.read_csv(csv, date_format="%Y%m%d")
    con.execute(f"create table {csv.stem} as select * from rel")

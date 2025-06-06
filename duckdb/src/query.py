import duckdb
con = duckdb.connect("/app/volume/datawarehouse.duckdb")
result = con.execute("SELECT * FROM staging.player_attacking_fact;").fetchall()
print(result)
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode, col, expr, substring
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from espn_to_refined import espn_team_load
from datetime import date
import json

spark = SparkSession.builder \
    .appName("esspn to minio") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

def player_load():
    from_map = [
    "premierleague/24_25/player",
        "laliga/24_25/player",
        "ligue1/24_25/player",
        "bundesliga/24_25/player",
        "seriea/24_25/player"
    ]
    attribute_map = ["attacking", "defending", "overall"]
    for source in from_map:
        today = date.today()
        for attribute in attribute_map:
            df = spark.read.json(f"s3a://trusted/{source}/{attribute}/year={today.year}/month={today.month}/day={today.day}")
            if attribute == "attacking":
                for col_name in df.columns:
                    if col_name not in {"player_name", "team_name"}:
                        target_type = "double" if col_name in {"SCA90", "GCA90", "SCA", "GCA"} else "int"
                        df = df.withColumn(col_name, col(col_name).cast(target_type))
                
                df = df.select(col("team_name"), col("player_name"), col("SCA"), 
                       col("SCA90"), col("GCA"), col("GCA90"), col("passlive"), col("season"))
                df.printSchema()
                df.write.mode("append").parquet(f"s3a://refined/player/{attribute}/year={today.year}/month={today.month}/day={today.day}")
            
            elif attribute == "defending":
                for col_name in df.columns:
                    if col_name not in {"player_name", "team_name"}:
                        target_type = "double" if col_name in {"tkl_pct"} else "int"
                        df = df.withColumn(col_name, col(col_name).cast(target_type))
                
                df = df.select(col("team_name"), col("player_name"), col("tkl"), 
                       col("tklWon"), col("tkl_pct"), col("interception"), col("blocks"), col("errors"), col("season"))
                df.printSchema()
                df.write.mode("append").parquet(f"s3a://refined/player/{attribute}/year={today.year}/month={today.month}/day={today.day}")
            else:
                for col_name in df.columns:
                    if col_name not in {"player_name", "team_name"}:
                        target_type = "double" if col_name in {"xG", "xAG"} else "int"
                        df = df.withColumn(col_name, col(col_name).cast(target_type))
                
                df = df.select(col("team_name"), col("player_name"), col("match_played"), 
                       col("age"), col("nationality"), col("minutes"), col("position"), col("goal"), col("assist"),
                       col("xG"), col("xAG"), col("season"))
                df.printSchema()
                df.write.mode("append").parquet(f"s3a://refined/player/{attribute}/year={today.year}/month={today.month}/day={today.day}")

def team_load(spark):
    result = espn_team_load(spark)
    from_map = [
    "premierleague/24_25/team",
        "laliga/24_25/team",
        "ligue1/24_25/team",
        "bundesliga/24_25/team",
        "seriea/24_25/team"
    ]
    for team in from_map:
        df_espn = result[0]
        today = date.today()
        df_team = spark.read.json(f"s3a://trusted/{team}/year={today.year}/month={today.month}/day={today.day}")
        for col_name in df_team.columns:
            df_team = df_team.withColumn(col_name, col(col_name).cast("int")) if col_name not in {"manager", "team_name"} else df_team
        df_join = df_team.join(df_espn, on="team_name", how="outer")
        df_join = df_join.select(col("team_name"), col("league_name"), col("point"), col("rank"), col("manager"),
                                 col("playedGames"), col("won"), col("draw"), col("lost"), col("goalDifference"),
                                 col("goalsFor"), col("goalsAgainst"), col("season"))
        df_join.printSchema()
        df_join.write.mode("append").json(f"s3a://refined/team/year={today.year}/month={today.month}/day={today.day}")
        result.pop(0)

player_load()
team_load(spark)
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode, col, expr, substring
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from espn_to_refined import team_load
from datetime import date
import json

spark = SparkSession.builder \
    .appName("esspn to minio") \
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
                    df = df.withColumn(col_name, col(col_name).cast("int")) if col_name not in {"player_name", "team_name"} else df
                    df = df.withColumn(col_name, col(col_name).cast("double")) if col_name in {"SCA90", "GCA90"} else df
                df.printSchema()
                df.write.mode("append").parquet(f"s3a://refined/player/{attribute}/year={today.year}/month={today.month}/day={today.day}")
            
            elif attribute == "defending":
                for col_name in df.columns:
                    df = df.withColumn(col_name, col(col_name).cast("int")) if col_name not in {"player_name", "team_name"} else df
                    df = df.withColumn(col_name, col(col_name).cast("double")) if col_name in {"tkl_pct"} else df
                df.printSchema()
                df.write.mode("append").parquet(f"s3a://refined/player/{attribute}/year={today.year}/month={today.month}/day={today.day}")
            else:
                for col_name in df.columns:
                    df = df.withColumn(col_name, col(col_name).cast("int")) if col_name not in {"player_name", "team_name", "nationality", "position"} else df
                    df = df.withColumn(col_name, col(col_name).cast("double")) if col_name in {"xG", "xAG"} else df
                df.printSchema()
                df.write.mode("append").parquet(f"s3a://refined/player/{attribute}/year={today.year}/month={today.month}/day={today.day}")

def team_load():
    result = team_load(spark)
    from_map = {
    "premierleague/24_25/team":"premierleague/24_25/league",
        "laliga/24_25/team":"laliga/24_25/league",
        "ligue1/24_25/team":"ligue1/24_25/league",
        "bundesliga/24_25/team":"bundesliga/24_25/league",
        "seriea/24_25/team":"seriea/24_25/league"
    }
    for team, league in from_map:
        df_espn = result[0]
        today = date.today()
        df_team = spark.read.json(f"s3a://trusted/{team}/year={today.year}/month={today.month}/day={today.day}")
        for col_name in df_team.columns:
            df_team = df_team.withColumn(col_name, col(col_name).cast("int")) if col_name not in {"manager", "team_name"} else df_team
        df_join = df_team.join(df_espn, on="team_name", how="outer")
        df_join.printSchema()
        df_join.write.mode("append").parquet(f"s3a://refined/team/year={today.year}/month={today.month}/day={today.day}")

player_load()
team_load()
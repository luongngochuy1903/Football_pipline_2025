from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode, col, expr, concat_ws, substring, when
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from modules.module_job import get_latest_partition_date
from modules.mapping import mappping_team
from datetime import date
import json

spark = SparkSession.builder \
    .appName("esspn to minio") \
    .getOrCreate()

def espn_team_load(spark):
    result = []
    from_map = [
    "premierleague/24_25/league",
        "laliga/24_25/league",
        "ligue1/24_25/league",
        "bundesliga/24_25/league",
        "seriea/24_25/league"
    ]
    for source in from_map:
        today = date.today()
        df = spark.read.json(f"s3a://trusted/{source}/year={today.year}/month={today.month}/day={today.day}")
        df = df.withColumn("standing", explode(col("standings")))
        df_filter = df.filter(col("standing.type") == "TOTAL") \
                        .select(explode(col("standing.table")).alias("table"), col("competition.name").alias("league_name"))
        df_final = df_filter.select(
            col("table.team.shortName").alias("team_name"), col("league_name"),
            col("table.playedGames").cast("int").alias("playedGames"), col("table.won").cast("int").alias("won"), 
            col("table.draw").cast("int").alias("draw"), col("table.lost").cast("int").alias("lost"), 
            col("table.goalDifference").cast("int").alias("goalDifference"), col("table.goalsFor").cast("int").alias("goalsFor"), 
            col("table.goalsAgainst").cast("int").alias("goalsAgainst")
        )
        for target, source in mappping_team.items():
            for item in source:
                df_final = df_final.withColumn(
                "team_name",
                when(col("team_name") == item, target).otherwise(col("team_name"))
        )
        df_final.printSchema()
        result.append(df_final)
    return result

def league_load():
    from_map = [
    "premierleague/24_25/league",
        "laliga/24_25/league",
        "ligue1/24_25/league",
        "bundesliga/24_25/league",
        "seriea/24_25/league"
    ]
    today = date.today()
    checking = get_latest_partition_date("refined", "league")
    if checking == None:
        for source in from_map:
            df = spark.read.json(f"s3a://trusted/{source}/year={today.year}/month={today.month}/day={today.day}")
            df = df.withColumn("seasons", concat_ws("-", substring(col("season.startDate"), 1, 4), substring(col("season.endDate"), 1, 4)))
            df = df.select(col("area.name").alias("country"), col("competition.name").alias("league_name"), 
                           col("competition.type").alias("type"), col("seasons")
                           )
            df.printSchema()
            df.write.mode("append").parquet(f"s3a://refined/league/year={today.year}/month={today.month}/day={today.day}")
    else:
        print("League information is already in the table")

def season_load():
    from_map = [
    "premierleague/24_25/league",
        "laliga/24_25/league",
        "ligue1/24_25/league",
        "bundesliga/24_25/league",
        "seriea/24_25/league"
    ]
    today = date.today()
    for source in from_map:
        df = spark.read.json(f"s3a://trusted/{source}/year={today.year}/month={today.month}/day={today.day}")
        df = df.withColumn("seasons", concat_ws("-", substring(col("season.startDate"), 1, 4), substring(col("season.endDate"), 1, 4)))
        df = df.select(col("seasons"), col("season.startDate").cast("date").alias("startDate"),
                       col("season.endDate").cast("date").alias("endDate"))
        df.printSchema()
        df.write.mode("overwrite").parquet(f"s3a://refined/season/year={today.year}/month={today.month}/day={today.day}")
        print("Compete loading to Refined/season")

league_load()
season_load()
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from modules.module_job import checking_duplicated
from modules.data_quality import checking_fbref
from datetime import date
import json

spark = SparkSession.builder \
    .appName("fbref to minio") \
    .getOrCreate()

#reading team info from shared_data
def team_read_from_json():
    shared_map = {
    "pl": "/opt/shared/premier_leagues/24_25/team_info/teams_overall.json",
    "la": "/opt/shared/laliga/24_25/team_info/teams_overall.json",
    "fl": "/opt/shared/ligue 1/24_25/team_info/teams_overall.json",
    "bun": "/opt/shared/bundesliga/24_25/team_info/teams_overall.json",
    "se": "/opt/shared/seria/24_25/team_info/teams_overall.json"
}
    path_map = {
    "pl": "premierleague/24_25/team",
    "la": "laliga/24_25/team",
    "fl": "ligue1/24_25/team",
    "bun": "bundesliga/24_25/team",
    "se": "seriea/24_25/team"
}

    for league_code, shared in shared_map.items():
        df = spark.read.option("multiline","true").json(shared)
        df.printSchema()
        for key, path in path_map.items():
                if key == league_code:
                    today = date.today()
                    if not checking_duplicated(spark, "trusted", df, path, ["point"]) and checking_fbref(df, spark, key, path):
                        print("Starting loading to Raw Zone task")
                        df = df.withColumn("year", lit(today.year)) \
                        .withColumn("month", lit(today.month)) \
                        .withColumn("day", lit(today.day))
                        df.printSchema()
                        df.write.mode("append").partitionBy("year","month","day").json(f"s3a://raw/{path}")
                    else:
                        print(f"There is no update data to flow in Trusted/{path} at {today.day}/{today.month}/{today.year}")
                    break
    print("Pipeline: fbhref-team: Load to RAW ZONE/news minio completed !")

#reading player info from shared_data
def player_read_from_json():
    shared_map = {
    "pl": "/opt/shared/premier_leagues/24_25/player_info",
    "la": "/opt/shared/laliga/24_25/player_info",
    "fl": "/opt/shared/ligue 1/24_25/player_info",
    "bun": "/opt/shared/bundesliga/24_25/player_info",
    "se": "/opt/shared/seria/24_25/player_info"
}
    attribute_map = ["attacking", "defending", "overall"]
    path_map = {
    "pl": "premierleague/24_25/player",
    "la": "laliga/24_25/player",
    "fl": "ligue1/24_25/player",
    "bun": "bundesliga/24_25/player",
    "se": "seriea/24_25/player"
}

    for league_code, shared in shared_map.items():
        for attribute in attribute_map:
            df = spark.read.option("multiline","true").json(f"{shared}/{attribute}/player_stat.json")
            df.printSchema()
            for key, path in path_map.items():
                if key == league_code:
                    today = date.today()
                    complete_path = f"{path}/{attribute}"
                    if not checking_duplicated(spark, "trusted", df, complete_path, ["SCA", "GCA"]) and checking_fbref(df, spark, key, complete_path):
                        print("Starting loading to Raw Zone task")
                        df = df.withColumn("year", lit(today.year)) \
                        .withColumn("month", lit(today.month)) \
                        .withColumn("day", lit(today.day))
                        df.printSchema()
                        df.write.mode("append").partitionBy("year","month","day").json(f"s3a://raw/{path}/{attribute}")
                    else:
                        print(f"There is no update data to flow in Trusted/{path} at {today.day}/{today.month}/{today.year}")
                    break
    print("Pipeline: fbhref-player: Load to RAW ZONE/player minio completed !")

team_read_from_json()
player_read_from_json()
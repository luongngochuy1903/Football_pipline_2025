from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from crawling.crawlData_espn import crawl_league_news, crawl_team_season
from modules.module_job import checking_duplicated
from datetime import date
import json
import hashlib
import logging

# Tạo SparkSession với cấu hình S3 (MinIO)
spark = SparkSession.builder \
    .appName("espn to minio") \
    .getOrCreate()

def league_read_from_json():
    crawl_team_season()
    league_lists = {
    "pl": "/opt/shared/premier_leagues/24_25/team_info/team_season.json",
    "la": "/opt/shared/laliga/24_25/team_info/team_season.json",
    "fl": "/opt/shared/ligue 1/24_25/team_info/team_season.json",
    "bun": "/opt/shared/bundesliga/24_25/team_info/team_season.json",
    "se": "/opt/shared/seria/24_25/team_info/team_season.json"
}
    path_map = {
    "pl": "premierleague/24_25/league",
    "la": "laliga/24_25/league",
    "fl": "ligue1/24_25/league",
    "bun": "bundesliga/24_25/league",
    "se": "seriea/24_25/league"
}

    for league_code, address in league_lists.items():
        df = spark.read.option("multiline", "true").json(address)
        print("bắt đầu df chưa transformed")
        df.printSchema()
        # df.show(3)
        for key, path in path_map.items():
            if key == league_code:
                today = date.today()
                if not checking_duplicated(spark, "trusted", df, path, ["season.currentMatchday"]):
                    print("Starting loading to Raw Zone task")
                    df = df.withColumn("year", lit(today.year)) \
                    .withColumn("month", lit(today.month)) \
                    .withColumn("day", lit(today.day))
                    print("Df đã transformed")
                    df.printSchema()
                    df.write.mode("append").partitionBy("year","month","day").json(f"s3a://raw/{path}")
                else:
                    logging.info(f"There is no update data to flow in Trusted/{path} at {today.day}/{today.month}/{today.year}")
                break
    print("Pipeline: espn-league: Load to RAW ZONE/league minio completed !")

league_read_from_json()
spark.stop()
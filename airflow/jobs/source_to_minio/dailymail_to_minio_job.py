from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from modules.module_job import checking_duplicated
from datetime import date
import json
import hashlib
import logging


spark = SparkSession.builder \
    .appName("esspn to minio") \
    .getOrCreate()

def news_read_from_json():
    shared_map = {
    "pl": "/opt/shared/news/dailymail/premierleague_dailymailnews.json",
    "la": "/opt/shared/news/dailymail/la-liga_dailymailnews.json",
    "fl": "/opt/shared/news/dailymail/ligue-1_dailymailnews.json",
    "bun": "/opt/shared/news/dailymail/bundesliga_dailymailnews.json",
    "se": "/opt/shared/news/dailymail/serie-a_dailymailnews.json"
}
    path_map = {
    "pl": "premierleague/24_25/news/dailymail",
    "la": "laliga/24_25/news/dailymail",
    "fl": "ligue1/24_25/news/dailymail",
    "bun": "bundesliga/24_25/news/dailymail",
    "se": "seriea/24_25/news/dailymail"
}

    for league_code, shared in shared_map.items():
        df = spark.read.json(shared)
        df.printSchema()
        df.show(3)
        for key, path in path_map.items():
                if key == league_code:
                    if not checking_duplicated(spark, "trusted", df, path, ["Url"]):
                        today = date.today()
                        print("Starting loading to Raw Zone task")
                        df = df.withColumn("year", lit(today.year)) \
                        .withColumn("month", lit(today.month)) \
                        .withColumn("day", lit(today.day))
                        df.printSchema()
                        df.write.mode("append").partitionBy("year","month","day").json(f"s3a://raw/{path}")
                    else:
                        print(f"There is no update data to flow in Trusted/{path} at {today.day}/{today.month}/{today.year}")
                    break
    print("Pipeline: Dailymail-news: Load to RAW ZONE/news minio completed !")


news_read_from_json()
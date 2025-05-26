from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from datetime import date
import json

spark = SparkSession.builder \
    .appName("esspn to minio") \
    .getOrCreate()

def payrolls_read_from_json():
    shared_map = {
    "pl": "/opt/shared/premier_leagues/24_25/team_info/team_finance/club_payrolls.json",
    "la": "/opt/shared/laliga/24_25/team_info/team_finance/club_payrolls.json",
    "fl": "/opt/shared/ligue 1/24_25/team_info/team_finance/club_payrolls.json",
    "bun": "/opt/shared/bundesliga/24_25/team_info/team_finance/club_payrolls.json",
    "se": "/opt/shared/seria/24_25/team_info/team_finance/club_payrolls.json"
}
    path_map = {
    "pl": "s3a://raw/premierleague/24_25/finance/payrolls",
    "la": "s3a://raw/laliga/24_25/finance/payrolls",
    "fl": "s3a://raw/ligue1/24_25/finance/payrolls",
    "bun": "s3a://raw/bundesliga/24_25/finance/payrolls",
    "se": "s3a://raw/seriea/24_25/finance/payrolls"
}

    for league_code, shared in shared_map.items():
        df = spark.read.option("multiline","true").json(shared)
        df.printSchema()
        for key, path in path_map.items():
            if key == league_code:
                today = date.today()
                df = df.withColumn("year", lit(today.year)) \
                    .withColumn("month", lit(today.month)) \
                    .withColumn("day", lit(today.day))
                df.write.mode("append").partitionBy("year","month","day").csv(path)
                break
    print("Load to RAW ZONE/team minio completed !")

def transfer_read_from_json():
    shared_map = {
    "pl": "/opt/shared/premier_leagues/24_25/team_info/team_finance/club_transfer.json",
    "la": "/opt/shared/laliga/24_25/team_info/team_finance/club_transfer.json",
    "fl": "/opt/shared/ligue 1/24_25/team_info/team_finance/club_transfer.json",
    "bun": "/opt/shared/bundesliga/24_25/team_info/team_finance/club_transfer.json",
    "se": "/opt/shared/seria/24_25/team_info/team_finance/club_transfer.json"
}
    path_map = {
    "pl": "s3a://raw/premierleague/24_25/finance/transfer",
    "la": "s3a://raw/laliga/24_25/finance/transfer",
    "fl": "s3a://raw/ligue1/24_25/finance/transfer",
    "bun": "s3a://raw/bundesliga/24_25/finance/transfer",
    "se": "s3a://raw/seriea/24_25/finance/transfer"
}

    for league_code, shared in shared_map.items():
        df = spark.read.option("multiline","true").json(shared)
        df.printSchema()
        for key, path in path_map.items():
            if key == league_code:
                today = date.today()
                df = df.withColumn("year", lit(today.year)) \
                    .withColumn("month", lit(today.month)) \
                    .withColumn("day", lit(today.day))
                df.write.mode("append").partitionBy("year","month","day").csv(path)
                break
    print("Load to RAW ZONE/team minio completed !")

def salary_read_from_json():
    shared_map = {
    "pl": "/opt/shared/premier_leagues/24_25/player_info/salary/player_salary.json",
    "la": "/opt/shared/laliga/24_25/player_info/salary/player_salary.json",
    "fl": "/opt/shared/ligue 1/24_25/player_info/salary/player_salary.json",
    "bun": "/opt/shared/bundesliga/24_25/player_info/salary/player_salary.json",
    "se": "/opt/shared/seria/24_25/player_info/salary/player_salary.json"
}
    path_map = {
    "pl": "s3a://raw/premierleague/24_25/finance/salary",
    "la": "s3a://raw/laliga/finance/24_25/salary",
    "fl": "s3a://raw/ligue1/finance/24_25/salary",
    "bun": "s3a://raw/bundesliga/24_25/finance/salary",
    "se": "s3a://raw/seriea/24_25/finance/salary"
}

    for league_code, shared in shared_map.items():
        df = spark.read.option("multiline","true").json(shared)
        df.printSchema()
        for key, path in path_map.items():
            if key == league_code:
                today = date.today()
                df = df.withColumn("year", lit(today.year)) \
                    .withColumn("month", lit(today.month)) \
                    .withColumn("day", lit(today.day))
                df.write.mode("append").partitionBy("year","month","day").csv(path)
                break
    print("Load to RAW ZONE/player minio completed !")

payrolls_read_from_json()
transfer_read_from_json()
salary_read_from_json()
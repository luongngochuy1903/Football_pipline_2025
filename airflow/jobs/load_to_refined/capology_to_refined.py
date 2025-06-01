from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode, col, expr
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from datetime import date
import json

spark = SparkSession.builder \
    .appName("esspn to minio") \
    .getOrCreate()

def payrolls_load():
    from_map = [
    "premierleague/24_25/finance/payrolls",
        "laliga/24_25/finance/payrolls",
        "ligue1/24_25/finance/payrolls",
        "bundesliga/24_25/finance/payrolls",
        "seriea/24_25/finance/payrolls"
    ]
    for source in from_map:
        today = date.today()
        df = spark.read.json(f"s3a://trusted/{source}/year={today.year}/month={today.month}/day={today.day}")
        for col_name in df.columns:
            if col_name not in  {"team", "season", "league"}:
                df = df.withColumn(col_name, col(col_name).cast("double"))

        df.printSchema()
        df.write.mode("append").parquet(f"s3a://refined/payrolls/year={today.year}/month={today.month}/day={today.day}")

def transfer_load():
    from_map = [
    "premierleague/24_25/finance/transfer",
        "laliga/24_25/finance/transfer",
        "ligue1/24_25/finance/transfer",
        "bundesliga/24_25/finance/transfer",
        "seriea/24_25/finance/transfer"
    ]
    for source in from_map:
        today = date.today()
        df = spark.read.json(f"s3a://trusted/{source}/year={today.year}/month={today.month}/day={today.day}")
        for col_name in df.columns:
            if col_name not in  {"team", "season", "league"}:
                df = df.withColumn(col_name, col(col_name).cast("double"))

        df.printSchema()
        df.write.mode("append").parquet(f"s3a://refined/transfer/year={today.year}/month={today.month}/day={today.day}")

def salary_load():
    from_map = [
    "premierleague/24_25/finance/salary",
        "laliga/24_25/finance/salary",
        "ligue1/24_25/finance/salary",
        "bundesliga/24_25/finance/salary",
        "seriea/24_25/finance/salary"
    ]
    for source in from_map:
        today = date.today()
        df = spark.read.json(f"s3a://trusted/{source}/year={today.year}/month={today.month}/day={today.day}")
        for col_name in df.columns:
            if col_name not in {"player_name", "team", "league", "season"}:
                df = df.withColumn(col_name, col(col_name).cast("double"))

        df.printSchema()
        df.write.mode("append").parquet(f"s3a://refined/salary/year={today.year}/month={today.month}/day={today.day}")

salary_load()
transfer_load()
payrolls_load()
    
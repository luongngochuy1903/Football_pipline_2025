from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode, col, expr, when
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from modules.mapping import mapping_league
from datetime import date
import json

spark = SparkSession.builder \
    .appName("capology to Refined") \
    .getOrCreate()

#transforming Payrolls to Refined
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

        df = df.select(col("team"), col("league"), col("gross/w"), col("gross/y"), col("gross_keeper"),
                  col("gross_defense"), col("gross_midfield"), col("gross_forward"), col("season"))
        for target, source in mapping_league.items():
            for item in source:
                df = df.withColumn(
                "league",
                when(col("league") == item, target).otherwise(col("league"))
        )
        df.printSchema()
        df.write.mode("append").parquet(f"s3a://refined/payrolls/year={today.year}/month={today.month}/day={today.day}")

#transforming transfer to Refined
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

        df = df.select(col("team"), col("league"), col("incomes"), col("expense"), col("balance"),
                  col("season"))
        for target, source in mapping_league.items():
            for item in source:
                df = df.withColumn(
                "league",
                when(col("league") == item, target).otherwise(col("league"))
        )
        df.printSchema()
        df.write.mode("append").parquet(f"s3a://refined/transfer/year={today.year}/month={today.month}/day={today.day}")

#transforming salary to Refined
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

        df = df.select(col("player_name"), col("team"), col("league"), 
                       col("gross/w"), col("gross/y"), col("signed"), col("expiration"),
                       col("gross_remaining"), col("release_clause"), col("season"))
        for target, source in mapping_league.items():
            for item in source:
                df = df.withColumn(
                "league",
                when(col("league") == item, target).otherwise(col("league"))
        )
        df.printSchema()
        df.write.mode("append").parquet(f"s3a://refined/salary/year={today.year}/month={today.month}/day={today.day}")

salary_load()
transfer_load()
payrolls_load()
    
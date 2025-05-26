from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode, col, expr
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from datetime import date
import json

spark = SparkSession.builder \
    .appName("esspn to minio") \
    .getOrCreate()

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
            if col_name not in ["player_name", "team"]:
                df = df.withColumn(col_name, col(col_name).cast("double"))

        df.printSchema()
        df.write.mode("append").parquet(f"s3a://refined/salary/year={today.year}/month={today.month}/day={today.day}")
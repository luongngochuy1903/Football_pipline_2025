from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode, col, expr
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from datetime import date
import json

spark = SparkSession.builder \
    .appName("dailymail to refined") \
    .getOrCreate()

#transforming news to Refined
def news_load():
    from_map = [
    "premierleague/24_25/news/dailymail",
        "laliga/24_25/news/dailymail",
        "ligue1/24_25/news/dailymail",
        "bundesliga/24_25/news/dailymail",
        "seriea/24_25/news/dailymail"
    ]
    for source in from_map:
        today = date.today()
        df = spark.read.json(f"s3a://trusted/{source}/year={today.year}/month={today.month}/day={today.day}")
        df.printSchema()
        final_df = df.select(
                    col("headline"), 
                    col("Published").cast("timestamp").alias("published"), 
                    expr("array_join(categories, ',')").alias("categories"), 
                    col("url"))
        final_df.printSchema()
        final_df.write.mode("append").parquet(f"s3a://refined/news/year={today.year}/month={today.month}/day={today.day}")
    
news_load()
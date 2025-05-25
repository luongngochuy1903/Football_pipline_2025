from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode, col, expr
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from datetime import date
import json

spark = SparkSession.builder \
    .appName("esspn to minio") \
    .getOrCreate()

def news_load():
    from_map = {
    "pl": "premierleague/24_25/news/dailymail",
    "la": "laliga/24_25/news/dailymail",
    "fl": "ligue1/24_25/news/dailymail",
    "bun": "bundesliga/24_25/news/dailymail",
    "se": "seriea/24_25/news/dailymail"
}
    for source in from_map:
        today = date.today()
        df = spark.read.json(f"trusted/{source}/year={today.year}/month={today.month}/day={today.day}")
        df_explode = df.select(explode(col("articles")).alias("articles"))
        final_df = df_explode.withColumn("url", lit(None)) \
            .select(col("articles.headline").alias("headline"), 
                    col("articles.published").alias("published"), 
                    expr("array_join(transform(articles.categories, x -> x.description), ',')").alias("categories"), 
                    col("url"))
        final_df.printSchema()
        final_df.write.mode("append").parquet(f"refined/news/year={today.year}/month={today.month}/day={today.day}")

    
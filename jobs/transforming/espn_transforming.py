from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from modules.module_null import handle_null
from datetime import date

spark = SparkSession.builder \
    .appName("esspn to minio") \
    .getOrCreate()

def transformEspn():
    league_map = [
        "premierleague/24_25/league",
        "laliga/24_25/league",
        "ligue1/24_25/league",
        "bundesliga/24_25/league",
        "seriea/24_25/league"
    ]
    for league in league_map:
        today = date.today()
        df_league = spark.read.option("multiline","true").json(f"s3a://raw/{league}/year={today.year}/month={today.month}/day={today.day}")
        df_league = df_league.select("area", "competition", "season", "standings")
        transform_df = handle_null(spark, df_league)
        print("Starting transforming to Trusted Zone task")
        transform_df = transform_df.withColumn("year", lit(today.year)) \
        .withColumn("month", lit(today.month)) \
        .withColumn("day", lit(today.day))
        transform_df.printSchema()
        transform_df.write.mode("append").partitionBy("year", "month", "day").json(f"s3a://trusted/{league}")
        print(f"Complete loading to Trusted/{league} !")

transformEspn()
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from modules.module_null import handle_null
from datetime import date

spark = SparkSession.builder \
    .appName("esspn to minio") \
    .getOrCreate()

def transformPayroll():
    team_map = [
        "premierleague/24_25/team",
        "laliga/24_25/team",
        "ligue1/24_25/team",
        "bundesliga/24_25/team",
        "seriea/24_25/team"
    ]
    for team in team_map:
        today = date.today()
        df_team = spark.read.option("multiline","true").json(f"s3a://raw/{team}/year={today.year}/month={today.month}/day={today.day}")
        transform_df = handle_null(spark, df_team)
        print("Starting transforming to Trusted Zone task")
        transform_df = transform_df.withColumn("year", lit(today.year)) \
        .withColumn("month", lit(today.month)) \
        .withColumn("day", lit(today.day))
        transform_df.printSchema()
        transform_df.write.mode("append").partitionBy("year", "month", "year").json(f"s3a://trusted/{team}")
        print(f"Complete loading to Trusted/{team} !")

def transformPlayer():
    attribute_map = ["attacking", "defending", "overall"]
    player_map = [
        "premierleague/24_25/player",
        "laliga/24_25/player",
        "ligue1/24_25/player",
        "bundesliga/24_25/player",
        "seriea/24_25/player"
    ]
    for player in player_map:
        for attribute in attribute_map:
            today = date.today()
            df_team = spark.read.option("multiline","true").json(f"s3a://raw/{player}/{attribute}/year={today.year}/month={today.month}/day={today.day}")
            transform_df = handle_null(spark, df_team)
            print("Starting transforming to Trusted Zone task")
            transform_df = transform_df.withColumn("year", lit(today.year)) \
            .withColumn("month", lit(today.month)) \
            .withColumn("day", lit(today.day))
            transform_df.printSchema()
            transform_df.write.mode("append").partitionBy("year", "month", "year").json(f"s3a://trusted/{player}/{attribute}")
            print(f"Complete loading to Trusted/{player}/{attribute} !")

transformPayroll()
transformPlayer()

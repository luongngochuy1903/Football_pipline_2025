from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, substring, trim, regexp_replace
from modules.module_null import handle_null
from datetime import date

spark = SparkSession.builder \
    .appName("fbref to Trusted") \
    .getOrCreate()

#transforming team info to Trusted
def transformTeam():
    team_map = [
        "premierleague/24_25/team",
        "laliga/24_25/team",
        "ligue1/24_25/team",
        "bundesliga/24_25/team",
        "seriea/24_25/team"
    ]
    for team in team_map:
        today = date.today()
        df_team = spark.read.json(f"s3a://raw/{team}/year={today.year}/month={today.month}/day={today.day}")
        transform_df = handle_null(spark, df_team)
        print("Starting transforming to Trusted Zone task")
        transform_df = transform_df.withColumn("year", lit(today.year)) \
        .withColumn("month", lit(today.month)) \
        .withColumn("day", lit(today.day))
        transform_df.printSchema()
        transform_df.write.mode("append").partitionBy("year", "month", "day").json(f"s3a://trusted/{team}")
        print(f"Complete loading to Trusted/{team} !")

#transforming player stats to Trusted
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
            df_team = spark.read.json(f"s3a://raw/{player}/{attribute}/year={today.year}/month={today.month}/day={today.day}")
            if attribute == "overall":
                df_team = df_team.withColumn("nationality", trim(substring(col("nationality"), 4, 6)))
                df_team = df_team.withColumn("age", trim(substring(col("age"), 1, 2)))
                df_team = df_team.withColumn("minutes", trim(regexp_replace(col("minutes"), "[,]", "")))
            df_team = df_team.filter(~col("player_name").isin("", "Player"))
            transform_df = handle_null(spark, df_team)
            print("Starting transforming to Trusted Zone task")
            transform_df = transform_df.withColumn("year", lit(today.year)) \
            .withColumn("month", lit(today.month)) \
            .withColumn("day", lit(today.day))
            transform_df.printSchema()
            transform_df.write.mode("append").partitionBy("year", "month", "day").json(f"s3a://trusted/{player}/{attribute}")
            print(f"Complete loading to Trusted/{player}/{attribute} !")

transformTeam()
transformPlayer()

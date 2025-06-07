from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_date, regexp_replace, col, trim
from modules.module_null import handle_null
from datetime import date

spark = SparkSession.builder \
    .appName("esspn to minio") \
    .getOrCreate()

def transformPayroll():
    payroll_map = [
        "premierleague/24_25/finance/payrolls",
        "laliga/24_25/finance/payrolls",
        "ligue1/24_25/finance/payrolls",
        "bundesliga/24_25/finance/payrolls",
        "seriea/24_25/finance/payrolls"
    ]
    for pay in payroll_map:
        today = date.today()
        df_pay = spark.read.json(f"s3a://raw/{pay}/year={today.year}/month={today.month}/day={today.day}")
        for col_name in df_pay.columns:
            if col_name != "team":
                df_pay = df_pay.withColumn(col_name, trim(regexp_replace(col(col_name), "[€,]", "")))

        transform_df = handle_null(spark, df_pay)
        print("Starting transforming to Trusted Zone task")
        transform_df = transform_df.withColumn("year", lit(today.year)) \
        .withColumn("month", lit(today.month)) \
        .withColumn("day", lit(today.day))
        transform_df.printSchema()
        transform_df.write.mode("append").partitionBy("year", "month", "day").json(f"s3a://trusted/{pay}")
        print(f"Complete loading to Trusted/{pay} !")

def transformTransfer():
    transfer_map = [
        "premierleague/24_25/finance/transfer",
        "laliga/24_25/finance/transfer",
        "ligue1/24_25/finance/transfer",
        "bundesliga/24_25/finance/transfer",
        "seriea/24_25/finance/transfer"
    ]
    for pay in transfer_map:
        today = date.today()
        df_pay = spark.read.json(f"s3a://raw/{pay}/year={today.year}/month={today.month}/day={today.day}")
        for col_name in df_pay.columns:
            if col_name != "team":
                df_pay = df_pay.withColumn(col_name, regexp_replace(col(col_name), "[€,]", ""))
        transform_df = handle_null(spark, df_pay)
        print("Starting transforming to Trusted Zone task")
        transform_df = transform_df.withColumn("year", lit(today.year)) \
        .withColumn("month", lit(today.month)) \
        .withColumn("day", lit(today.day))
        transform_df.printSchema()
        transform_df.write.mode("append").partitionBy("year", "month", "day").json(f"s3a://trusted/{pay}")
        print(f"Complete loading to Trusted/{pay} !")


def transformSalary():
    salary_map = [
        "premierleague/24_25/finance/salary",
        "laliga/24_25/finance/salary",
        "ligue1/24_25/finance/salary",
        "bundesliga/24_25/finance/salary",
        "seriea/24_25/finance/salary"
    ]
    for pay in salary_map:
        today = date.today()
        df_pay = spark.read.json(f"s3a://raw/{pay}/year={today.year}/month={today.month}/day={today.day}")
        print("Starting transforming to Trusted Zone task")
        df_pay = df_pay.withColumn("signed", to_date(col("signed"), "MMM d, yyyy")) \
            .withColumn("expiration", to_date(col("expiration"), "MMM d, yyyy")) \
            .withColumn("gross/w", regexp_replace(col("gross/w"), "[€,]", "").cast("double")) \
            .withColumn("gross/y", regexp_replace(col("gross/y"), "[€,]", "").cast("double")) \
            .withColumn("gross_remaining", regexp_replace(col("gross_remaining"), "[€,]", "").cast("double")) \
            .withColumn("release_clause", regexp_replace(col("release_clause"), "[€,]", "").cast("double")) 
        transform_df = handle_null(spark, df_pay)
        transform_df = transform_df.withColumn("year", lit(today.year)) \
        .withColumn("month", lit(today.month)) \
        .withColumn("day", lit(today.day))
        transform_df.printSchema()
        transform_df.write.mode("append").partitionBy("year", "month", "day").json(f"s3a://trusted/{pay}")
        print(f"Complete loading to Trusted/{pay} !")

transformPayroll()
transformTransfer()
transformSalary()
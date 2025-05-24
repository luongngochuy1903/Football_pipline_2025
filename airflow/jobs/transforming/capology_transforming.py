from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
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
        df_pay = spark.read.option("multiline","true").json(f"s3a://raw/{pay}/year={today.year}/month={today.month}/day={today.day}")
        transform_df = handle_null(spark, df_pay)
        print("Starting transforming to Trusted Zone task")
        transform_df = transform_df.withColumn("year", lit(today.year)) \
        .withColumn("month", lit(today.month)) \
        .withColumn("day", lit(today.day))
        transform_df.printSchema()
        transform_df.write.mode("append").partitionBy("year", "month", "year").json(f"s3a://trusted/{pay}")
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
        df_pay = spark.read.option("multiline","true").json(f"s3a://raw/{pay}/year={today.year}/month={today.month}/day={today.day}")
        transform_df = handle_null(spark, df_pay)
        print("Starting transforming to Trusted Zone task")
        transform_df = transform_df.withColumn("year", lit(today.year)) \
        .withColumn("month", lit(today.month)) \
        .withColumn("day", lit(today.day))
        transform_df.printSchema()
        transform_df.write.mode("append").partitionBy("year", "month", "year").json(f"s3a://trusted/{pay}")
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
        df_pay = spark.read.option("multiline","true").json(f"s3a://raw/{pay}/year={today.year}/month={today.month}/day={today.day}")
        transform_df = handle_null(spark, df_pay)
        print("Starting transforming to Trusted Zone task")
        transform_df = transform_df.withColumn("year", lit(today.year)) \
        .withColumn("month", lit(today.month)) \
        .withColumn("day", lit(today.day))
        transform_df.printSchema()
        transform_df.write.mode("append").partitionBy("year", "month", "year").json(f"s3a://trusted/{pay}")
        print(f"Complete loading to Trusted/{pay} !")

transformPayroll()
transformTransfer()
transformSalary()
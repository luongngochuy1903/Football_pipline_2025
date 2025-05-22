from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from datetime import date, datetime
import boto3
from pyspark.sql.functions import col, to_json, struct, md5
import logging

def checking_duplicated(spark, target, raw_df, address, list_value):
    if not get_latest_partition_date(target, address):
        return False
    year, month, day = get_latest_partition_date(target, address)
    trusted_df = spark.read.parquet(f"s3a://{target}/{address}/year={year}/month={month}/day={day}")
    def hash_column(df, columns):
        return df.select(md5(to_json(struct(*[col(f"`{c}`") for c in columns]))).alias("hash")) \
             .rdd.map(lambda row: row["hash"]).collect()

    raw_list = hash_column(raw_df, list_value)
    trusted_list = hash_column(trusted_df, list_value)

    print("Raw Hashes là:", raw_list)
    print("Trusted Hashes là:", trusted_list)
    for item in raw_list:
        if item not in trusted_list:
            return False
    return True
    

def load_to_trusted(df, path):
    df.printSchema()
    df.show(3)
    today = date.today()
    df = df.withColumn("year", lit(today.year)) \
    .withColumn("month", lit(today.month)) \
    .withColumn("day", lit(today.day))
    df.write.mode("append").partitionBy("year","month","day").parquet(path)
    print("Load to TRUSTED ZONE/news minio completed !")

def get_latest_partition_date(bucket, prefix):
        s3_client = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minio_access_key",
            aws_secret_access_key="minio_secret_key"
        )
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' not in response:
                print(f"There is no data available in Trusted/{prefix}, ready to load")
                return None
        contents = response.get("Contents", [])
        partitions = set()

        for obj in contents:
            key = obj["Key"]
            parts = key.split("/")
            try:
                year = next(int(p.split("=")[1]) for p in parts if p.startswith("year="))
                month = next(int(p.split("=")[1]) for p in parts if p.startswith("month="))
                day = next(int(p.split("=")[1]) for p in parts if p.startswith("day="))
                partitions.add(datetime(year, month, day))
                print(f"Ngày tháng năm partition gần nhất là: {year}/{month}/{day}")
            except:
                continue 

        if not partitions:
            return None
        latest = max(partitions)
        return latest.year, latest.month, latest.day

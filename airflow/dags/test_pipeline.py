import boto3
import pandas as pd
from airflow import DAG
from io import BytesIO
from airflow.decorators import dag, task
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import json

data = [
    {
        "team_name": "Liverpool",
        "rank": "1",
        "point": "76",
        "manager": "Arne Slot"
    },
    {
        "team_name": "Arsenal",
        "rank": "2",
        "point": "63",
        "manager": "Arteta"
    }
]

default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 5, 8),
        'retries': 1,
    }

MINIO_ENDPOINT = "http://minio:9000"
AWS_ACCESS_KEY_ID = "minio_access_key"
AWS_SECRET_ACCESS_KEY = "minio_secret_key"

@dag(
    dag_id="test_minio",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) 
def extract_pipeline():
    @task(task_id="connect_to_minio")
    def _connect_to_minio():
        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        response = s3_client.list_buckets()
        bucket_names = [bucket["Name"] for bucket in response["Buckets"]]
        print(f"✅ Kết nối MinIO thành công! Danh sách bucket: {bucket_names}")
        return "success"
    
    @task(task_id="write_to_minio")
    def _write_to_minio():
        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        df = pd.DataFrame(data)
        today = datetime.today()
        df["year"] = today.year
        df["month"] = today.month

        table = pa.Table.from_pandas(df)
        print(table.schema)

        buffer = BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        file_key = f"teams/year={today.year}/month={today.month}/teams_{today.strftime('%Y%m%d')}.parquet"

        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        s3.upload_fileobj(buffer, "test", file_key)
        print(f"✅ Uploaded {file_key} to bucket test")

    _connect_to_minio() >> _write_to_minio()
etl_pipeline_dag = extract_pipeline()
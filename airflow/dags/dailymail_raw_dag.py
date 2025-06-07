from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import datetime
import subprocess
import logging
import sys
import os
logger = logging.getLogger(__name__)

@dag(
    schedule_interval='@daily', 
    start_date=datetime(2025, 5, 12), 
    catchup=False
    )
def dailymail_to_raw_dag():
    @task
    def write_news_to_shared():
        print("Bắt đầu crawl dữ liệu news cho data_shared...")
        command = [
            "docker", "exec", "selenium-crawler",
            "python", "/app/scripts/crawlData_dailymail.py"
        ]
        result = subprocess.run(command, capture_output=True, text=True)

        print("===== STDOUT =====")
        print(result.stdout)
        print("===== STDERR =====")
        print(result.stderr)

        if result.returncode != 0:
            raise Exception("Crawling job failed")
        logging.info("Crawling completed!")
    
    @task
    def spark_submit_to_raw():
        command = [
    "docker", "exec",
    "-e", "PYTHONPATH=/opt/spark_jobs",  # thêm dòng này
    "football_pipeline_2025-spark-master-1",
    "spark-submit", "--master", "spark://spark-master:7077",
    "/opt/spark_jobs/source_to_minio/dailymail_to_minio_job.py"
]

        result = subprocess.run(command, capture_output=True, text=True)

        print("===== STDOUT =====")
        print(result.stdout)
        print("===== STDERR =====")
        print(result.stderr)
        if result.returncode != 0:
            raise Exception("Loading job failed")
        logging.info("Loading job completed!")

    @task
    def clear_from_shared():
        os.system("rm -rf /opt/shared/news/dailymail/*")
        print("Deleted shared data")
    
    @task
    def transform_load():
        command = [
    "docker", "exec",
    "-e", "PYTHONPATH=/opt/spark_jobs",
    "football_pipeline_2025-spark-master-1",
    "spark-submit", "--master", "spark://spark-master:7077",
    "/opt/spark_jobs/transforming/dailymail_transforming.py"
]
        result = subprocess.run(command, capture_output=True, text=True)

        print("===== STDOUT =====")
        print(result.stdout)
        print("===== STDERR =====")
        print(result.stderr)

        if result.returncode != 0:
            raise Exception("Spark job failed")
        logging.info("completed dailymail load to Trusted!")
    
    @task
    def load_to_refined():
        command = [
    "docker", "exec",
    "-e", "PYTHONPATH=/opt/spark_jobs",
    "football_pipeline_2025-spark-master-1",
    "spark-submit", "--master", "spark://spark-master:7077",
    "/opt/spark_jobs/load_to_refined/dailymail_to_refined.py"
]

        result = subprocess.run(command, capture_output=True, text=True)

        print("===== STDOUT =====")
        print(result.stdout)
        print("===== STDERR =====")
        print(result.stderr)

        if result.returncode != 0:
            raise Exception("Spark job failed")
        logging.info("completed dailymail load to refined!")

    trigger_target = TriggerDagRunOperator(
        task_id='trigger_target_dag',
        trigger_dag_id='dw_process',  
        wait_for_completion=False,   
        reset_dag_run=True,          
    )

    write_news_to_shared() >> spark_submit_to_raw() >> clear_from_shared() >> transform_load() >> load_to_refined() >> trigger_target

dailymail_to_raw_dag()
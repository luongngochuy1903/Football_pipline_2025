from airflow.decorators import dag, task
from airflow.utils.dates import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import boto3
import logging
import subprocess
import json
import os

@dag(
    schedule_interval='@daily', 
    start_date=datetime(2025, 5, 12), 
    catchup=False
    )
def espn_to_raw_dag():

    @task()
    def extract_espn_raw():
        command = [
    "docker", "exec",
    "-e", "PYTHONPATH=/opt/spark_jobs",  
    "football_pipeline_2025-spark-master-1",
    "spark-submit", "--master", "spark://spark-master:7077",
    "/opt/spark_jobs/source_to_minio/espn_to_minio_job.py"
]

        result = subprocess.run(command, capture_output=True, text=True)

        print("===== STDOUT =====")
        print(result.stdout)
        print("===== STDERR =====")
        print(result.stderr)

        if result.returncode != 0:
            raise Exception("Spark job failed")
    
    @task
    def clear_from_shared():
        league_folder = ["bundesliga", "laliga", "ligue 1", "premier_leagues", "seria"]
        attribute_folder = "team_info/team_season.json"
        for league in league_folder:
            os.system(f"rm -f /opt/shared/{league}/24_25/{attribute_folder}")
        print("Deleted shared data")
    
    @task
    def transform_load():
        command = [
    "docker", "exec",
    "-e", "PYTHONPATH=/opt/spark_jobs",
    "football_pipeline_2025-spark-master-1",
    "spark-submit", "--master", "spark://spark-master:7077",
    "/opt/spark_jobs/transforming/espn_transforming.py"
]

        result = subprocess.run(command, capture_output=True, text=True)

        print("===== STDOUT =====")
        print(result.stdout)
        print("===== STDERR =====")
        print(result.stderr)

        if result.returncode != 0:
            raise Exception("Spark job failed")
        logging.info("completed espn load to Trusted!")
    
    @task
    def load_to_refined():
        command = [
    "docker", "exec",
    "-e", "PYTHONPATH=/opt/spark_jobs",
    "football_pipeline_2025-spark-master-1",
    "spark-submit", "--master", "spark://spark-master:7077",
    "/opt/spark_jobs/load_to_refined/espn_to_refined.py"
]

        result = subprocess.run(command, capture_output=True, text=True)

        print("===== STDOUT =====")
        print(result.stdout)
        print("===== STDERR =====")
        print(result.stderr)

        if result.returncode != 0:
            raise Exception("Spark job failed")
        print("completed espn load to refined!")

    trigger_target = TriggerDagRunOperator(
        task_id='trigger_target_dag',
        trigger_dag_id='dw_process',  
        wait_for_completion=False,   
        reset_dag_run=True,          
    )

    extract_espn_raw() >> clear_from_shared() >> transform_load() >> load_to_refined() >> trigger_target
espn_to_raw_dag()
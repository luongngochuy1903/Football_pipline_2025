from airflow.decorators import dag, task
from airflow.utils.dates import datetime
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import subprocess
import logging
import os
import sys
logger = logging.getLogger(__name__)

@dag(
    schedule_interval=timedelta(days=4), 
    start_date=datetime(2025, 5, 12), 
    catchup=False
    )
def fbref_to_raw_dag():
    @task
    def write_team_player_to_shared():
        print("Bắt đầu crawl dữ liệu cho team và player cho data_shared...")
        command = [
            "docker", "exec", "selenium-crawler",
            "python", "/app/scripts/crawlData_fbhref.py"
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
    "-e", "PYTHONPATH=/opt/spark_jobs",  
    "football_pipeline_2025-spark-master-1",
    "spark-submit", "--master", "spark://spark-master:7077",
    "/opt/spark_jobs/source_to_minio/fbhref_to_minio_job.py"
]

        result = subprocess.run(command, capture_output=True, text=True)

        print("===== STDOUT =====")
        print(result.stdout)
        print("===== STDERR =====")
        print(result.stderr)
        if result.returncode != 0:
            raise Exception("Loading job failed")
        logging.info("Crawling completed!")

    @task
    def clear_from_shared():
        league_folder = ["bundesliga", "laliga", "ligue 1", "premier_leagues", "seria"]
        attribute_folder = ["team_info/teams_overall.json", "player_info/overall/*", "player_info/attacking/*", "player_info/defending/*"]
        for league in league_folder:
            for attribute in attribute_folder:
                os.system(f"rm -f /opt/shared/{league}/24_25/{attribute}")
        print("Deleted shared data")
    
    @task
    def transform_load():
        command = [
    "docker", "exec",
    "-e", "PYTHONPATH=/opt/spark_jobs",
    "football_pipeline_2025-spark-master-1",
    "spark-submit", "--master", "spark://spark-master:7077",
    "/opt/spark_jobs/transforming/fbhref_transforming.py"
]

        result = subprocess.run(command, capture_output=True, text=True)

        print("===== STDOUT =====")
        print(result.stdout)
        print("===== STDERR =====")
        print(result.stderr)

        if result.returncode != 0:
            raise Exception("Spark job failed")
        logging.info("completed fbhref load to Trusted!")
    
    @task
    def load_to_refined():
        command = [
    "docker", "exec",
    "-e", "PYTHONPATH=/opt/spark_jobs",
    "football_pipeline_2025-spark-master-1",
    "spark-submit", "--master", "spark://spark-master:7077",
    "/opt/spark_jobs/load_to_refined/player_to_refined.py"
]

        result = subprocess.run(command, capture_output=True, text=True)

        print("===== STDOUT =====")
        print(result.stdout)
        print("===== STDERR =====")
        print(result.stderr)

        if result.returncode != 0:
            raise Exception("Spark job failed")
        logging.info("completed fbhref load to refined!")
    
    trigger_target = TriggerDagRunOperator(
        task_id='trigger_target_dag',
        trigger_dag_id='dw_process',  
        wait_for_completion=False,   
        reset_dag_run=True,          
    )

    write_team_player_to_shared() >> spark_submit_to_raw() >> clear_from_shared() >> transform_load() >> load_to_refined() >> trigger_target

fbref_to_raw_dag()
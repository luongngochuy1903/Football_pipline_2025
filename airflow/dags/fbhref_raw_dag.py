from airflow.decorators import dag, task
from airflow.utils.dates import datetime
from datetime import datetime, timedelta
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
    "-e", "PYTHONPATH=/opt/spark_jobs",  # thêm dòng này
    "football_pipeline_2025-spark-master-1",
    "spark-submit", "--master", "spark://spark-master:7077",
    "/opt/spark_jobs/source_to_minio/fbhref_to_minio_job.py"
]

        result = subprocess.run(command, capture_output=True, text=True)

        print("===== STDOUT =====")
        print(result.stdout)
        print("===== STDERR =====")
        print(result.stderr)

    @task
    def clear_from_shared():
        league_folder = ["bundesliga", "laliga", "ligue 1", "premier_leagues", "seria"]
        attribute_folder = ["team_info/teams_overall.json", "player_info/overall/*", "player_info/attacking/*", "player_info/defending/*"]
        for league in league_folder:
            for attribute in attribute_folder:
                os.system(f"rm -f /opt/shared/{league}/24_25/{attribute}")
        print("Deleted shared data")

    write_team_player_to_shared() >> spark_submit_to_raw() >> clear_from_shared()

fbref_to_raw_dag()
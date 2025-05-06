from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator


from datetime import datetime, date
import requests
import json
import boto3
import logging
import os
from dotenv import load_dotenv



#Load from .env file
load_dotenv(dotenv_path='D:/HUST/Intern/FSOFT/course/data-pipeline-project/.env')

#Game schedule info

headers = {"accept": "application/json",
           "x-api-key": "9IoqO5ggRImWPUfiDapBgb7XkX3pqJs2S2B6Xqs9"}
    
filename = 'respone'
url = "https://api.sportradar.com/nba/trial/v8/en/games/2024/REG/schedule.json"

response = requests.get(url, headers=headers)


def extract_date_from_api(datetime_str):
    if datetime_str.endswith('Z'):
        dt = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%SZ")
    else:
        dt = datetime.fromisoformat(datetime_str)
    
    return dt.date()


def is_same_date(api_datetime_str: str, target: date) -> bool:
    try:
        api_datetime = datetime.strptime(api_datetime_str, "%Y-%m-%dT%H:%M:%SZ")
        return api_datetime.date() == target
    except ValueError as e:
        print(f"Invalid datetime format: {e}")
        return False

def check_today_games(**context):
    schedule = response.json()
    today_games = []

    for game in schedule['games']:
        if is_same_date(api_datetime_str=game['scheduled'], target=date(2025, 2, 15)):
        # if (game['status'] == "closed"):
            game_id = game['id']
            if not os.path.exists(f"/data/transformed/{game_id}.done"):
                today_games.append((game_id, game['scheduled']))
    
    #Push this list to XCom
    context['ti'].xcom_push(key='today_games', value=today_games)  


def upload_game_summary(**context):
    s3 = boto3.client(
        's3', 
        endpoint_url='http://minio:9000/',
        aws_access_key_id=os.getenv('MINIO_ROOT_USER'),
        aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD'),
        region_name='us-east-1'
    )

    today_games = context['ti'].xcom_pull(key='today_games', task_ids='update_game_today')

    #Fetching data
    for game_id, game_schedule in today_games:
        res = requests.get(url=f"https://api.sportradar.com/nba/trial/v8/en/games/{game_id}/summary.json", headers=headers).json()

        local_path = f"/data/{game_id}.json" 
        with open(local_path, 'w') as f:
            json.dump(res, f)

        s3.upload_file(local_path, os.getenv('MINIO_DEFAULT_BUCKETS'), f"games/{extract_date_from_api(game_schedule)}/raw_games/{game_id}.json")


with DAG (
    dag_id="nba_processing",
    start_date=datetime(2023, 1, 1),
        schedule="@daily",
        catchup=False
    ) as dag:
    
    # update_game_today = PythonOperator(
    #     task_id = 'update_game_today',
    #     python_callable=check_today_games,
    #     provide_context=True
    # )

    # extract_game = PythonOperator(
    #     task_id = 'extract_game',
    #     python_callable=upload_game_summary,
    #     provide_context=True
    # )

    data_quality_check = BashOperator (
        task_id = 'data_quality_check',
        bash_command="spark-submit --master local[*] --jars /opt/airflow/includes/aws-java-sdk-bundle-1.11.1026.jar,/opt/airflow/includes/hadoop-aws-3.3.4.jar /opt/airflow/includes/data_quality_check.py"
    )

    data_transform_load = BashOperator (
        task_id = 'data_transform_load',
        bash_command="spark-submit --master local[*] --jars /opt/airflow/includes/aws-java-sdk-bundle-1.11.1026.jar,/opt/airflow/includes/hadoop-aws-3.3.4.jar /opt/airflow/includes/data_transform_load.py"
    )
    # update_game_today >> extract_game >>  
    data_quality_check >> data_transform_load





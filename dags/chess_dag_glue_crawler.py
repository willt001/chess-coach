from airflow.decorators import dag, task 
import pendulum
from dateutil.relativedelta import relativedelta
from airflow.hooks.base import BaseHook
import boto3
from config import REGION_NAME, GLUE_CRAWLER_CONFIG
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
import json


glue_crawler_config = json.loads(GLUE_CRAWLER_CONFIG)

@dag(
    start_date=pendulum.datetime(2024, 9, 1, 1),
    schedule_interval=relativedelta(months=1),
    catchup=False,
    max_active_runs=1
    )
def chess_glue_crawler():

    @task
    def check_glue_crawler_status(crawler_name):
        connection = BaseHook.get_connection('aws_access')
        glue_client = boto3.client(
                                'glue',
                                aws_access_key_id=connection.login,
                                aws_secret_access_key=connection.password,
                                region_name=REGION_NAME
                                )
        response = glue_client.get_crawler(Name=crawler_name)
        crawler_state = response['Crawler']['State']
        if crawler_state in ['RUNNING', 'STOPPING']:
            raise AirflowSkipException(f"Crawler {crawler_name} is already running.")
    
    crawl_s3 = GlueCrawlerOperator(
        task_id="crawl_s3",
        config=glue_crawler_config,
        aws_conn_id='aws_access',
        wait_for_completion=False
    )

    check_glue_crawler_status(glue_crawler_config['Name']) >> crawl_s3

chess_glue_crawler()
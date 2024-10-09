import pendulum
import boto3
from airflow.decorators import dag, task, task_group
from extract_games import get_monthly_games
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
import logging
from config import BUCKET_NAME, LAMBDA_FUNCTION_NAME, GLUE_CRAWLER_CONFIG, REGION_NAME
import json
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
import time

glue_crawler_config = json.loads(GLUE_CRAWLER_CONFIG)

@dag(
    start_date=pendulum.datetime(2019, 1, 1, tz='UTC'),
    schedule_interval='@monthly',
    catchup=False
    )
def chess_etl_lambda():

    @task
    def extract_games(ti):
        return get_monthly_games(ti.execution_date)
    
    @task_group
    def process_partition(zipped_kwargs):

        @task(multiple_outputs=True)
        def process_xcom(zipped_kwargs):
            # Preparing payload for lambda function invokation.
            zipped_kwargs['payload'] = f'{{ "bucket_name": "{BUCKET_NAME}", "object_key": "{zipped_kwargs['dest_key']}" }}'
            logging.info(zipped_kwargs['payload'])
            return zipped_kwargs
        
        @task.bash(trigger_rule='all_done')
        def delete_local_file(filename):
            return f'rm -rf /opt/airflow/{filename}'    
        
        kwargs = process_xcom(zipped_kwargs)
        local_to_s3 = LocalFilesystemToS3Operator(
                                                task_id='local_to_s3',
                                                filename=kwargs['filename'],
                                                dest_bucket=BUCKET_NAME,
                                                dest_key=kwargs['dest_key'],
                                                aws_conn_id='aws_access',
                                                replace=True
        )
        calculate_blunders = LambdaInvokeFunctionOperator(
                                                    task_id='calculate_blunders',
                                                    function_name=LAMBDA_FUNCTION_NAME,
                                                    payload=kwargs['payload'],
                                                    invocation_type="Event",
                                                    aws_conn_id='aws_access'
        )
        
        local_to_s3 >> [calculate_blunders, delete_local_file(kwargs['filename'])]
    
    @task(pool='glue_crawler_pool')
    def wait_30_seconds():
        time.sleep(30)

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

    data = extract_games()
    process_partition.expand(zipped_kwargs=data) >> wait_30_seconds() >> check_glue_crawler_status(glue_crawler_config['Name']) >> crawl_s3

chess_etl_lambda()
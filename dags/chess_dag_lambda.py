import pendulum
from airflow.decorators import dag, task, task_group
from extract_games import get_monthly_games
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
import logging
from config import BUCKET_NAME, LAMBDA_FUNCTION_NAME

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
        
        kwargs = process_xcom(zipped_kwargs)
        
        local_to_s3 = LocalFilesystemToS3Operator(
                                                task_id='local_to_s3',
                                                filename=kwargs['filename'],
                                                dest_bucket=BUCKET_NAME,
                                                dest_key=kwargs['dest_key'],
                                                aws_conn_id='aws_access',
                                                replace=True
        )

        @task.bash(trigger_rule='all_done')
        def delete_local_file(filename):
            return f'rm -rf /opt/airflow/{filename}'                                        
        
        calculate_blunders = LambdaInvokeFunctionOperator(
                                                    task_id='calculate_blunders',
                                                    function_name=LAMBDA_FUNCTION_NAME,
                                                    payload=kwargs['payload'],
                                                    invocation_type="Event",
                                                    aws_conn_id='aws_access'
        )
        
        local_to_s3 >> [calculate_blunders, delete_local_file(kwargs['filename'])]

    data = extract_games()
    process_partition.expand(zipped_kwargs=data)

chess_etl_lambda()
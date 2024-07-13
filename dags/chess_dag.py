from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from chess_etl_code import get_monthly_games
from blunder_etl_code import get_monthly_blunders
from redshift_etl_code import load_game_data, load_move_data

with DAG(
    dag_id='chess_etl', 
    start_date=pendulum.datetime(2019, 1, 1, tz='UTC'),
    schedule_interval='@monthly',
    catchup=True,
    max_active_runs=2
    ) as dag:

    
    def games_etl_task(**context):
        execution_date = context.get('execution_date')
        return get_monthly_games(execution_date)

    def blunders_etl_task(**context):
        execution_date = context.get('execution_date')
        return get_monthly_blunders(execution_date)

    def redshift_game_task(**context):
        execution_date = context.get('execution_date')
        return load_game_data(execution_date)
    
    def redshift_move_task(**context):
        execution_date = context.get('execution_date')
        return load_move_data(execution_date)
    
    games_etl = PythonOperator(task_id='games_etl', python_callable=games_etl_task)

    blunders_etl = PythonOperator(task_id='blunders_etl', python_callable=blunders_etl_task)

    redshift_game_load = PythonOperator(task_id='redshift_game_load', python_callable=redshift_game_task)

    redshift_move_load = PythonOperator(task_id='redshift_move_load', python_callable=redshift_move_task)

    games_etl >> blunders_etl >> redshift_game_load >> redshift_move_load
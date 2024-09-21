from airflow.decorators import dag, task
import pendulum
from chess_etl_code import get_monthly_games
from blunder_etl_code import get_monthly_blunders
from redshift_etl_code import load_game_data, load_move_data

@dag(
    start_date=pendulum.datetime(2019, 1, 1, tz='UTC'),
    schedule_interval='@monthly',
    catchup=True,
    max_active_runs=2
    )
def chess_etl():
    
    @task
    def download_games_upload_s3(ti=None):
        return get_monthly_games(ti.execution_date)

    @task
    def calculate_blunders_upload_s3(ti):
        return get_monthly_blunders(ti.execution_date)

    @task
    def load_games_to_redshift(ti):
        return load_game_data(ti.execution_date)
    
    @task
    def load_blunders_to_redshift(ti):
        return load_move_data(ti.execution_date)

    
    games = download_games_upload_s3()

    blunders = calculate_blunders_upload_s3()

    games >> [blunders, load_games_to_redshift()]

    blunders >> load_blunders_to_redshift()

chess_etl()
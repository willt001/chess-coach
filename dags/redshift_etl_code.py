import s3fs
import redshift_connector
from airflow.hooks.base import BaseHook


def load_game_data(execution_date):
    '''Load monthly game data from S3 to Redshift'''
    execution_date = str(execution_date)
    year_month = execution_date[:7].replace('-', '_')

    #S3 Credentials are securely stored in Airflow connections list
    connection = BaseHook.get_connection('chess_project_s3')
    my_key = connection.login
    my_secret = connection.password
    s3 = s3fs.S3FileSystem(anon=False, key=my_key, secret=my_secret)
    #Return None if no games are found for the given month
    try:
        with s3.open(f's3://chess-coach-de-project/{year_month}_games.csv', 'r') as f:
            pass
    except:
        print(f'No games for period {year_month}')
        return
    
    #Redshift Credentials are securely stored in Airflow connections list
    connection = BaseHook.get_connection('chess_project_redshift')
    my_user = connection.login
    my_password = connection.password
    my_host = connection.host
    my_port = connection.port
    my_database = connection.schema
    conn = redshift_connector.connect(
    host=my_host,
    port=my_port,
    database=my_database,
    user=my_user,
    password=my_password,
    )

    delete_games_staging_query = f"""
    DELETE FROM "dev"."chess_project_staging"."game"
    WHERE dateid >= date '{execution_date}' and dateid < dateadd(month, 1, '{execution_date}')
    """
    copy_games_staging_query = f"""
    COPY "dev"."chess_project_staging"."game"
    FROM 's3://chess-coach-de-project/{year_month}_games.csv' 
    IAM_ROLE 'arn:aws:iam::905418173427:role/service-role/AmazonRedshift-CommandsAccessRole-20240610T231914' 
    FORMAT AS CSV 
    DELIMITER ',' 
    QUOTE '"' 
    IGNOREHEADER 1 
    REGION AS 'eu-north-1';
    """
    check_games_query = f"""
    with staging as (
    select COUNT(*) as rows FROM "dev"."chess_project_staging"."game"
    WHERE dateid >= date '{execution_date}' and dateid < dateadd(month, 1, '{execution_date}')
    ), 
    prod as (
    select COUNT(*) as rows FROM "dev"."chess_project"."game"
    WHERE dateid >= date '{execution_date}' and dateid < dateadd(month, 1, '{execution_date}')
    )
    select (staging.rows>=prod.rows)::int as check from staging, prod
    """
    delete_games_query = f"""
    DELETE FROM "dev"."chess_project"."game"
    WHERE dateid >= date '{execution_date}' and dateid < dateadd(month, 1, '{execution_date}')
    ;
    """
    insert_games_query = f"""
    INSERT INTO "dev"."chess_project"."game" (
        SELECT
        url,
        dateid,
        time_control,
        case rated_flag when 'True' then 1 else 0 end rated_flag,
        fen,
        time_class,
        termination,
        termination_reason,
        result,
        eco,
        opening,
        REPLACE(opening_short, '-', ' ') opening_short,
        cast(start_time as TIME) start_time,
        cast(end_time as TIME) end_time,
        hero_colour,
        hero_username,
        hero_result,
        villain_colour,
        villain_username,
        villain_result
    FROM
        "dev"."chess_project_staging"."game"
    WHERE 
        dateid >= date '{execution_date}' and dateid < dateadd(month, 1, '{execution_date}')
    )
    """
    cursor = conn.cursor()
    cursor.execute(delete_games_staging_query)
    cursor.execute(copy_games_staging_query)
    #If staging row count is less than prod row count, do not load staging data to prod
    cursor.execute(check_games_query)
    check = cursor.fetchall()[0][0]
    if check==0:
        conn.close()
        return None
    
    cursor.execute(delete_games_query)
    cursor.execute(insert_games_query)
    conn.commit()
    conn.close()

def load_move_data(execution_date):
    '''Load monthly game data from S3 to Redshift'''
    execution_date = str(execution_date)
    year_month = execution_date[:7].replace('-', '_')
    #S3 Credentials are securely stored in Airflow connections list
    connection = BaseHook.get_connection('chess_project_s3')
    my_key = connection.login
    my_secret = connection.password
    s3 = s3fs.S3FileSystem(anon=False, key=my_key, secret=my_secret)
    #Return None if no moves data is found in S3 bucket for given month
    try:
        with s3.open(f's3://chess-coach-de-project/{year_month}_moves.csv', 'r') as f:
            pass
    except:
        print(f'No games for period {year_month}')
        return
    
    #Redshift Credentials are securely stored in Airflow connections list
    connection = BaseHook.get_connection('chess_project_redshift')
    my_user = connection.login
    my_password = connection.password
    my_host = connection.host
    my_port = connection.port
    my_database = connection.schema
    conn = redshift_connector.connect(
    host=my_host,
    port=my_port,
    database=my_database,
    user=my_user,
    password=my_password,
    )

    delete_moves_staging_query = f"""
    DELETE FROM "dev"."chess_project_staging"."move"
    WHERE dateid >= date '{execution_date}' and dateid < dateadd(month, 1, '{execution_date}')
    ;
    """
    copy_moves_staging_query = f"""
    COPY "dev"."chess_project_staging"."move"
    FROM 's3://chess-coach-de-project/{year_month}_moves.csv' 
    IAM_ROLE 'arn:aws:iam::905418173427:role/service-role/AmazonRedshift-CommandsAccessRole-20240610T231914' 
    FORMAT AS CSV 
    DELIMITER ',' 
    QUOTE '"' 
    IGNOREHEADER 1 
    REGION AS 'eu-north-1'
    """
    check_moves_query = f"""
    with staging as (
    select COUNT(*) as rows FROM "dev"."chess_project_staging"."move"
    WHERE dateid >= date '{execution_date}' and dateid < dateadd(month, 1, '{execution_date}')
    ), 
    prod as (
    select COUNT(*) as rows FROM "dev"."chess_project"."move"
    WHERE dateid >= date '{execution_date}' and dateid < dateadd(month, 1, '{execution_date}')
    )
    select (staging.rows>=prod.rows)::int as check from staging, prod
    """
    delete_moves_query = f"""
    DELETE FROM "dev"."chess_project"."move"
    WHERE dateid >= date '{execution_date}' and dateid < dateadd(month, 1, '{execution_date}')
    ;
    """
    insert_moves_query = f"""
    INSERT INTO "dev"."chess_project"."move" (
        SELECT 
            game_url,
            dateid,
            san,
            uci,
            side,
            colour,
            move,
            game_state_before,
            evaluation_type_before,
            evaluation_before,
            material_evaluation_before,
            game_state_after,
            evaluation_type_after,
            evaluation_after,
            material_evaluation_after  
        FROM 
            "dev"."chess_project_staging"."move"
        WHERE 
            dateid >= date '{execution_date}' and dateid < dateadd(month, 1, '{execution_date}')
    )
    """
    cursor = conn.cursor()
    cursor.execute(delete_moves_staging_query)
    cursor.execute(copy_moves_staging_query)
    #If staging row count is less than prod row count, do not load staging data to prod
    cursor.execute(check_moves_query)
    check = cursor.fetchall()[0][0]
    if check==0:
        conn.close()
        return None
    cursor.execute(delete_moves_query)
    cursor.execute(insert_moves_query)
    conn.commit()
    conn.close()
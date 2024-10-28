import requests
import pandas as pd
from datetime import datetime
import regex as re
from airflow import AirflowException
import pyarrow as pa
import pyarrow.parquet as pq


def schema_check(game_json: dict) -> None:
    expected_keys = set([
        'url',
        'pgn',
        'time_control',
        'end_time',
        'rated',
        'tcn',
        'uuid',
        'initial_setup',
        'fen',
        'time_class',
        'rules',
        'white',
        'black',
        'eco'
    ])
    actual_keys = set(game_json.keys())
    missing_keys = expected_keys - actual_keys
    assert missing_keys == set(), f'Missing Keys: {missing_keys}'

def types_check(game_json: dict) -> None:
    expected_types = {
        "url": str,
        "pgn": str,
        "time_control": str,
        "end_time": int,
        "rated": bool,
        "tcn": str,
        "uuid": str,
        "initial_setup": str,
        "fen": str,
        "time_class": str,
        "rules": str,
        "white": dict,
        "black": dict,
        "eco": str
    }
    for key_name, data in game_json.items():
        if key_name in expected_types:
            expected_type = expected_types[key_name]
            assert isinstance(data, expected_type), f'key {key_name} should be {expected_type}, not {type(data)}'

def nullability_check(game_json: dict) -> None:
    non_null_keys = [
        'url'
    ]
    for key_name in non_null_keys:
        assert game_json[key_name] is not None, f'Key {key_name} cannot be null.'

def get_monthly_games(date: datetime, username: str = 'willlt001') -> None:
    '''Takes in a date and Chess.com username and outputs a csv with all Chess.com games in that month'''
    # Chess.com API returns one calendar month of games for one user.
    year_month = str(date)[:7].replace('-', '/')
    url = f'https://api.chess.com/pub/player/{username}/games/{year_month}'
    # Request will be blocked if default headers are used.
    headers = {"User-Agent": username}
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        raise AirflowException(f'Bad Status Code: {r.status_code}')
    games = r.json()
    games = games.get('games')
    # If no games are found, return blank list so downstream tasks are skipped.
    if not games:
        return []
    game_list = []
    for game in games:
        schema_check(game)
        types_check(game)
        nullability_check(game)
        # PGN is a standard plain text format for recording chess games
        pgn = game.get('pgn').split('\n')
        if len(pgn) < 23:
            print(f'Unable to parse: {pgn}')
            continue
        refined_game = {'url': game.get('url'),
                        'game_date': pgn[2][7:17].replace('.', '-'),
                        'moves': pgn[22],
                        'time_control': game.get('time_control'),
                        'rated_flag': game.get('rated'),
                        'fen': game.get('fen'),
                        'time_class': game.get('time_class'),
                        'termination': pgn[16].split('"')[1],
                        'result': pgn[6].split('"')[1],
                        'ECO': pgn[9].split('"')[1],
                        'opening': pgn[10].split('/')[-1][:-2],
                        'start_time': pgn[17].split('"')[1],
                        'end_time': pgn[19].split('"')[1]
                        }
        
        if pgn[4].split('"')[1] == username:
            refined_game['hero_colour'] = 'white'
            refined_game['villain_colour'] = 'black'
            refined_game['hero_username'] = username
            refined_game['villain_username'] = pgn[5].split('"')[1]
        else:
            refined_game['villain_colour'] = 'white'
            refined_game['hero_colour'] = 'black'
            refined_game['hero_username'] = username
            refined_game['villain_username'] = pgn[4].split('"')[1]
        
        if refined_game['result'] == '1/2-1/2':
            refined_game['hero_result'] = 'draw'
            refined_game['villain_result'] = 'draw'
        elif refined_game['result'] == '1-0' and refined_game['hero_colour'] == 'white':
            refined_game['hero_result'] = 'win'
            refined_game['villain_result'] = 'loss'
        elif refined_game['result'] == '1-0' and refined_game['hero_colour'] == 'black':
            refined_game['hero_result'] = 'loss'
            refined_game['villain_result'] = 'win'
        elif refined_game['result'] == '0-1' and refined_game['hero_colour'] == 'white':
            refined_game['hero_result'] = 'loss'
            refined_game['villain_result'] = 'win'
        elif refined_game['result'] == '0-1' and refined_game['hero_colour'] == 'black':
            refined_game['hero_result'] = 'win'
            refined_game['villain_result'] = 'loss'

        # Extract keywords for game termination reason.
        if 'by' in refined_game['termination']:
            refined_game['termination_reason'] = refined_game['termination'].split('by ')[-1]
        else:
            refined_game['termination_reason'] = refined_game['termination'].split(' ')[-1]

        # Extract the first two words from the opening string
        pattern = re.compile(r'^([A-Z]([a-z]|-)+)\-([A-Z][a-z]+)+')
        match = re.match(pattern, refined_game['opening'])
        refined_game['opening_short'] = match.group(0) if match else refined_game['opening']

        game_list.append(refined_game)

    df = pd.DataFrame(game_list)
    df.game_date = df.game_date.astype('datetime64[us]')

    # Split the data into partitions of <=50 games each. It takes AWS Lambda approx 10 minutes to process 50 games.
    df['partition_num'] = df.index // 50
    partitions = df.groupby('partition_num')
    year_month = year_month.replace('/', '')
    xcom = []
    # Save each partition to one parquet file, using Apache Hive style partitioning for file naming.
    for idx, df in partitions:
        table = pa.Table.from_pandas(df.drop('partition_num', axis=1), preserve_index=False)
        pq.write_table(table, f'games_m={year_month}_p={idx}.parquet')
        xcom.append(
            {
                'filename': f'games_m={year_month}_p={idx}.parquet',
                'dest_key': f'games/month={year_month}/games_{idx}.parquet'
            }  
        )
    # Push XCom to use for dynamic task mapping in downstream tasks.
    return xcom

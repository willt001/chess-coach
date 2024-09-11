import requests
import pandas as pd
from datetime import date
import s3fs
from airflow.hooks.base import BaseHook
import logging
import regex as re
from airflow import AirflowException

def schema_check(game_json):
    keys_required = ['pgn', 'time_control', 'rated', 'fen', 'time_class', 'url']
    for key in keys_required:
        if key not in game_json:
            return False
    return True

def get_monthly_games(date, username='willlt001'):
    '''Takes in a date and Chess.com username and outputs a csv with all Chess.com games in that month'''
    #Send REST API call and extract json response
    year_month = str(date)[:7].replace('-', '/')
    url = f'https://api.chess.com/pub/player/{username}/games/{year_month}'
    headers = {
        "User-Agent": username
    }
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        return
    games = r.json()
    games = games.get('games')
    #Return None if no games are found for the given year/month
    if not games:
        return None
    game_list = []
    for i, game in enumerate(games):
        #Check fields required are in json
        if not schema_check(game):
            raise AirflowException(f'Schema Check for game {i}')

        #PGN is a standard plain text format for recording chess games
        pgn = game.get('pgn').split('\n')
        if len(pgn)<23:
            print(f'Unable to parse: {pgn}')
            continue

        refined_game = {'url': game.get('url'),
                        'date': pgn[2][7:17].replace('.', '-'),
                        'datenum': pgn[2][7:17].replace('.', ''),
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
        
        if pgn[4].split('"')[1]==username:
            refined_game['hero_colour']='white'
            refined_game['villain_colour']='black'
            refined_game['hero_username']=username
            refined_game['villain_username']=pgn[5].split('"')[1]
        else:
            refined_game['villain_colour']='white'
            refined_game['hero_colour']='black'
            refined_game['hero_username']=username
            refined_game['villain_username']=pgn[4].split('"')[1]
        
        if refined_game['result']=='1/2-1/2':
            refined_game['hero_result']='draw'
            refined_game['villain_result']='draw'
        elif refined_game['result']=='1-0' and refined_game['hero_colour']=='white':
            refined_game['hero_result']='win'
            refined_game['villain_result']='loss'
        elif refined_game['result']=='1-0' and refined_game['hero_colour']=='black':
            refined_game['hero_result']='loss'
            refined_game['villain_result']='win'
        elif refined_game['result']=='0-1' and refined_game['hero_colour']=='white':
            refined_game['hero_result']='loss'
            refined_game['villain_result']='win'
        elif refined_game['result']=='0-1' and refined_game['hero_colour']=='black':
            refined_game['hero_result']='win'
            refined_game['villain_result']='loss'
        #Extract keywords for game termination reason
        if 'by' in refined_game['termination']:
            refined_game['termination_reason']=refined_game['termination'].split('by ')[-1]
        else:
            refined_game['termination_reason']=refined_game['termination'].split(' ')[-1]
        #Extract the first two words from the opening string
        pattern = re.compile(r'^([A-Z][a-z]+)\-([A-Z][a-z]+)+')
        refined_game['opening_short'] = re.match(pattern, refined_game['opening']).group(0)

        game_list.append(refined_game)

    df = pd.DataFrame(game_list)
    
    #S3 Credentials are securely stored in Airflow connections list
    connection = BaseHook.get_connection('chess_project_s3')
    my_key = connection.login
    my_secret = connection.password

    #Load df to S3 bucket as csv
    year_month = year_month.replace('/', '_')
    s3 = s3fs.S3FileSystem(anon=False, key=my_key, secret=my_secret)
    with s3.open(f's3://chess-coach-de-project/{year_month}_games.csv', 'w') as f:
        df.to_csv(f)

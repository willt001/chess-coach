from stockfish import Stockfish
import chess
import pandas as pd
from typing import List
import boto3
import io
import pyarrow as pa
import pyarrow.parquet as pq

def new_stockfish(depth: int=16, threads: int=1, hash: int=2048) -> Stockfish:
    '''
    Initialise a new stockfish process.
    '''
    parameters = {
        'Threads': threads,
        'Hash': hash
    }
    stockfish = Stockfish('/usr/local/bin/stockfish', depth=depth, parameters=parameters)
    return stockfish

def get_san_moves(pgn: str) -> List[str]:
    '''
    Converts pgn game format to a list of SAN moves.
    '''
    game = pgn.split(' ')
    san_moves = [game[i] for i in range(1, len(game), 4)]
    return san_moves

def san_to_uci(san_moves: List[str]) -> List[str]:
    '''Converts an array of SAN format moves to UCI format moves'''
    board = chess.Board()
    uci_moves = []
    for move in san_moves:
        uci_moves.append(board.uci(board.parse_san(move)))
        board.push_san(move)
    return uci_moves

def get_evaluations(uci_moves: List[str], stockfish: Stockfish=None) -> List[dict]:
    '''Takes in an array of UCI moves and outputs array containing the Stockfish evaluation after each move'''
    if not stockfish:
        stockfish = new_stockfish()
    eval_array = []
    for move in uci_moves:
        stockfish.make_moves_from_current_position([move])
        stockfish_evaluation = stockfish.get_evaluation()
        eval_array.append(stockfish_evaluation)
    return eval_array

def game_state(evaluation: dict) -> int:
    '''Converts a single Stockfish evaluation to a game state'''
    #Game state indicates which side is winning and to what extent
    if evaluation['type'] == 'mate':
        if evaluation['value'] > 0:
            return 4
        elif evaluation['value'] < 0:
            return -4
        else:
            #Return infinity if checkmate is on the board
            return float('inf')
    elif evaluation['type'] == 'cp':
        if evaluation['value'] > 400:
            return 3
        elif evaluation['value'] > 200:
            return 2
        elif evaluation['value'] > 50:
            return 1
        elif evaluation['value'] >= -50:
            return 0
        elif evaluation['value'] >= -200:
            return -1
        elif evaluation['value'] >= -400:
            return -2
        else:
            return -3

def get_blunders(evaluations: List[dict], uci_moves: List[str], san_moves: List[str]) -> List[list]:
    '''Takes in an array of chess moves and outputs the moves which are classified as blunders'''
    game_states = [game_state(i) for i in evaluations]
    blunders = []
    for i in range(len(evaluations) - 1):
        delta = game_states[i + 1] - game_states[i]
        colour = 'white' if (i + 1) % 2 == 0 else 'black'
        #Game is over due to checkmate
        if delta == float('inf'):
            continue
        #Change in game state of 2 or greater indicates a blunder
        elif abs(delta) >= 2: 
            blunders.append([san_moves[i + 1], uci_moves[i + 1], colour, (i + 1) % 2, ((i + 1) // 2) + 1, game_states[i + 1], game_states[i]] + list(evaluations[i + 1].values()) + list(evaluations[i].values()))
        #Change in game state of 1 indicates a blunder if change in evaluation is also high
        elif abs(delta) == 1 and abs(evaluations[i + 1]['value'] - evaluations[i]['value']) >= 300 and evaluations[i + 1]['type'] != 'mate' and evaluations[i]['type'] != 'mate':
            blunders.append([san_moves[i + 1], uci_moves[i + 1], colour, (i + 1) % 2, ((i + 1) // 2) + 1, game_states[i + 1], game_states[i]] + list(evaluations[i + 1].values()) + list(evaluations[i].values()))
    return blunders

def handler(event, context):
    bucket_name = event['bucket_name']
    object_key = event['object_key']
    print(bucket_name, object_key)
    output_object_key = object_key.replace('games', 'moves')

    s3_client = boto3.client('s3')
    object = s3_client.get_object(
        Bucket=bucket_name,
        Key=object_key
    )

    file_stream = io.BytesIO(object['Body'].read())
    games_df = pd.read_parquet(file_stream)
    games = games_df[['url', 'game_date', 'moves']].to_numpy()
    blunders_dfs = []
    for url, date, game in games:
        san_moves = get_san_moves(game)
        uci_moves = san_to_uci(san_moves)
        evaluations = get_evaluations(uci_moves)
        blunders = get_blunders(evaluations, uci_moves, san_moves)
        df = pd.DataFrame(blunders, columns=['san', 'uci', 'colour', 'side', 'move','game_state_after', 'game_state_before', 'eval_type_after', 'eval_after', 'eval_type_before', 'eval_before'])
        df['url'] = url
        df['game_date'] = date
        df.game_date = df.game_date.astype('datetime64[us]')
        blunders_dfs.append(df)

    blunders_df = pd.concat(blunders_dfs)
    table = pa.Table.from_pandas(blunders_df)
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer)
    s3_client.put_object(Bucket=bucket_name, Key=output_object_key, Body=parquet_buffer.getvalue())
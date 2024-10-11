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

def pgn_to_san(pgn: str) -> List[str]:
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

def material_evaluation(stockfish: Stockfish) -> int:
    '''Takes in a chess position and outputs the material evaluation as an integer'''
    from collections import Counter
    piece_values = {'R': 5, 'N': 3, 'B': 3, 'Q': 9, 'P': 1, 'r': -5, 'n': -3, 'b': -3, 'q': -9, 'p': -1}
    fen = stockfish.get_fen_position()
    fen = fen.split(' ')[0]
    pieces = Counter(fen)
    res = 0
    for piece in pieces:
        if piece_values.get(piece):
            res += piece_values[piece]*pieces[piece]
    return res

def get_evaluations(uci_moves: List[str], stockfish: Stockfish=None) -> List[dict]:
    '''Takes in an array of UCI moves and outputs array containing the Stockfish evaluation after each move'''
    if not stockfish:
        stockfish = new_stockfish()
    eval_array = []
    for move in uci_moves:
        stockfish.make_moves_from_current_position([move])
        stockfish_evaluation = stockfish.get_evaluation()
        stockfish_evaluation['material_eval'] = material_evaluation(stockfish)
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

def get_moves(evaluations: List[dict], uci_moves: List[str], san_moves: List[str]) -> pd.DataFrame:
    '''Combine moves and evaluation arrays to produce a dataset of moves'''
    game_states = [game_state(evaluation) for evaluation in evaluations]
    moves = []
    n = len(evaluations)
    for i in range(n):
        move = [
            san_moves[i], 
            uci_moves[i], 
            'white' if i % 2 == 0 else 'black', 
            i % 2, 
            (i // 2) + 1, 
            game_states[i] if game_states[i] < float('inf') else None, 
            game_states[i - 1] if i > 0 else 0,
            *evaluations[i].values(),
            *(evaluations[i - 1].values() if i > 0 else ['cp', 31, 0]),
            0 # Default value for flag_blunder is 0
            ]
        # Assign a game state 5/-5 for checkmate.
        if move[5] is None and i % 2 == 0:
            move[5] = 5
        elif move[5] is None and i % 2 == 1:
            move[5] = -5
        # Assign value for flag_blunder.
        delta = (game_states[i] - game_states[i - 1]) if i > 0 else None 
        # Checkmate or first move cannot be a blunder.
        if not delta or delta == float('inf'):
            pass
        # Change in game state of 2 or greater indicates a blunder.
        elif abs(delta) >= 2: 
            move[-1] = 1
        # Change in game state of 1 indicates a blunder if change in evaluation is also high.
        elif abs(delta) == 1 and abs(evaluations[i]['value'] - evaluations[i - 1]['value']) >= 300 and evaluations[i]['type'] != 'mate' and evaluations[i - 1]['type'] != 'mate':
            move[-1] = 1   
        # Assign value for flag_capture
        if move[12] != move[9]:
            move.append(1)
        else:
            move.append(0)
        moves.append(move)
    df = pd.DataFrame(moves, columns=[
            'san', 
            'uci', 
            'colour', 
            'side', 
            'move',
            'game_state_after', 
            'game_state_before', 
            'eval_type_after', 
            'eval_after',
            'material_eval_after', 
            'eval_type_before', 
            'eval_before', 
            'material_eval_before',
            'flag_blunder',
            'flag_capture'
            ])
    return df

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
        san_moves = pgn_to_san(game)
        uci_moves = san_to_uci(san_moves)
        evaluations = get_evaluations(uci_moves)
        df = get_moves(evaluations, uci_moves, san_moves)
        df['url'] = url
        df['game_date'] = date
        df.game_date = df.game_date.astype('datetime64[us]')
        blunders_dfs.append(df)
    blunders_df = pd.concat(blunders_dfs)
    blunders_df['flag_checkmate'] = blunders_df.san.apply(lambda x: 1 if x[-1] in '#' else 0)
    blunders_df['flag_check'] = blunders_df.san.apply(lambda x: 1 if x[-1] in '#+' else 0)
    blunders_df = blunders_df[[
            'game_date',
            'url',
            'san', 
            'uci', 
            'colour', 
            'side', 
            'move',
            'game_state_after', 
            'game_state_before', 
            'eval_type_after', 
            'eval_after',
            'material_eval_after', 
            'eval_type_before', 
            'eval_before', 
            'material_eval_before', 
            'flag_blunder',
            'flag_checkmate',
            'flag_check',
            'flag_capture'
            ]]
    table = pa.Table.from_pandas(blunders_df, preserve_index=False)
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer)
    s3_client.put_object(Bucket=bucket_name, Key=output_object_key, Body=parquet_buffer.getvalue())
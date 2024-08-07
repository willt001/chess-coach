from stockfish import Stockfish
import chess
import pandas as pd
import s3fs
from airflow.hooks.base import BaseHook

def pgn_to_san(game):
    '''Converts PGN game format to the SAN game format'''
    game = game.split(' ')
    n = len(game)
    moves_array = []
    for i in range(1, n, 4):
        moves_array.append(game[i])
    return moves_array

def san_to_uci(moves_array):
    '''Converts an array of SAN format moves to UCI format moves'''
    board = chess.Board()
    res = []
    for move in moves_array:
        res.append(board.uci(board.parse_san(move)))
        board.push_san(move)
    return res

def material_evaluation(fish):
    '''Takes in a chess position and outputs the material evaluation as an integer'''
    from collections import Counter
    piece_values = {'R': 5, 'N': 3, 'B': 3, 'Q': 9, 'P': 1, 'r': -5, 'n': -3, 'b': -3, 'q': -9, 'p': -1}
    fen = fish.get_fen_position()
    fen = fen.split(' ')[0]
    pieces = Counter(fen)
    res = 0
    for piece in pieces:
        if piece_values.get(piece):
            res += piece_values[piece]*pieces[piece]
    return res

def get_evaluation_array(moves_array):
    '''Takes in an array of chess moves and outputs array containing the Stockfish evaluation and material evaluation after each move'''    
    moves_array = san_to_uci(moves_array)
    stockfish = Stockfish(r'/usr/games/stockfish')
    eval_array = []
    for move in moves_array:
        stockfish.make_moves_from_current_position([move])
        stockfish_evaluation = stockfish.get_evaluation()
        stockfish_evaluation['material']=material_evaluation(stockfish)
        eval_array.append(stockfish_evaluation)
    return eval_array

def game_state(evaluation):
    '''Converts a single Stockfish evaluation number to a game state'''
    #Game state indicates which side is winning and to what extent
    if evaluation['type']=='mate':
        if evaluation['value']>0:
            return 4
        elif evaluation['value']<0:
            return -4
        else:
            #Return infinity if checkmate is on the board
            return float('inf')
    elif evaluation['type']=='cp':
        if evaluation['value']>400:
            return 3
        elif evaluation['value']>200:
            return 2
        elif evaluation['value']>50:
            return 1
        elif evaluation['value']>=-50:
            return 0
        elif evaluation['value']>=-200:
            return -1
        elif evaluation['value']>=-400:
            return -2
        else:
            return -3

def get_blunders(moves_array):
    '''Takes in an array of chess moves and outputs the moves which are classified as blunders'''
    uci_moves = san_to_uci(moves_array)
    eval_array = get_evaluation_array(moves_array)
    state_array = [game_state(i) for i in eval_array]
    blunders = []
    for i in range(len(eval_array)-1):
        delta = state_array[i+1] - state_array[i]
        #Game is over due to checkmate
        if delta==float('inf'):
            continue
        #Change in game state of 2 or greater indicates a blunder
        elif abs(delta) >= 2: 
            blunders.append([moves_array[i+1], uci_moves[i+1], (i+1)%2, (i//2)+1, state_array[i+1], state_array[i]] + list(eval_array[i+1].values()) + list(eval_array[i].values()))
        #Change in game state of 1 indicates a blunder if change in evaluation is also high
        elif abs(delta)==1 and abs(eval_array[i+1]['value']-eval_array[i]['value'])>=300 and (eval_array[i+1]['type']!='mate' or eval_array[i+1]['type']!='mate'):
            blunders.append([moves_array[i+1], uci_moves[i+1], (i+1)%2, (i//2)+1, state_array[i+1], state_array[i]] + list(eval_array[i+1].values()) + list(eval_array[i].values()))
    return blunders

def get_monthly_blunders(date):
    '''Takes in game data downloaded from S3 bucket and outputs a csv to S3 bucket with monthly blunders dataset'''
    year_month = str(date)[:7].replace('-', '_')
    #S3 Credentials are securely stored in Airflow connections list
    connection = BaseHook.get_connection('chess_project_s3')
    my_key = connection.login
    my_secret = connection.password
    s3 = s3fs.S3FileSystem(anon=False, key=my_key, secret=my_secret)
    #Return None if there are no chess games in S3 bucket for given month
    try:
        with s3.open(f's3://chess-coach-de-project/{year_month}_games.csv', 'r') as f:
            df = pd.read_csv(f)
    except FileNotFoundError:
        print(f'No games for period {year_month}')
        return
    except Exception as e:
        print(f'An error occured: {e}')
        return
    #Store the relevant fields from games data
    games, urls, dates, datenums = df.loc[:, 'moves'], df.loc[:, 'url'], df.loc[:, 'date'], df.loc[:, 'datenum']
    df_blunders =  pd.DataFrame(columns=['game_url', 'date', 'datenum', 'san', 'uci', 'side', 'move', 'game_state_after', 'game_state_before', 'evaluation_type_after', 'evaluation_after', 'material_evaluation_after', 'evaluation_type_before', 'evaluation_before', 'material_evaluation_before'])
    n=0
    for game, url, day, daynum in zip(games, urls, dates, datenums):
        moves_array = pgn_to_san(game)
        blunders = get_blunders(moves_array)
        for blunder in blunders:
            df_blunders.loc[n]=[url] + [day] + [daynum] + blunder 
            n+=1

    df_blunders['colour']=df_blunders['side'].apply(lambda x: 'white'  if x==0 else 'black')
    #Load moves data to S3 bucket
    with s3.open(f's3://chess-coach-de-project/{year_month}_moves.csv', 'w') as f:
        df_blunders.to_csv(f)
    
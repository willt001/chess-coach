# Chess coaching data
Anyone who regularly plays chess on Chess.com may know that they have excellent tools for analysing individual chess games such as game review and analysis board. However, there are no tools for analysing trends or aggregated game data, this is the problem I have aimed to solve with this project!

I have created a pipeline which ingests data from Chess.com's public API, stores the game data in AWS S3, analyses the game data with the chess engine 'Stockfish' to obtain blunders (moves which are mistakes) data, and finally loads the data to AWS Redshift where it is ready for analytical queries. 

The pipeline is orchestrated using Apache Airflow and I have created a custom Dockerfile which can be used to run the project on a local machine. Alternatively the project can be deployed to an AWS EC2 machine but I choose to run this on my local machine because the Chess.com API only offers game data in monthly batches, meaning the pipeline only needs to be ran once per month.

## Airflow DAG
The below DAG shows the 4 tasks in this pipeline and their dependencies.
![chess etl dag](https://github.com/user-attachments/assets/6535cbf2-8f0b-4fe2-9208-1a460becbccd)

## games_etl Task
This task is responsible for ingesting data (if it exists) from Chess.com's API [official documentation](https://www.chess.com/news/view/published-data-api) for a given month and user, after some basic transformations the data is stored in an S3 bucket.

Example data:
![sample game data](https://github.com/user-attachments/assets/fbfd72a4-2366-4bb9-bcc5-88184458ec51)

## blunders_etl Task
This task is responsible for transforming the moves string for each game ingested in the previous task. The output is a new dataset of moves which are classified as blunders which is stored in an S3 bucket.

I am using the Stockfish chess engine for the algorithm to classify blunders, the Stockfish python library works by sending commands to the Stockfish CLI program as a subprocess. 

I have implemented a naive algorithm to classify blunders: 
* Stockfish evaluations are mapped to an integer between 4 and -4, 4 indicates white has a forced checkmate, -4 indicates black has a forced checkmate, 0 is a drawn position (0.5 to -0.5 evaluation), 1 indicates white is slightly better (2 to 0.5 evaluation), etc.
* I have defined a blunder as a move which changes the game state by more than 1, or changes the centipawn evaluation by more than 3.
* In a specific game analysis this algorithm may be insufficient, but in a long term/aggregate analysis it is sufficient.

Example data:
![sample move data](https://github.com/user-attachments/assets/e193132c-34cb-4e04-bf1c-a1f3f511ecc3)

## redshift_game_load and redshift_move_load Tasks
Both these tasks work very similarly, they are responsible for copying the csv data in S3 to Redshift in monthly batches, various CRUD operations are performed:
* Delete records for the given month from the staging table.
* Copy records for the given month to the staging table.
* Check the staging table's row count is not less than the row count for any existing records in the final table. If it is then cancel the transaction.
* Delete records for the given month from the final table.
* Insert records from staging table to final table, applying any final transformations.

The data is then ready for analytical queries to be run, examples below:

Query:
![sample analytical query](https://github.com/user-attachments/assets/f170fcdb-b300-4696-b03b-da1b7c4a4d31)

Result set:
![sample result set](https://github.com/user-attachments/assets/a5d0cfc6-0668-4bc6-ab90-b4c875f5b62a)

# Chess coaching data
Anyone who regularly plays chess on Chess.com may know that they have excellent tools for analysing individual chess games such as game review and analysis board. However, there are no tools for analysing trends or aggregated game data, this is the problem I have aimed to solve with this project!

I have created a pipeline which ingests data from Chess.com's public API, stores the game data in AWS S3, analyses the game data with the chess engine 'Stockfish' to obtain blunders (moves which are mistakes) data, and finally loads the data to AWS Redshift where it is ready for analytical queries. 

The pipeline is orchestrated using Apache Airflow running with Docker Compose. The Docker Desktop application is required to run the containers, then running the command 'docker compose up -d --build' will allow you to view the DAG in the Airflow webserver at localhost:8080. The DAG will fail without creating the necessary AWS infrastructure and corresponding connections in Airflow, though I am looking to create a test version of this pipeline in the future which can be run locally to make the project more sharable.

## Airflow DAG
The below DAG shows the 4 tasks in this pipeline and their dependencies.
![chess_etl](https://github.com/user-attachments/assets/f983b6de-9960-4c16-a033-2c8dcbe489ea)

## download_games_upload_s3 Task
This task is responsible for ingesting data (if it exists) from Chess.com's API [official documentation](https://www.chess.com/news/view/published-data-api) for a given month and user, after some basic transformations the data is stored in an S3 bucket.

Example data:
![sample game data](https://github.com/user-attachments/assets/fbfd72a4-2366-4bb9-bcc5-88184458ec51)

## calculate_blunders_upload_s3 Task
This task is responsible for transforming the moves string for each game ingested in the previous task. The output is a new dataset of moves which are classified as blunders which is stored in an S3 bucket.

I am using the Stockfish chess engine for the algorithm to classify blunders, the Stockfish python library works by sending commands to the Stockfish CLI program as a subprocess. 

I have implemented a naive algorithm to classify blunders: 
* Stockfish evaluations are mapped to an integer between 4 and -4, 4 indicates white has a forced checkmate, -4 indicates black has a forced checkmate, 0 is a drawn position (0.5 to -0.5 evaluation), 1 indicates white is slightly better (2 to 0.5 evaluation), etc.
* I have defined a blunder as a move which changes the game state by more than 1, or changes the centipawn evaluation by more than 3.
* In a specific game analysis this algorithm may be insufficient, but in a long term/aggregate analysis it is sufficient.

Example data:
![sample move data](https://github.com/user-attachments/assets/e193132c-34cb-4e04-bf1c-a1f3f511ecc3)

## load_games_to_redshift and load_moves_to_redshift Tasks
Both these tasks work very similarly, they are responsible for copying the csv data in S3 to Redshift in monthly batches, various CRUD operations are performed:
* Delete records for the given month from the staging table.
* Copy records for the given month to the staging table.
* Check the staging table's row count is not less than the row count for any existing records in the final table. If it is then cancel the transaction.
* Delete records for the given month from the final table.
* Insert records from staging table to final table, applying any final transformations.

The data is then ready for analytical queries to be run, examples below:

![sample analytical query](https://github.com/user-attachments/assets/f170fcdb-b300-4696-b03b-da1b7c4a4d31)

Result set:
![sample result set](https://github.com/user-attachments/assets/a5d0cfc6-0668-4bc6-ab90-b4c875f5b62a)

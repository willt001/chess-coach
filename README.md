# chess-coach
Anyone who regularly plays chess on Chess.com may know that they have excellent tools for analysing individual chess games such as game review and analysis board. However, there are no tools for analysing trends or aggregated game data, this is the problem I have aimed to solve with this project!

I have created a pipeline which ingests data from Chess.com's public API, stores the game data in AWS S3, analyses the game data with the chess engine 'Stockfish' to obtain blunders (moves which are mistakes) data, and finally loads the data to AWS Redshift. 

The pipeline is orchestrated using Apache Airflow and I have created a custom Dockerfile which can be used to run the project on a local machine. Alternatively the project can be deployed to an AWS EC2 machine but I choose to run this on my local machine because the Chess.com API only offers game data in monthly batches, meaning the pipeline only needs to be ran once per month.

# dag
![chess etl dag](https://github.com/user-attachments/assets/6535cbf2-8f0b-4fe2-9208-1a460becbccd)

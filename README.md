# chess-coach
Anyone who regularly plays chess on Chess.com may know that they have excellent tools for analysing individual chess games such as game review and analysis board. However, there are no tools for analysing trends or aggregated game data, this is the problem I have aimed to solve with this project!

In this project I have created a pipeline which ingests data from Chess.com's public API, stores the game data in S3, analyses the game data with the chess engine 'Stockfish' to obtain blunders (moves which are mistakes) data, and finally loading the data to Redshift.

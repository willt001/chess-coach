FROM apache/airflow:2.9.1
RUN python -m pip install s3fs
RUN python -m pip install stockfish
RUN python -m pip install chess
RUN python -m pip install regex
USER root
RUN apt-get update
RUN apt-get install stockfish
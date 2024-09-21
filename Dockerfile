FROM apache/airflow:2.9.1
COPY requirements.txt .
RUN python -m pip install -r requirements.txt
USER root
RUN apt-get update
RUN apt-get install stockfish
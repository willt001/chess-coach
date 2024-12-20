# Chess coach data pipeline
Anyone who regularly plays chess on Chess.com may know that they have excellent tools for analysing individual chess games such as game review and analysis board. However, there are no tools for analysing trends or aggregated game data, this is the problem I have solved with this project! Sample data in both csv and parquet format can be found in the sample_data directory.

I have created a pipeline which ingests games data from Chess.com's public API and analyses the games data with chess engine Stockfish 17 to obtain a rich dataset of moves, with valuable attributes such as flag_blunder, flag_capture, flag_check, etc.

The pipeline is hosted on AWS using serverless architecture: S3 for storage, Lambda for compute, Glue for data catalog, and Athena for analytics. The pipeline is orchestrated using Apache Airflow and can be run using Docker Desktop and running the following command inside the repository directory.
```bash
docker compose up -d --build
```
## ETL DAG
![image](https://github.com/user-attachments/assets/5c9f4945-088e-4cbd-abb6-6c6399822520)

## extract_games Task
Extracting data from Chess.com's official API for a given month and user. Each extract is partitioned into parquet files of 50 records each and temporarily stored on the local file system of the Airflow worker (with Apache Hive style partitioning on 'month'). 

Each batch of 50 chess games will take approximately 10 minutes to process with the Lambda function, partition size of 50 games was chosen to fall below the maximum timeout of 15 minutes on Lambda. 

I push an XCom containing file names for each of the parquet files created to be dynamically used by downstream tasks.

## process_partiton Task Group
Using dynamic task mapping on each of the parquet files produced by the extract_games task.

## local_to_s3 Task
Copying parquet file from local file system on Airflow worker to 'games' table in S3 with Hive style partitioning (so Glue data crawler will automatically detect a partitioned table). After copying to S3, the local file is cleaned up with the 'delete_local_file' bash task.

![hive partitions](https://github.com/user-attachments/assets/c2c5224a-51cc-4b45-bcdc-ca571a7a6f52)
![image](https://github.com/user-attachments/assets/934b2a41-c96f-4800-a8f2-32e225b5c5ce)

## calculate_blunders Task
Invoking the Lambda function to process one partition and save the output to a seperate 'moves' table in S3.

The Stockfish chess engine is used for the algorithm to classify moves as blunders. Since the Stockfish engine is only available as an executable file (which I use the Stockfish python library to interact with), the data needs to be processed in an environment where the Stockfish binary is installed. 

I solved this problem by writing a Dockerfile to install and compile the Stockfish source code on an existing AWS Lambda Python image. Lambda provides functionality to use a docker image which has been pushed to Elastic Container Registry as an environment, more details on this below.

Typically, machine learning and advanced metrics such as expected win probability are used to classify blunders. However, for an aggregated analysis, the naive algorithm I have derived and detailed below proves sufficient:
* Stockfish evaluations are mapped to an integer 'game state' between 4 and -4:
    * 4: Forced checkmate for white.
    * 3: Strong advantage for white.
    * 2: Advantage for white.
    * 1: Slight advantage for white.
    * 0: Drawing position
    * -1: Slight advantage for black.
    * -2: Advantage for black.
    * -3: Strong advantage for black.
    * -4: Forced checkmate for black.
* A blunder is a move which shifts the game state by more than 1, or shifts the Stockfish evaluation by more than 3.

## Glue Crawler DAG

![image](https://github.com/user-attachments/assets/4b719b05-c751-4a46-8b09-5646c99082a6)

## crawl_s3 Task

Running the AWS Glue Crawler to update the data catalog with the new partitions added to the data lake in S3, the crawl_s3 task is skipped if a crawler is already running (from a parallel DagRun). The latest data will then be available to query in Athena.

## AWS Architecture
![chess etl flow](https://github.com/user-attachments/assets/a6f7f41d-5f09-4f75-ad0a-0c556b8ac54f)

## Lambda Function

The code which runs on AWS Lambda is the 'handler' function found in the lambda-docker-image/lambda_function.py file. The value for the 'event' argument will be the JSON value we pass to the 'payload' argument of LambdaInvokeFunctionOperator in Airflow.

## Deploying function code to AWS (Using Docker image)

The prerequisites for deploying the function code are to have the AWS CLI and Docker Desktop installed on your development environment. [Reference](https://docs.aws.amazon.com/AmazonECR/latest/userguide/getting-started-cli.html)

* Build the docker image from the Dockerfile. The Dockerfile installs the dependencies and compiles the Stockfish 17 source code from https://github.com/official-stockfish/Stockfish.
```bash
cd lambda-docker-image | docker build -t <image-name> .
```
* Create a repository in ECR to store the docker image.
```bash
aws ecr create-repository --repository-name <repository-name> --region <region-name>
```
* Authenticate Docker to the AWS ECR repository.
```bash
aws ecr get-login-password --region <region-name> | docker login --username AWS --password-stdin <aws_account_id>.dkr.ecr.<region-name>.amazonaws.com
```
* Tag the Docker image.
```bash
docker tag <image-name>:latest <aws_account_id>.dkr.ecr.<region-name>.amazonaws.com/<repository-name>
```
* Push the docker image to ECR.
```bash
docker push <aws_account_id>.dkr.ecr.<region-name>.amazonaws.com/<repository-name>
```
* Navigate to ECR in the AWS Management Console and copy the Image URI.
* Navigate to Lambda in the AWS Management Console and use the Image URI to create a Lambda Function.

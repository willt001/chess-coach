## Chess coach data pipeline
Anyone who regularly plays chess on Chess.com may know that they have excellent tools for analysing individual chess games such as game review and analysis board. However, there are no tools for analysing trends or aggregated game data, this is the problem I have solved with this project!

I have created a pipeline which ingests games data from Chess.com's public API and analyses the games data with chess engine Stockfish 17 to obtain data for blunders (moves which are mistakes). 

The pipeline is hosted on AWS using serverless architecture: S3 for storage, Lambda for compute, Glue for data catalog, and Athena for analytics. The pipeline is orchestrated using Apache Airflow and can be run using Docker Desktop and running the command 'docker compose up -d --build' inside the repository directory.

# Airflow DAG
![chess lambda DAG](https://github.com/user-attachments/assets/369af722-e6a7-46b8-8ede-6db3e52d95b6)
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

## crawl_s3 Task

Running the AWS Glue Crawler to update the data catalog with the new partitions added to the data lake in S3. The latest data will then be available to query in Athena.

# Lambda Function

The code which runs on AWS Lambda is the 'handler' function found in the lambda-docker-image/lambda_function.py file. The value for the 'event' argument will be the JSON value we pass to the 'payload' argument of LambdaInvokeFunctionOperator in Airflow.

## Deploying function code to AWS (Using Docker image)

The prerequisites for deploying the function code are to have the AWS CLI and Docker Desktop installed on your development environment. [Reference](https://docs.aws.amazon.com/AmazonECR/latest/userguide/getting-started-cli.html)

* cd into the lambda-docker-image directory and run the 'docker build -t **image-name** .' command to build the docker image from the Dockerfile. The Dockerfile installs the dependencies and compiles the Stockfish 17 source code from https://github.com/official-stockfish/Stockfish.
* Create a repository in ECR to store the docker image with 'aws ecr create-repository --repository-name **repository-name** --region **region-name**'.
* Authenticate Docker to the AWS ECR repository with 'aws ecr get-login-password --region **region-name** | docker login --username AWS --password-stdin **aws_account_id**.dkr.ecr.**region-name**.amazonaws.com'.
* Tag the docker image with 'docker tag **image-name**:latest **aws_account_id**.dkr.ecr.**region-name**.amazonaws.com/**repository-name**'.
* Push the docker image to ECR with 'docker push **aws_account_id**.dkr.ecr.**region-name**.amazonaws.com/**repository-name**'.
* Navigate to ECR in the AWS Management Console and copy the Image URI.
* Navigate to Lambda in the AWS Management Console and use the Image URI to create a Lambda Function.

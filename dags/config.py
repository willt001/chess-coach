import json


BUCKET_NAME = "chess-coach-041"
LAMBDA_FUNCTION_NAME = 'calculate-blunders'
GLUE_CRAWLER_CONFIG = json.dumps({
        "Name": "chess-coach-crawler",
        "Role": "arn:aws:iam::905418173427:role/AirflowChessCoachCrawler",
        "DatabaseName": "chess-coach",
        "Targets": {"S3Targets": [{"Path": "chess-coach-041"}]},
    })
REGION_NAME = "eu-north-1"
# create_ugu_points.py
import boto3
import os
from dotenv import load_dotenv

load_dotenv()  # ← .env を読む（AWS_ACCESS_KEY_ID などを取り込む）

region = os.getenv("AWS_REGION", "ap-northeast-1")
table_name = os.getenv("DYNAMO_UGU_POINTS_TABLE", "ugu_points")

dynamodb = boto3.client("dynamodb", region_name=region)

resp = dynamodb.create_table(
    TableName=table_name,
    AttributeDefinitions=[
        {"AttributeName": "user_id", "AttributeType": "S"},
        {"AttributeName": "point_id", "AttributeType": "S"},
    ],
    KeySchema=[
        {"AttributeName": "user_id", "KeyType": "HASH"},
        {"AttributeName": "point_id", "KeyType": "RANGE"},
    ],
    BillingMode="PAY_PER_REQUEST",
)

print("creating table...", resp)

waiter = dynamodb.get_waiter("table_exists")
waiter.wait(TableName=table_name)
print("table created:", table_name)

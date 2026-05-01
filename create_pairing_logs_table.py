"""
bad-pairing-logs テーブルを DynamoDB に作成するスクリプト
ローカルで実行: python create_pairing_logs_table.py
"""

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

load_dotenv()

dynamodb = boto3.resource(
    'dynamodb',
    region_name=os.getenv('AWS_REGION', 'ap-northeast-1'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
)

TABLE_NAME = 'bad-pairing-logs'

try:
    table = dynamodb.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {'AttributeName': 'match_id', 'KeyType': 'HASH'},
        ],
        AttributeDefinitions=[
            {'AttributeName': 'match_id', 'AttributeType': 'S'},
        ],
        BillingMode='PAY_PER_REQUEST',
    )
    table.wait_until_exists()
    print(f"✅ テーブル '{TABLE_NAME}' を作成しました")

except ClientError as e:
    if e.response['Error']['Code'] == 'ResourceInUseException':
        print(f"ℹ️  テーブル '{TABLE_NAME}' は既に存在します")
    else:
        print(f"❌ エラー: {e}")

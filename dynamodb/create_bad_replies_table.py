import boto3
import os
from dotenv import load_dotenv

# .envファイルを読み込む
load_dotenv()

# テーブル名の定義
REPLIES_TABLE_NAME = 'replies'

def init_dynamodb():
    """
    DynamoDB クライアントを初期化する
    """
    return boto3.resource(
        'dynamodb',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )

def create_replies_table():
    dynamodb = init_dynamodb()
    try:
        dynamodb.create_table(
            TableName=REPLIES_TABLE_NAME,
            KeySchema=[
                {'AttributeName': 'post_id', 'KeyType': 'HASH'},
                {'AttributeName': 'created_at', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'post_id', 'AttributeType': 'S'},
                {'AttributeName': 'created_at', 'AttributeType': 'S'},
                {'AttributeName': 'reply_id', 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'reply_id_index',
                    'KeySchema': [
                        {'AttributeName': 'reply_id', 'KeyType': 'HASH'}
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        print("Created replies table")
        return True
    except dynamodb.exceptions.ResourceInUseException:
        print("Replies table already exists")
        return False

def verify_table():
    """テーブルの構造を確認する"""
    dynamodb = init_dynamodb()
    try:
        table = dynamodb.Table(REPLIES_TABLE_NAME)
        print("\nTable details:")
        print(f"Name: {table.table_name}")
        print(f"Key Schema: {table.key_schema}")
        print(f"Attribute Definitions: {table.attribute_definitions}")
        if table.global_secondary_indexes:
            print(f"GSI: {table.global_secondary_indexes}")
        return True
    except Exception as e:
        print(f"Error verifying table: {e}")
        return False

if __name__ == '__main__':
    created = create_replies_table()
    if created or not created:  # テーブルが新規作成でもすでに存在でも確認を行う
        verify_table()
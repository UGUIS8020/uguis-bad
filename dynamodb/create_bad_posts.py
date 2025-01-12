import boto3
import os
from dotenv import load_dotenv

# .envファイルを読み込む
load_dotenv()

# テーブル名の定義
POSTS_TABLE_NAME = 'posts'
FOLLOWS_TABLE_NAME = 'follows'

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

# def create_posts_table(dynamodb):
#     """
#     Posts テーブルを作成する
#     """
#     try:
#         table = dynamodb.create_table(
#             TableName=POSTS_TABLE_NAME,
#             KeySchema=[
#                 {'AttributeName': 'PK', 'KeyType': 'HASH'},
#                 {'AttributeName': 'SK', 'KeyType': 'RANGE'}
#             ],
#             AttributeDefinitions=[
#                 {'AttributeName': 'PK', 'AttributeType': 'S'},
#                 {'AttributeName': 'SK', 'AttributeType': 'S'},
#                 {'AttributeName': 'GSI1PK', 'AttributeType': 'S'},
#                 {'AttributeName': 'GSI1SK', 'AttributeType': 'S'}
#             ],
#             GlobalSecondaryIndexes=[
#                 {
#                     'IndexName': 'GSI1',
#                     'KeySchema': [
#                         {'AttributeName': 'GSI1PK', 'KeyType': 'HASH'},
#                         {'AttributeName': 'GSI1SK', 'KeyType': 'RANGE'}
#                     ],
#                     'Projection': {'ProjectionType': 'ALL'}
#                 }
#             ],
#             BillingMode='PAY_PER_REQUEST'
#         )
#         print(f"テーブル '{POSTS_TABLE_NAME}' を作成中...")
#         table.meta.client.get_waiter('table_exists').wait(TableName=POSTS_TABLE_NAME)
#         print(f"テーブル '{POSTS_TABLE_NAME}' が作成されました。")
#         return table
#     except Exception as e:
#         print(f"Postsテーブルの作成中にエラーが発生しました: {str(e)}")
#         raise

def create_posts_table(dynamodb):
    """
    Posts テーブルを作成する
    """
    try:
        table = dynamodb.create_table(
            TableName=POSTS_TABLE_NAME,
            KeySchema=[
                {'AttributeName': 'PK', 'KeyType': 'HASH'},
                {'AttributeName': 'SK', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'PK', 'AttributeType': 'S'},
                {'AttributeName': 'SK', 'AttributeType': 'S'},
                {'AttributeName': 'GSI1PK', 'AttributeType': 'S'},
                {'AttributeName': 'GSI1SK', 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'GSI1',
                    'KeySchema': [
                        {'AttributeName': 'GSI1PK', 'KeyType': 'HASH'},
                        {'AttributeName': 'GSI1SK', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                }
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        print(f"テーブル '{POSTS_TABLE_NAME}' を作成中...")
        table.meta.client.get_waiter('table_exists').wait(TableName=POSTS_TABLE_NAME)
        print(f"テーブル '{POSTS_TABLE_NAME}' が作成されました。")
        return table
    except Exception as e:
        print(f"Postsテーブルの作成中にエラーが発生しました: {str(e)}")
        raise



def create_follows_table(dynamodb):
    """
    Follows テーブルを作成する
    """
    try:
        table = dynamodb.create_table(
            TableName=FOLLOWS_TABLE_NAME,
            KeySchema=[
                {'AttributeName': 'followerId', 'KeyType': 'HASH'},
                {'AttributeName': 'followedId', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'followerId', 'AttributeType': 'S'},
                {'AttributeName': 'followedId', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        print(f"テーブル '{FOLLOWS_TABLE_NAME}' を作成中...")
        table.meta.client.get_waiter('table_exists').wait(TableName=FOLLOWS_TABLE_NAME)
        print(f"テーブル '{FOLLOWS_TABLE_NAME}' が作成されました。")
        return table
    except Exception as e:
        print(f"Followsテーブルの作成中にエラーが発生しました: {str(e)}")
        raise

def create_remaining_tables():
    """
    Posts と Follows テーブルを作成する
    """
    try:
        dynamodb = init_dynamodb()
        create_posts_table(dynamodb)
        create_follows_table(dynamodb)
        print("全てのテーブルが正常に作成されました。")
    except Exception as e:
        print(f"テーブル作成中にエラーが発生しました: {str(e)}")

if __name__ == "__main__":
    create_remaining_tables()
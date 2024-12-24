import boto3
import os

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

def create_table():
    """
    DynamoDBテーブル 'bad-posts' を作成する
    """
    dynamodb = init_dynamodb()

    table_name = "bad-posts"

    try:
        # テーブルを作成
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'post_user_id', 'KeyType': 'HASH'},  # パーティションキー
                {'AttributeName': 'updated_at', 'KeyType': 'RANGE'}   # ソートキー
            ],
            AttributeDefinitions=[
                {'AttributeName': 'post_user_id', 'AttributeType': 'S'},  # String型
                {'AttributeName': 'updated_at', 'AttributeType': 'S'}    # String型
            ],
            BillingMode='PAY_PER_REQUEST'  # オンデマンド課金
        )

        # テーブルが作成されるまで待機
        print(f"テーブル '{table_name}' を作成中...")
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        print(f"テーブル '{table_name}' が作成されました。")

    except Exception as e:
        print(f"テーブルの作成中にエラーが発生しました: {str(e)}")

if __name__ == "__main__":
    create_table()
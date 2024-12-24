from dotenv import load_dotenv
import os
import boto3
from botocore.exceptions import ClientError

# .envファイルから環境変数を読み込む
load_dotenv()

# AWS認証情報を辞書として定義
aws_credentials = {
    'aws_access_key_id': os.getenv("AWS_ACCESS_KEY_ID"),
    'aws_secret_access_key': os.getenv("AWS_SECRET_ACCESS_KEY"),
    'region_name': os.getenv("AWS_REGION", "us-east-1")  # デフォルト値を設定
}

def create_bad_schedules_table_with_gsi():
    """DynamoDBテーブルを作成し、GSIを追加"""
    table_name = 'bad_schedules'
    gsi_name = 'status-date-index'

    try:
        # DynamoDBリソースを作成
        dynamodb = boto3.resource('dynamodb', **aws_credentials)
        
        # テーブルが既に存在するか確認
        existing_tables = list(dynamodb.tables.all())
        if any(table.name == table_name for table in existing_tables):
            print(f"テーブル '{table_name}' は既に存在します")
            return dynamodb.Table(table_name)

        # テーブルの作成
        print(f"テーブル '{table_name}' を作成中...")
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'schedule_id', 'KeyType': 'HASH'},  # Partition Key
                {'AttributeName': 'date', 'KeyType': 'RANGE'}        # Sort Key
            ],
            AttributeDefinitions=[
                {'AttributeName': 'schedule_id', 'AttributeType': 'S'},  # String型
                {'AttributeName': 'date', 'AttributeType': 'S'},         # String型
                {'AttributeName': 'status', 'AttributeType': 'S'}        # GSIのPartition Key
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': gsi_name,  # GSI名
                    'KeySchema': [
                        {'AttributeName': 'status', 'KeyType': 'HASH'},  # GSIのPartition Key
                        {'AttributeName': 'date', 'KeyType': 'RANGE'}   # GSIのSort Key
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'  # 必要なすべての属性を含める
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

        # テーブル作成完了まで待機
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        print(f"テーブル '{table_name}' が正常に作成されました")
        return table

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ResourceInUseException':
            print(f"テーブル '{table_name}' は既に存在します")
            return dynamodb.Table(table_name)
        else:
            print(f"クライアントエラーが発生しました: {e.response['Error']['Message']}")
            return None
    except Exception as e:
        print(f"予期しないエラーが発生しました: {str(e)}")
        return None

def verify_credentials():
    """AWS認証情報の検証"""
    try:
        dynamodb = boto3.resource('dynamodb', **aws_credentials)
        list(dynamodb.tables.all())  # テーブル一覧を取得して認証を確認
        print("AWS認証情報は有効です")
        return True
    except Exception as e:
        print(f"AWS認証情報の検証に失敗しました: {str(e)}")
        return False

if __name__ == '__main__':
    if verify_credentials():
        table = create_bad_schedules_table_with_gsi()
        if table:
            print(f"テーブル状態: {table.table_status}")
    else:
        print("AWS認証情報を確認してください")
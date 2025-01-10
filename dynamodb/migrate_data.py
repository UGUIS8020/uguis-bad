import os
import boto3
from flask import Flask
from dotenv import load_dotenv
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

# Flask アプリケーションの作成
def create_app():
    app = Flask(__name__)
    
    # .envファイルから環境変数をロード
    load_dotenv()

    # AWS DynamoDBリソースの設定
    app.dynamodb = boto3.resource('dynamodb',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "ap-northeast-1")
    )
    
    return app

# DynamoDBリソースの初期化
app = create_app()
dynamodb = app.dynamodb

# 既存テーブルと新しいテーブルの名前
source_table_name = 'bad-schedule'  # 元のテーブル名
destination_table_name = 'bad_schedules'  # 新しいテーブル名

def migrate_data():
    try:
        # テーブルを取得
        source_table = dynamodb.Table(source_table_name)
        destination_table = dynamodb.Table(destination_table_name)

        # データをスキャン（全件取得）
        response = source_table.scan()
        items = response.get('Items', [])

        # ページネーション対応
        while 'LastEvaluatedKey' in response:
            response = source_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))

        print(f"移行対象データ: {len(items)} 件")

        # データを新しいテーブルに挿入
        for item in items:
            # 必要に応じてデータ変換
            new_item = {
                'schedule_id': item.get('schedule_id'),  # 必須フィールド
                'date': item.get('date'),               # 必須フィールド
                'created_at': item.get('created_at', ''),  # デフォルト値を設定
                'day_of_week': item.get('day_of_week', ''),
                'end_time': item.get('end_time', ''),
                'participants': item.get('participants', []),
                'start_time': item.get('start_time', ''),
                'status': item.get('status', 'active'),   # デフォルト値
                'venue': item.get('venue', '')
            }

            # 新しいテーブルにデータを挿入
            destination_table.put_item(Item=new_item)

        print("データ移行が完了しました")

    except ClientError as e:
        print(f"エラーが発生しました: {str(e)}")
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {str(e)}")

if __name__ == '__main__':
    migrate_data()
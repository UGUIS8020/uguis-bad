import boto3
from datetime import datetime
import os
from dotenv import load_dotenv
from flask import Flask
from botocore.exceptions import ClientError
import uuid
from typing import Optional, Dict, Any, List
from werkzeug.security import generate_password_hash

def create_app():
    app = Flask(__name__)
    load_dotenv()

    # AWS DynamoDB リソースの設定
    app.dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "ap-northeast-1")
    )

       

    # 掲示板テーブル名の設定 (デフォルトを"bad-board-table"に変更)
    board_table_name = os.getenv("TABLE_NAME_BOARD", "bad-board-table")
    app.board_table_name = board_table_name

        
    # 掲示板テーブルの初期化
    try:
        app.board_table = app.dynamodb.Table(app.board_table_name)
        print(f"Board table initialized: {app.board_table}")
    except Exception as e:
        print(f"Failed to initialize board table: {e}")
        raise

    return app

if __name__ == "__main__":
    app = create_app()

    # サンプルデータを作成
    sample_post = {
        'user#user_id': 'user#test-user',
        'post#post_id': str(uuid.uuid4()),
        'user_id': 'test-user',
        'title': 'Sample Post',
        'content': 'This is a sample post.',
        'image_url': '',
        'created_at': datetime.now().isoformat(),
        'updated_at': datetime.now().isoformat(),
        'status': 'active'
    }

    # サンプルデータをDynamoDBに挿入
    try:
        app.board_table.put_item(Item=sample_post)
        print("Sample post inserted successfully.")
    except Exception as e:
        print(f"Error inserting sample post: {e}")

    # 再度スキャンしてアイテム数を確認
    try:
        response = app.board_table.scan()
        items = response.get('Items', [])
        print(f"Found {len(items)} item(s) in board table after insertion.")
    except ClientError as e:
        print(f"Error scanning board table: {e}")

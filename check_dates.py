import os
import logging
import boto3
from dotenv import load_dotenv
from dateutil import parser
from datetime import datetime

# .env ファイルから環境変数を読み込む
load_dotenv()

# ロガー設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def check_date_formats():
    # DynamoDBの初期化
    dynamodb = init_dynamodb()
    table = dynamodb.Table('bad-board-table')  # テーブル名を実際のものに変更してください
    
    # 全ての投稿を取得
    logger.info("Scanning DynamoDB table for posts...")
    response = table.scan()
    posts = response.get('Items', [])
    
    logger.info(f"Found {len(posts)} posts")
    print("\n=== 日付フォーマットチェック ===")
    
    for post in posts:
        print("\n投稿ID:", post.get('post#post_id'))
        print("タイトル:", post.get('title'))
        
        # created_atのチェック
        created_at = post.get('created_at')
        print(f"created_at: {created_at} (型: {type(created_at)})")
        try:
            parsed_created = parser.parse(created_at)
            print(f"→ パース後: {parsed_created}")
        except Exception as e:
            logger.error(f"Created_at parse error: {str(e)}")
            print(f"→ パースエラー: {str(e)}")
        
        # updated_atのチェック
        updated_at = post.get('updated_at')
        print(f"updated_at: {updated_at} (型: {type(updated_at)})")
        try:
            parsed_updated = parser.parse(updated_at)
            print(f"→ パース後: {parsed_updated}")
        except Exception as e:
            logger.error(f"Updated_at parse error: {str(e)}")
            print(f"→ パースエラー: {str(e)}")
        
        print("-" * 50)

if __name__ == "__main__":
    try:
        check_date_formats()
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
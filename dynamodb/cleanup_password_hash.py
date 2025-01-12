import boto3
import os
import logging
from dotenv import load_dotenv

# .envファイルを読み込む
load_dotenv()

# ロガー設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_table():
    """
    DynamoDB テーブルを初期化する
    """
    # AWS認証情報を取得
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )

    # テーブル名を環境変数から取得
    table_name = os.getenv("TABLE_NAME_USER")
    table = dynamodb.Table(table_name)
    return table

def clean_password_hash(table):
    """
    DynamoDB から password_hash フィールドを削除する
    """
    try:
        # 全データをスキャン
        response = table.scan()
        items = response.get('Items', [])
        logger.info(f"Found {len(items)} items in DynamoDB.")

        for item in items:
            # password_hash が存在する場合のみ削除
            if 'password_hash' in item:
                user_id = item['user#user_id']
                logger.info(f"Removing 'password_hash' for user: {user_id}")

                # password_hash を削除
                table.update_item(
                    Key={'user#user_id': user_id},
                    UpdateExpression="REMOVE password_hash"
                )

        logger.info("Cleanup completed successfully.")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}", exc_info=True)

if __name__ == "__main__":
    # テーブルを初期化
    table = init_table()
    # パスワードハッシュを削除
    clean_password_hash(table)
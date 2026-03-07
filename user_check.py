import sys
import os
import datetime
# boto3からKeyをインポート
from boto3.dynamodb.conditions import Key

# パスを通す
sys.path.append(os.getcwd())

from app import app
from uguu.dynamo import DynamoDB

TARGET_ID = 'c06221c9-d934-4c35-b45b-b07be711568c'

def run_user_investigation(user_id: str):
    with app.app_context():
        db_instance = DynamoDB()
        
        # 1. ユーザー基本情報
        user_info = db_instance.get_user_info(user_id)
        print(f"\nChecking: {user_info.get('display_name', 'Unknown')} ({user_id})")

        # 2. DynamoDBから生の全レコードを直接取得
        print(f"\n■ DynamoDB 生データ全件表示")
        print(f"{'日付':<12} | {'Status':<12} | {'Action':<15} | {'Registered_at'}")
        print("-" * 70)

        # part_historyテーブルを直接クエリ
        resp = db_instance.part_history.query(
            KeyConditionExpression=Key("user_id").eq(user_id)
        )
        raw_items = resp.get("Items", [])

        # 日付順にソートして表示
        for it in sorted(raw_items, key=lambda x: (x.get('date') or x.get('event_date') or '')):
            d = it.get('date') or it.get('event_date') or 'no-date'
            s = it.get('status', 'none')
            a = it.get('action', 'none')
            # タイムスタンプ系の候補をいくつか確認
            ts = it.get('registered_at') or it.get('created_at') or it.get('joined_at') or 'no-ts'
            
            print(f"{str(d):<12} | {str(s):<12} | {str(a):<15} | {str(ts)}")

        if not raw_items:
            print("レコードが見つかりませんでした。")

        print(f"\n{'#'*70}\n")

if __name__ == "__main__":
    run_user_investigation(TARGET_ID)
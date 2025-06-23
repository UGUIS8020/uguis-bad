import boto3
import os
import time
from datetime import datetime

# AWS設定（環境変数または設定ファイルから読み込む）
AWS_REGION = 'ap-northeast-1'  # 必要に応じて変更

# DynamoDBリソースを初期化
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)

def recreate_match_table():
    """match_entriesテーブルを再作成する"""
    table_name = 'match_entries'
    
    # 既存テーブルが存在する場合は削除
    try:
        table = dynamodb.Table(table_name)
        table.delete()
        print(f"テーブル {table_name} を削除しました")
        time.sleep(10)  # テーブル削除完了を待つ
    except Exception as e:
        print(f"テーブル削除中にエラーが発生しました（存在しない場合もOK）: {str(e)}")
    
    # 新しいテーブルを作成
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'entry_id', 'KeyType': 'HASH'},  # パーティションキー
            ],
            AttributeDefinitions=[
                {'AttributeName': 'entry_id', 'AttributeType': 'S'},
                {'AttributeName': 'user_id', 'AttributeType': 'S'},
                {'AttributeName': 'match_id', 'AttributeType': 'S'},
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'UserMatchIndex',
                    'KeySchema': [
                        {'AttributeName': 'user_id', 'KeyType': 'HASH'},
                        {'AttributeName': 'match_id', 'KeyType': 'RANGE'},
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                },
                {
                    'IndexName': 'MatchIndex',
                    'KeySchema': [
                        {'AttributeName': 'match_id', 'KeyType': 'HASH'},
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                }
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        
        # テーブルの作成完了を待つ
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        print(f"新しいテーブル {table_name} を作成しました")
        return True
    except Exception as e:
        print(f"テーブル作成中にエラーが発生しました: {str(e)}")
        return False

def create_test_data():
    """テスト用のデータを作成する"""
    table = dynamodb.Table('match_entries')
    
    # テストユーザー作成
    test_users = [
        {"user_id": "test_user_1", "display_name": "テスト太郎", "badminton_experience": "初級"},
        {"user_id": "test_user_2", "display_name": "テスト次郎", "badminton_experience": "中級"},
        {"user_id": "test_user_3", "display_name": "テスト三郎", "badminton_experience": "上級"},
        {"user_id": "test_user_4", "display_name": "テスト四郎", "badminton_experience": "初級"},
        {"user_id": "test_user_5", "display_name": "テスト五郎", "badminton_experience": "中級"},
        {"user_id": "test_user_6", "display_name": "テスト六郎", "badminton_experience": "上級"}
    ]
    
    # 現在の日時
    now = datetime.now().isoformat()
    
    # テストデータをテーブルに挿入
    for i, user in enumerate(test_users):
        entry_id = f"test_entry_{i+1}"
        item = {
            "entry_id": entry_id,
            "user_id": user["user_id"],
            "match_id": "pending",
            "display_name": user["display_name"],
            "badminton_experience": user["badminton_experience"],
            "joined_at": now
        }
        
        try:
            table.put_item(Item=item)
            print(f"テストデータを追加しました: {entry_id}")
        except Exception as e:
            print(f"テストデータ追加中にエラーが発生しました: {str(e)}")

if __name__ == "__main__":
    print("match_entriesテーブルを再作成します...")
    if recreate_match_table():
        print("テストデータを作成します...")
        create_test_data()
    print("完了しました")
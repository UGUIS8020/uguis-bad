import boto3

# DynamoDB リソース初期化（東京リージョン）
dynamodb = boto3.client('dynamodb', region_name='ap-northeast-1')

# テーブル作成
def create_match_entries_table():
    try:
        response = dynamodb.create_table(
            TableName='match_entries',
            KeySchema=[
                {
                    'AttributeName': 'match_id',
                    'KeyType': 'HASH'  # パーティションキー
                },
                {
                    'AttributeName': 'user_id',
                    'KeyType': 'RANGE'  # ソートキー
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'match_id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'user_id',
                    'AttributeType': 'S'
                }
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        print("✅ match_entries テーブル作成開始:", response['TableDescription']['TableName'])
    except dynamodb.exceptions.ResourceInUseException:
        print("⚠️ テーブル 'match_entries' はすでに存在します。")
    except Exception as e:
        print("❌ エラー:", e)

# 実行
create_match_entries_table()
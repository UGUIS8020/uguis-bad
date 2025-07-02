import boto3

# DynamoDBリソースの取得（リージョンは必要に応じて変更）
dynamodb = boto3.client('dynamodb', region_name='ap-northeast-1')

def create_bad_game_matches_table():
    try:
        response = dynamodb.create_table(
            TableName='bad-game-matches',
            KeySchema=[
                {
                    'AttributeName': 'match_id',
                    'KeyType': 'HASH'  # パーティションキー
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'match_id',
                    'AttributeType': 'S'
                }
            ],
            BillingMode='PAY_PER_REQUEST',  # オンデマンド課金
        )
        print("✅ テーブル作成開始:", response['TableDescription']['TableName'])
    except dynamodb.exceptions.ResourceInUseException:
        print("⚠️ すでにテーブルは存在しています。")
    except Exception as e:
        print("❌ エラー:", str(e))

if __name__ == '__main__':
    create_bad_game_matches_table()
import boto3

dynamodb = boto3.client('dynamodb', region_name='ap-northeast-1')  # リージョンは適宜修正

table_name = 'bad-users-history'

try:
    response = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {'AttributeName': 'user_id', 'KeyType': 'HASH'},   # パーティションキー
            {'AttributeName': 'joined_at', 'KeyType': 'RANGE'} # ソートキー
        ],
        AttributeDefinitions=[
            {'AttributeName': 'user_id', 'AttributeType': 'S'},
            {'AttributeName': 'joined_at', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    print(f"✅ テーブル {table_name} が作成されました")
except dynamodb.exceptions.ResourceInUseException:
    print(f"⚠️ テーブル {table_name} は既に存在します")
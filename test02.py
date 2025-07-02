import boto3

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')

table_name = "bad-game-results"

try:
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'result_id',
                'KeyType': 'HASH'  # Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'result_id',
                'AttributeType': 'S'
            }
        ],
        BillingMode='PAY_PER_REQUEST',  # オートスケーリング
    )

    print("Creating table... wait until ACTIVE")
    table.wait_until_exists()
    print(f"✅ Table '{table_name}' created successfully.")

except Exception as e:
    print(f"❌ Error creating table: {e}")
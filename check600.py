import boto3

REGION = "ap-northeast-1"
TABLE_NAME = "bad-game-match_entries_v2"

dynamodb = boto3.client("dynamodb", region_name=REGION)

def create_table():
    dynamodb.create_table(
        TableName=TABLE_NAME,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsi1pk", "AttributeType": "S"},
            {"AttributeName": "gsi1sk", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            # ユーザー別に探したい場合のため（任意だけど便利）
            {
                "IndexName": "GSI_User",
                "KeySchema": [
                    {"AttributeName": "gsi1pk", "KeyType": "HASH"},  # user#<user_id>
                    {"AttributeName": "gsi1sk", "KeyType": "RANGE"}, # updated#<timestamp>#entry#<id>
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )

if __name__ == "__main__":
    create_table()
    print("OK")

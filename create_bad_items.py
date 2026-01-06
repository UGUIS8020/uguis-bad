import os, time
from dotenv import load_dotenv
from botocore.exceptions import ClientError
import boto3

load_dotenv()  # ← .env を読む（AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION など）

TABLE = os.getenv("BAD_TABLE_NAME", "bad_items")
REGION = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION", "ap-northeast-1")

# 明示的にセッションを作成（環境変数から取得）
session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=REGION,
)

dynamodb = session.client("dynamodb")
sts = session.client("sts")

def ensure_table():
    # 既存確認
    try:
        desc = dynamodb.describe_table(TableName=TABLE)["Table"]
        print(f"{TABLE} already exists. Status:", desc["TableStatus"])
        return
    except dynamodb.exceptions.ResourceNotFoundException:
        pass

    print("Creating table:", TABLE)
    dynamodb.create_table(
        TableName=TABLE,
        AttributeDefinitions=[
            {"AttributeName": "pk",     "AttributeType": "S"},
            {"AttributeName": "sk",     "AttributeType": "S"},
            {"AttributeName": "gsi1pk", "AttributeType": "S"},
            {"AttributeName": "gsi1sk", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
        GlobalSecondaryIndexes=[{
            "IndexName": "gsi1",
            "KeySchema": [
                {"AttributeName": "gsi1pk", "KeyType": "HASH"},
                {"AttributeName": "gsi1sk", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }],
    )

    # テーブルACTIVE待ち
    waiter = dynamodb.get_waiter("table_exists")
    waiter.wait(TableName=TABLE)
    print("Table ACTIVE")

    # GSI ACTIVE待ち
    while True:
        d = dynamodb.describe_table(TableName=TABLE)["Table"]
        g = next((x for x in d.get("GlobalSecondaryIndexes", []) if x["IndexName"] == "gsi1"), None)
        if g and g["IndexStatus"] == "ACTIVE":
            print("GSI gsi1 ACTIVE")
            break
        time.sleep(3)

if __name__ == "__main__":
    ident = sts.get_caller_identity()
    print("Using account:", ident["Account"], "region:", REGION, "table:", TABLE)
    ensure_table()
import os
import time
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

TABLE_NAME = os.getenv("POSTS_TABLE", "uguu_post")
REGION = os.getenv("AWS_REGION")

INDEX_NAME = "gsi_feed"
PK_ATTR = "feed_pk"
SK_ATTR = "feed_sk"

def main():
    dynamodb = boto3.client(
        "dynamodb",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=REGION,
    )

    # すでに存在するか確認
    desc = dynamodb.describe_table(TableName=TABLE_NAME)
    gsis = desc["Table"].get("GlobalSecondaryIndexes", []) or []
    if any(g.get("IndexName") == INDEX_NAME for g in gsis):
        print(f"[SKIP] GSI already exists: {INDEX_NAME}")
        return

    print(f"[CREATE] add GSI '{INDEX_NAME}' to table '{TABLE_NAME}'")

    try:
        dynamodb.update_table(
            TableName=TABLE_NAME,
            AttributeDefinitions=[
                {"AttributeName": PK_ATTR, "AttributeType": "S"},
                {"AttributeName": SK_ATTR, "AttributeType": "S"},
            ],
            GlobalSecondaryIndexUpdates=[
                {
                    "Create": {
                        "IndexName": INDEX_NAME,
                        "KeySchema": [
                            {"AttributeName": PK_ATTR, "KeyType": "HASH"},
                            {"AttributeName": SK_ATTR, "KeyType": "RANGE"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    }
                }
            ],
        )
    except ClientError as e:
        print("[ERROR] update_table failed")
        raise

    # ACTIVE待ち
    print("[WAIT] building index... (this can take a few minutes)")
    while True:
        desc = dynamodb.describe_table(TableName=TABLE_NAME)
        gsis = desc["Table"].get("GlobalSecondaryIndexes", []) or []
        g = next((x for x in gsis if x.get("IndexName") == INDEX_NAME), None)
        status = (g or {}).get("IndexStatus")
        print(f"  - status: {status}")
        if status == "ACTIVE":
            break
        time.sleep(10)

    print(f"[OK] GSI ACTIVE: {INDEX_NAME}")

if __name__ == "__main__":
    main()

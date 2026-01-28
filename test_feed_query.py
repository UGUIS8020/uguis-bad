import os
import boto3
from boto3.dynamodb.conditions import Key
from dotenv import load_dotenv

load_dotenv()

TABLE = os.getenv("POSTS_TABLE", "uguu_post")

dynamodb = boto3.resource(
    "dynamodb",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

tbl = dynamodb.Table(TABLE)

res = tbl.query(
    IndexName="gsi_feed",
    KeyConditionExpression=Key("feed_pk").eq("FEED"),
    ScanIndexForward=False,
    Limit=5
)

items = res.get("Items", [])
print(f"[OK] got {len(items)} items")
for it in items:
    print(it.get("post_id"), it.get("updated_at") or it.get("created_at"), it.get("content", "")[:30])

print("[NextKey]", res.get("LastEvaluatedKey"))

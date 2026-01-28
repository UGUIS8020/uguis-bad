import os
import boto3
from boto3.dynamodb.conditions import Attr
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

# METADATA だけ拾う
res = tbl.scan(
    FilterExpression=Attr("SK").begins_with("METADATA#")
)
items = res.get("Items", [])

updated = 0
for it in items:
    pk = it["PK"]
    sk = it["SK"]
    post_id = it.get("post_id") or pk.split("#", 1)[1]
    ts = it.get("updated_at") or it.get("created_at") or ""

    if not ts:
        continue

    feed_pk = "FEED"
    feed_sk = f"TS#{ts}#{post_id}"

    tbl.update_item(
        Key={"PK": pk, "SK": sk},
        UpdateExpression="SET feed_pk = :p, feed_sk = :s",
        ExpressionAttributeValues={":p": feed_pk, ":s": feed_sk},
    )
    updated += 1

print(f"[DONE] updated feed keys: {updated} items in {TABLE}")

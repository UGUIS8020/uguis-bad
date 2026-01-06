import os, boto3
from boto3.dynamodb.conditions import Attr
from dotenv import load_dotenv

load_dotenv()

dynamodb = boto3.resource(
    "dynamodb",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION") or "ap-northeast-1",
)
table = dynamodb.Table("bad_schedules")

# ここを、りささんの正しい user_id に差し替え
uid = "5e03ccca-7164-4b40-bc29-5cccbf187745"


items = []
resp = table.scan(FilterExpression=Attr("participants").contains(uid))
items += resp.get("Items", [])
while "LastEvaluatedKey" in resp:
    resp = table.scan(
        FilterExpression=Attr("participants").contains(uid),
        ExclusiveStartKey=resp["LastEvaluatedKey"]
    )
    items += resp.get("Items", [])

print(f"found {len(items)} schedules for {uid}")
for it in items:
    print(it.get("date"), it.get("schedule_id"))
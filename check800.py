import os
import boto3
from boto3.dynamodb.conditions import Attr

region = os.getenv("AWS_REGION", "ap-northeast-1")
table_name = "bad-users-history"

ddb = boto3.resource("dynamodb", region_name=region)
table = ddb.Table(table_name)

found = 0
scanned_total = 0
eks = None

while True:
    kwargs = dict(
        FilterExpression=Attr("paid_at").exists(),
        ProjectionExpression="user_id, joined_at, paid_at, event_date, payment_type, kind",
        Limit=200,  # 返す最大件数（一致したもの）
    )
    if eks:
        kwargs["ExclusiveStartKey"] = eks

    res = table.scan(**kwargs)

    scanned_total += res.get("ScannedCount", 0)
    items = res.get("Items", [])

    if items:
        print("FOUND:", len(items))
        for it in items[:10]:
            print(it)
        found += len(items)
        break

    eks = res.get("LastEvaluatedKey")
    print("page scanned:", res.get("ScannedCount", 0), "total scanned:", scanned_total, "found so far:", found)

    if not eks:
        print("NOT FOUND in entire table.")
        break
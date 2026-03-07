import boto3

dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")
table = dynamodb.Table("bad-users")

response = table.scan(
    ProjectionExpression="#pk, user_name",
    ExpressionAttributeNames={"#pk": "user#user_id"}
)
items = response.get("Items", [])

while "LastEvaluatedKey" in response:
    response = table.scan(
        ProjectionExpression="#pk, user_name",
        ExpressionAttributeNames={"#pk": "user#user_id"},
        ExclusiveStartKey=response["LastEvaluatedKey"]
    )
    items.extend(response.get("Items", []))

# テストユーザー（user_nameが"テスト_"で始まる）のみ削除
deleted = 0
skipped = 0
for item in items:
    pk = item.get("user#user_id", "")
    name = item.get("user_name", "")
    
    if not pk.startswith("user#"):
        continue
    
    if str(name).startswith("テスト_"):
        table.delete_item(Key={"user#user_id": pk})
        deleted += 1
        print(f"削除: {pk} | {name}")
    else:
        skipped += 1
        print(f"スキップ: {pk} | {name}")

print(f"\n削除={deleted} スキップ={skipped}")
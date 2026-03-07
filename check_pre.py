import boto3

dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")
table = dynamodb.Table("bad-users")

response = table.scan(
    ProjectionExpression="#pk, user_name, skill_score",
    ExpressionAttributeNames={"#pk": "user#user_id"}
)
items = response.get("Items", [])

while "LastEvaluatedKey" in response:
    response = table.scan(
        ProjectionExpression="#pk, user_name, skill_score",
        ExpressionAttributeNames={"#pk": "user#user_id"},
        ExclusiveStartKey=response["LastEvaluatedKey"]
    )
    items.extend(response.get("Items", []))

with_prefix = [i for i in items if str(i.get("user#user_id", "")).startswith("user#")]
without_prefix = [i for i in items if not str(i.get("user#user_id", "")).startswith("user#")]

print(f"\n=== user# プレフィックスあり: {len(with_prefix)}件 ===")
for i in with_prefix:
    print(f"  {i.get('user#user_id')} | {i.get('user_name', '?')} | skill={i.get('skill_score', '?')}")

print(f"\n=== プレフィックスなし: {len(without_prefix)}件 ===")
for i in without_prefix:
    print(f"  {i.get('user#user_id')} | {i.get('user_name', '?')} | skill={i.get('skill_score', '?')}")
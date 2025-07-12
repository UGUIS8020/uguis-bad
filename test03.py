import boto3

# DynamoDB初期化
dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")
table = dynamodb.Table("bad-users-history")

# 全件取得
response = table.scan()
items = response.get("Items", [])

# 各アイテムに対して更新処理
for item in items:
    user_id = item["user_id"]
    joined_at = item["joined_at"]

    update_expression = []
    expression_values = {}

    # 存在しないフィールドだけ追加
    if "match_count" not in item:
        update_expression.append("match_count = :mc")
        expression_values[":mc"] = 0

    if "rest_count" not in item:
        update_expression.append("rest_count = :rc")
        expression_values[":rc"] = 0

    # 更新が必要な場合だけ更新処理を実行
    if update_expression:
        try:
            table.update_item(
                Key={
                    "user_id": user_id,
                    "joined_at": joined_at
                },
                UpdateExpression="SET " + ", ".join(update_expression),
                ExpressionAttributeValues=expression_values
            )
            print(f"✅ Updated: {user_id} | {joined_at}")
        except Exception as e:
            print(f"❌ Error updating {user_id} | {joined_at} : {e}")

print("🎉 全ての処理が完了しました")
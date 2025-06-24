import boto3
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")
table = dynamodb.Table("match_entries")

# スキャンして court を持つ全エントリ取得
response = table.scan(
    FilterExpression=Attr("court").exists()
)

items = response.get("Items", [])
updated_count = 0

print(f"🔍 対象件数: {len(items)} 件")

for item in items:
    entry_id = item["entry_id"]
    court_value = item.get("court")

    # case 1: "なし" や None → 削除
    if court_value in ["なし", "None", None]:
        table.update_item(
            Key={"entry_id": entry_id},
            UpdateExpression="REMOVE court"
        )
        print(f"🗑 court 削除: {entry_id}")
        updated_count += 1

    # case 2: "1" や "2" → 数値に変換
    elif isinstance(court_value, str) and court_value.isdigit():
        table.update_item(
            Key={"entry_id": entry_id},
            UpdateExpression="SET court = :court",
            ExpressionAttributeValues={":court": int(court_value)}
        )
        print(f"🔁 court 数値化: {entry_id} → {court_value}")
        updated_count += 1

print(f"✅ 更新完了: {updated_count} 件")
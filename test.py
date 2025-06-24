import boto3
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")
table = dynamodb.Table("match_entries")

def is_digit_string(value):
    return isinstance(value, str) and value.isdigit()

def convert_court_str_to_int():
    print("🔍 数字文字列の court を整数に変換中...")
    response = table.scan()
    items = response.get("Items", [])
    updated = 0

    for item in items:
        court = item.get("court")
        entry_id = item["entry_id"]

        if is_digit_string(court):
            court_int = int(court)
            table.update_item(
                Key={"entry_id": entry_id},
                UpdateExpression="SET court = :c",
                ExpressionAttributeValues={":c": court_int}
            )
            print(f"🔄 更新: entry_id={entry_id}, court='{court}' → {court_int}")
            updated += 1

    print(f"✅ 更新完了: {updated} 件の court を整数に変換しました。")

if __name__ == "__main__":
    convert_court_str_to_int()
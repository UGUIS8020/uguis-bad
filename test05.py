import boto3
from decimal import Decimal

# DynamoDB初期化
dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")
table = dynamodb.Table("bad-game-match_entries")

print("🧪 正しいentry_idでの休憩カウントテスト開始...")

# 全エントリーを取得して、佐藤花子さんの正しいentry_idを見つける
response = table.scan()
items = response.get("Items", [])

sato_entry = None
for item in items:
    if item.get("display_name") == "佐藤花子":
        sato_entry = item
        break

if sato_entry:
    test_entry_id = sato_entry["entry_id"]
    print(f"📋 佐藤花子さんのentry_id: {test_entry_id}")
    
    # 現在の値を確認
    current_rest_count = sato_entry.get("rest_count", 0)
    print(f"📊 現在のrest_count: {current_rest_count}")
    
    # increment_rest_count関数をシミュレート
    try:
        update_response = table.update_item(
            Key={"entry_id": test_entry_id},
            UpdateExpression="SET rest_count = if_not_exists(rest_count, :zero) + :inc",
            ExpressionAttributeValues={":inc": 1, ":zero": 0},
            ReturnValues="ALL_NEW"
        )
        new_rest_count = update_response["Attributes"].get("rest_count")
        print(f"✅ 更新後のrest_count: {new_rest_count}")
        print(f"🎉 カウントアップ成功！ {current_rest_count} → {new_rest_count}")
        
        # 確認のために再度取得
        verify_response = table.get_item(Key={"entry_id": test_entry_id})
        if "Item" in verify_response:
            verified_count = verify_response["Item"].get("rest_count", 0)
            print(f"🔍 検証結果: rest_count = {verified_count}")
        
    except Exception as e:
        print(f"❌ 更新失敗: {e}")
        
else:
    print("❌ 佐藤花子さんのエントリーが見つかりません")

print("\n" + "="*50)
print("🔍 テスト後の全エントリー状態:")
response = table.scan()
items = response.get("Items", [])

for item in items:
    name = item.get("display_name", "Unknown")
    match_count = item.get("match_count", 0)
    rest_count = item.get("rest_count", 0)
    print(f"  {name}: match_count={match_count}, rest_count={rest_count}")
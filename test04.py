import boto3
from decimal import Decimal

# DynamoDB初期化
dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")
table = dynamodb.Table("bad-game-match_entries")

print("🔄 フィールドを強制的に更新中...")

# 全件取得
response = table.scan()
items = response.get("Items", [])

updated_count = 0
error_count = 0

for item in items:
    entry_id = item["entry_id"]
    display_name = item.get("display_name", "Unknown")
    
    try:
        # 既存の値を取得（存在しない場合は0）
        current_match_count = item.get("match_count", 0)
        current_rest_count = item.get("rest_count", 0)
        
        # 強制的に更新（既存値を保持）
        table.update_item(
            Key={"entry_id": entry_id},
            UpdateExpression="SET match_count = :mc, rest_count = :rc",
            ExpressionAttributeValues={
                ":mc": Decimal(str(current_match_count)),
                ":rc": Decimal(str(current_rest_count))
            },
            ReturnValues="ALL_NEW"
        )
        
        print(f"✅ 更新完了: {display_name} (match_count: {current_match_count}, rest_count: {current_rest_count})")
        updated_count += 1
        
    except Exception as e:
        print(f"❌ 更新失敗: {display_name} - {e}")
        error_count += 1

print(f"\n🎉 処理完了: 成功 {updated_count}件, 失敗 {error_count}件")

# 更新後の確認
print("\n🔍 更新後の確認...")
response = table.scan()
items = response.get("Items", [])

for item in items[:3]:  # 最初の3件を確認
    print(f"  {item.get('display_name')}: match_count={item.get('match_count')}, rest_count={item.get('rest_count')}")
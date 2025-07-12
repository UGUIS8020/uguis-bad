import boto3
from boto3.dynamodb.conditions import Attr

# DynamoDB初期化
dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")
match_table = dynamodb.Table("bad-game-match_entries")

print("🔍 現在の状況を詳細デバッグ")
print("=" * 60)

# 全エントリーの状況を確認
response = match_table.scan()
items = response.get("Items", [])

print(f"📊 総エントリー数: {len(items)}")
print()

# ステータス別に分類
pending_count = 0
playing_count = 0
other_count = 0

for item in items:
    name = item.get("display_name", "Unknown")
    entry_status = item.get("entry_status", "unknown")
    match_id = item.get("match_id", "none")
    rest_count = item.get("rest_count", 0)
    
    print(f"👤 {name}:")
    print(f"   entry_status: {entry_status}")
    print(f"   match_id: {match_id}")
    print(f"   rest_count: {rest_count}")
    print()
    
    if entry_status == "pending":
        pending_count += 1
    elif entry_status == "playing":
        playing_count += 1
    else:
        other_count += 1

print("=" * 60)
print(f"📋 参加待ち (pending): {pending_count}人")
print(f"🎮 試合中 (playing): {playing_count}人")
print(f"❓ その他: {other_count}人")

print("\n" + "=" * 60)
print("🔍 get_players_status関数のフィルター条件をテスト")

# get_players_status関数と同じ条件でスキャン
filter_expr = Attr('entry_status').eq('pending') & Attr('match_id').eq('pending')
filtered_response = match_table.scan(FilterExpression=filter_expr)
filtered_items = filtered_response.get('Items', [])

print(f"📊 フィルター結果: {len(filtered_items)}人")
for item in filtered_items:
    name = item.get("display_name", "Unknown")
    rest_count = item.get("rest_count", 0)
    print(f"  {name} (rest_count: {rest_count})")
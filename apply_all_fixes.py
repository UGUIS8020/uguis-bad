# apply_all_fixes.py
import sys
sys.path.insert(0, '.')

from uguu.dynamo import DynamoDB
import json

db = DynamoDB()

# fix_data.json を読み込み
with open('fix_data.json', 'r', encoding='utf-8') as f:
    fix_data = json.load(f)

print(f"=== 全{len(fix_data)}件を修正 ===\n")

success_count = 0
error_count = 0

for i, item in enumerate(fix_data, 1):
    record = {
        "user_id": item["user_id"],
        "joined_at": item["joined_at"],
        "schedule_id": item["schedule_id"],
        "date": item["date"],
        "location": item["location"],
        "status": "registered",
        "action": "join",
    }
    
    try:
        db.part_history.put_item(Item=record)
        print(f"{i:2}. ✓ {item['date']} - {item['user_id'][:8]}...")
        success_count += 1
    except Exception as e:
        print(f"{i:2}. ✗ {item['date']} - {item['user_id'][:8]}... : {e}")
        error_count += 1

print(f"\n成功: {success_count}件")
print(f"失敗: {error_count}件")
print("\n[DONE]")
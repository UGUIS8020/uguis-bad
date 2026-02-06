# test_fix_2_records.py
import sys
sys.path.insert(0, '.')

from uguu.dynamo import DynamoDB
from boto3.dynamodb.conditions import Key

db = DynamoDB()

# テスト対象の2人
TEST_USERS = [
    {
        "user_id": "eac198b8-eac4-49fd-bf77-8be0be12a028",  # さくらこさん
        "date": "2025-09-22",
        "schedule_id": "0a642a15-835f-4eff-bba4-41bdea9e3a00",
        "joined_at": "2025-09-22T12:00:00+00:00",
    },
    {
        "user_id": "2ed84651-6862-4a3f-a8c2-04134fcc8455",  # 2ed84651さん
        "date": "2025-09-22",
        "schedule_id": "0a642a15-835f-4eff-bba4-41bdea9e3a00",
        "joined_at": "2025-09-22T12:00:00+00:00",
    },
]

print("=== テスト修正: 2件 ===\n")

for user in TEST_USERS:
    print(f"[{user['user_id'][:8]}...] {user['date']}")
    
    # 修正前の状態を確認
    print("  修正前:")
    resp = db.part_history.query(
        KeyConditionExpression=Key('user_id').eq(user['user_id']),
        ScanIndexForward=True
    )
    date_records = [r for r in resp.get('Items', []) if r.get('date') == user['date']]
    for r in date_records:
        print(f"    status={r.get('status')}, action={r.get('action')}, joined_at={r.get('joined_at')[:19]}")
    
    # 正式参加レコードを追加
    item = {
        "user_id": user["user_id"],
        "joined_at": user["joined_at"],
        "schedule_id": user["schedule_id"],
        "date": user["date"],
        "location": "越谷市立地域スポーツセンターA面(3面)",
        "status": "registered",
        "action": "join",
    }
    
    try:
        db.part_history.put_item(Item=item)
        print("  ✓ 正式参加レコード追加成功")
    except Exception as e:
        print(f"  ✗ 追加失敗: {e}")
    
    # 修正後の状態を確認
    print("  修正後:")
    resp = db.part_history.query(
        KeyConditionExpression=Key('user_id').eq(user['user_id']),
        ScanIndexForward=True
    )
    date_records = [r for r in resp.get('Items', []) if r.get('date') == user['date']]
    for r in date_records:
        print(f"    status={r.get('status')}, action={r.get('action')}, joined_at={r.get('joined_at')[:19]}")
    
    print()

# 修正後の関数の動作を確認
print("=== 関数の動作確認 ===\n")

for user in TEST_USERS:
    print(f"[{user['user_id'][:8]}...] {user['date']}")
    
    # get_user_participation_history()
    history = db.get_user_participation_history(user['user_id'])
    count1 = len([d for d in history if (d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else d) == user['date']])
    print(f"  get_user_participation_history(): {user['date']} = {count1}件")
    
    # get_user_participation_history_with_timestamp()
    history_ts = db.get_user_participation_history_with_timestamp(user['user_id'])
    count2 = len([r for r in history_ts if r.get('event_date') == user['date']])
    print(f"  get_user_participation_history_with_timestamp(): {user['date']} = {count2}件")
    
    print()

print("[DONE]")
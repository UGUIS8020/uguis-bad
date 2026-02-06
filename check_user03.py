# check_0922_all_users.py
import sys
sys.path.insert(0, '.')

from uguu.dynamo import DynamoDB
from boto3.dynamodb.conditions import Attr
from collections import defaultdict

db = DynamoDB()
DATE = "2025-09-22"

print(f"[INFO] {DATE} の全データを調査\n")

# 1. スケジュールテーブルから参加者リストを取得
print("=== スケジュールテーブル ===")
resp = db.schedule_table.scan(FilterExpression=Attr('date').eq(DATE))
schedules = resp.get('Items', [])

if schedules:
    schedule = schedules[0]
    participants = schedule.get('participants', [])
    print(f"schedule_id: {schedule.get('schedule_id')}")
    print(f"参加者数: {len(participants)}")
    print(f"参加者: {participants[:5]}... (最初の5人)\n")
else:
    print("スケジュールが見つかりません\n")
    participants = []

# 2. bad-users-history から9月22日の全レコードを取得
print("=== bad-users-history の全レコード ===")

# 全ユーザーの9月22日のレコードを取得（スキャン）
resp = db.part_history.scan(
    FilterExpression=Attr('date').eq(DATE)
)
history_records = resp.get('Items', [])

while resp.get('LastEvaluatedKey'):
    resp = db.part_history.scan(
        FilterExpression=Attr('date').eq(DATE),
        ExclusiveStartKey=resp['LastEvaluatedKey']
    )
    history_records.extend(resp.get('Items', []))

print(f"総レコード数: {len(history_records)}\n")

# ユーザーごとにグループ化
user_records = defaultdict(list)
for rec in history_records:
    user_id = rec.get('user_id')
    if user_id:
        user_records[user_id].append(rec)

print(f"ユーザー数: {len(user_records)}\n")

# 3. 不整合チェック
print("=== 不整合チェック ===\n")

# スケジュールにいるがhistoryにない
missing_in_history = []
for user_id in participants:
    if user_id not in user_records:
        missing_in_history.append(user_id)
    else:
        # historyにあるが、正式参加レコードがない
        has_official = False
        for rec in user_records[user_id]:
            status = (rec.get('status') or '').lower()
            action = rec.get('action') or ''
            if status not in ('cancelled', 'tentative') or (status == 'tentative' and action != 'tara_join'):
                has_official = True
                break
        
        if not has_official:
            # たら参加のみ
            print(f"⚠ たら参加のみ: {user_id[:8]}...")
            for rec in user_records[user_id]:
                print(f"    status={rec.get('status')}, action={rec.get('action')}, joined_at={rec.get('joined_at')}")

if missing_in_history:
    print(f"\n⚠ スケジュールにいるが履歴なし: {len(missing_in_history)}人")
    for user_id in missing_in_history[:5]:
        print(f"    {user_id[:8]}...")
else:
    print("\n✓ 全参加者に履歴レコードあり")

# 4. 正式参加レコードの統計
print(f"\n=== レコードタイプ統計 ===")
official_count = 0
tara_only_count = 0
cancelled_count = 0

for user_id, records in user_records.items():
    has_official = False
    has_cancelled = False
    
    for rec in records:
        status = (rec.get('status') or '').lower()
        action = rec.get('action') or ''
        
        if status == 'cancelled':
            has_cancelled = True
        elif status in ('registered', '') or (status is None and action in ('', None)):
            has_official = True
        elif status == 'tentative' and action != 'tara_join':
            has_official = True
    
    if has_cancelled:
        cancelled_count += 1
    elif has_official:
        official_count += 1
    else:
        tara_only_count += 1

print(f"正式参加: {official_count}人")
print(f"たら参加のみ: {tara_only_count}人")
print(f"キャンセル: {cancelled_count}人")

print("\n[DONE]")
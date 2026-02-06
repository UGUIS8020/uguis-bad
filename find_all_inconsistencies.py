# find_all_inconsistencies.py（修正版）
import sys
sys.path.insert(0, '.')

from uguu.dynamo import DynamoDB
from boto3.dynamodb.conditions import Attr
from collections import defaultdict
from datetime import datetime
from utils.timezone import JST

db = DynamoDB()

print("[INFO] 全スケジュールの不整合を検出\n")

# 1. 過去のスケジュールを全て取得
today = datetime.now(JST).date()
resp = db.schedule_table.scan()
all_schedules = resp.get('Items', [])

while resp.get('LastEvaluatedKey'):
    resp = db.schedule_table.scan(ExclusiveStartKey=resp['LastEvaluatedKey'])
    all_schedules.extend(resp.get('Items', []))

# 過去のスケジュールのみ
past_schedules = []
for s in all_schedules:
    try:
        date_obj = datetime.strptime(s['date'], "%Y-%m-%d").date()
        if date_obj <= today:
            past_schedules.append(s)
    except:
        continue

print(f"過去のスケジュール: {len(past_schedules)}件\n")

# 2. bad-users-history の全レコードを取得
print("bad-users-history を読み込み中...")
resp = db.part_history.scan()
history_records = resp.get('Items', [])

while resp.get('LastEvaluatedKey'):
    resp = db.part_history.scan(ExclusiveStartKey=resp['LastEvaluatedKey'])
    history_records.extend(resp.get('Items', []))

# 日付とユーザーごとにグループ化
user_date_records = defaultdict(list)
for rec in history_records:
    user_id = rec.get('user_id')
    date = rec.get('date') or rec.get('event_date')
    if user_id and date:
        user_date_records[(user_id, date)].append(rec)

print(f"履歴レコード: {len(history_records)}件\n")

# 3. 不整合を検出
missing_records = []

for schedule in past_schedules:
    date = schedule.get('date')
    schedule_id = schedule.get('schedule_id')
    participants = schedule.get('participants', [])
    
    for user_id in participants:
        if not isinstance(user_id, str):
            continue
        
        records = user_date_records.get((user_id, date), [])
        
        # 正式参加レコードがあるかチェック
        has_official = False
        for rec in records:
            status = (rec.get('status') or '').lower()
            action = rec.get('action') or ''
            
            # 正式参加の条件
            if status == 'registered':
                has_official = True
                break
            if status not in ('cancelled', 'tentative'):
                has_official = True
                break
            if status == 'tentative' and action != 'tara_join':
                has_official = True
                break
        
        # 正式参加レコードがない場合
        if not has_official:
            # ユーザー情報を取得
            user = db.get_user_by_id(user_id)
            username = user.get('username') if user else None
            
            missing_records.append({
                'user_id': user_id,
                'username': username or 'Unknown',  # ★修正
                'date': date,
                'schedule_id': schedule_id,
                'location': schedule.get('venue', '') + schedule.get('court', ''),
                'has_tara_record': len(records) > 0,
                'records': records
            })

print(f"=== 不整合データ: {len(missing_records)}件 ===\n")

# 日付ごとにグループ化して表示
by_date = defaultdict(list)
for item in missing_records:
    by_date[item['date']].append(item)

for date in sorted(by_date.keys()):
    items = by_date[date]
    print(f"\n{date} ({len(items)}件)")
    for item in items:
        tara_str = "たら参加あり" if item['has_tara_record'] else "レコードなし"
        username_display = item['username'][:10] if item['username'] != 'Unknown' else 'Unknown'
        print(f"  - {username_display:10} ({item['user_id'][:8]}...) [{tara_str}]")

# 4. 修正用のデータを生成
print(f"\n\n=== 修正データを生成 ===\n")

fix_data = []
for item in missing_records:
    # 推定時刻（たら参加がある場合はその後、ない場合は12:00）
    if item['has_tara_record']:
        # たら参加の時刻を取得
        tara_time = item['records'][0].get('joined_at', '')
        if tara_time:
            # たら参加の1時間後を推定
            try:
                from datetime import timedelta
                dt = datetime.fromisoformat(tara_time.replace('Z', '+00:00'))
                dt = dt + timedelta(hours=1)
                joined_at = dt.isoformat()
            except:
                joined_at = f"{item['date']}T12:00:00+00:00"
        else:
            joined_at = f"{item['date']}T12:00:00+00:00"
    else:
        joined_at = f"{item['date']}T12:00:00+00:00"
    
    fix_data.append({
        'user_id': item['user_id'],
        'username': item['username'],
        'joined_at': joined_at,
        'schedule_id': item['schedule_id'],
        'date': item['date'],
        'location': item['location'],
        'status': 'registered',
        'action': 'join',
    })

# 5. JSONファイルに保存
import json
with open('fix_data.json', 'w', encoding='utf-8') as f:
    json.dump(fix_data, f, ensure_ascii=False, indent=2)

print(f"修正データを fix_data.json に保存しました ({len(fix_data)}件)")
print("\n[DONE]")
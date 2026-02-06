# check_schedule_0922_detail.py
import sys
sys.path.insert(0, '.')

from uguu.dynamo import DynamoDB
from boto3.dynamodb.conditions import Attr
import json
from decimal import Decimal

def json_default(o):
    if isinstance(o, Decimal):
        return int(o) if o % 1 == 0 else float(o)
    return str(o)

db = DynamoDB()
DATE = "2025-09-22"
SAKURAKO_ID = "eac198b8-eac4-49fd-bf77-8be0be12a028"
USER_2_ID = "2ed84651-6862-4a3f-a8c2-04134fcc8455"

print(f"[INFO] {DATE} のスケジュール詳細\n")

resp = db.schedule_table.scan(FilterExpression=Attr('date').eq(DATE))
schedules = resp.get('Items', [])

if schedules:
    schedule = schedules[0]
    
    print("=== スケジュール情報 ===")
    print(f"schedule_id: {schedule.get('schedule_id')}")
    print(f"date: {schedule.get('date')}")
    print(f"created_at: {schedule.get('created_at')}")
    print(f"updated_at: {schedule.get('updated_at')}")
    
    participants = schedule.get('participants', [])
    tara_participants = schedule.get('tara_participants', [])
    
    print(f"\nparticipants: {len(participants)}人")
    print(f"tara_participants: {len(tara_participants)}人")
    
    print(f"\nさくらこさん in participants: {SAKURAKO_ID in participants}")
    print(f"さくらこさん in tara_participants: {SAKURAKO_ID in tara_participants}")
    
    print(f"\n2ed84651さん in participants: {USER_2_ID in participants}")
    print(f"2ed84651さん in tara_participants: {USER_2_ID in tara_participants}")
    
    # tara_participants の全リスト
    if tara_participants:
        print(f"\n=== tara_participants 全リスト ===")
        for i, user_id in enumerate(tara_participants, 1):
            print(f"{i}. {user_id[:8]}...")
    
    # 全フィールドを表示
    print(f"\n=== スケジュール全フィールド ===")
    print(json.dumps(schedule, ensure_ascii=False, indent=2, default=json_default))

print("\n[DONE]")
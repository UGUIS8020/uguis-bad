# check_oi_schedules.py
import sys
sys.path.insert(0, '.')

from uguu.dynamo import DynamoDB
from boto3.dynamodb.conditions import Attr

db = DynamoDB()
USER_ID = "380c2e1a-8906-40ab-bf4d-a091704ec257"  # オイさん

# たら参加のみの日付
tara_only_dates = [
    "2025-11-08", "2025-11-13", "2025-11-17", "2025-11-19",
    "2025-11-27", "2025-12-03", "2025-12-08", "2025-12-13",
    "2025-12-15", "2025-12-22", "2025-12-25", "2026-01-12"
]

print(f"[INFO] たら参加のみの日付がスケジュールに登録されているか確認\n")

for date in tara_only_dates:
    resp = db.schedule_table.scan(
        FilterExpression=Attr('date').eq(date)
    )
    
    schedules = resp.get('Items', [])
    if schedules:
        schedule = schedules[0]
        participants = schedule.get('participants', [])
        is_in = USER_ID in participants
        
        print(f"{date}: スケジュールあり, 参加者数={len(participants)}, オイさん={'✓' if is_in else '✗'}")
    else:
        print(f"{date}: スケジュールなし")

print("\n[DONE]")
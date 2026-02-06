# check_sakurako_simple.py
import sys
sys.path.insert(0, '.')

from uguu.dynamo import DynamoDB
db = DynamoDB()

USER_ID = "1d5b135f-368e-4917-81ee-44dbe7d6dac8"

print(f"[INFO] Checking user: {USER_ID}\n")

# 1. timestamp付きバージョン
print("=== get_user_participation_history_with_timestamp ===")
raw_history_ts = db.get_user_participation_history_with_timestamp(USER_ID)
print(f"Total: {len(raw_history_ts)}\n")

for i, record in enumerate(raw_history_ts, 1):
    print(f"Record {i}:")
    print(f"  event_date: {record.get('event_date')}")
    print(f"  status: {record.get('status')}")
    print(f"  action: {record.get('action')}")
    print()

# 2. timestamp無しバージョン（こちらがraw=3を検出している？）
print("=== get_user_participation_history ===")
raw_history = db.get_user_participation_history(USER_ID)
print(f"Total: {len(raw_history)}\n")

for i, record in enumerate(raw_history, 1):
    print(f"Record {i}:")
    print(f"  Type: {type(record)}")
    if isinstance(record, dict):
        print(f"  event_date: {record.get('event_date')}")
        print(f"  status: {record.get('status')}")
        print(f"  action: {record.get('action')}")
    else:
        print(f"  Value: {record}")
    print()

print("[DONE]")
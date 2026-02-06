# check_code_history.py
import sys
sys.path.insert(0, '.')

from uguu.dynamo import DynamoDB
from boto3.dynamodb.conditions import Attr

db = DynamoDB()

# 9月22日の参加者16人の履歴を全て確認
PARTICIPANTS = [
    "1e14658d-0f86-4df3-9b33-f9b5ccc9a2aa",
    "2212429d-713d-4b6a-8ae1-c47a1acef892",
    "2ed84651-6862-4a3f-a8c2-04134fcc8455",  # 問題あり
    "52b9d36e-1413-49c3-8362-9130016df2d4",
    "5819b184-240a-4751-8d1f-498db7fafa27",
    "274815ec-14f4-48d2-8c9d-5dd8e10c22cf",
    "3c24a5a8-8a95-43b9-8b72-960852dee934",
    "cd6d2feb-0281-4f5e-ab95-7248c97c65ce",
    "c611d9f5-beac-4f7d-bd52-7b4dcff54cfb",
    "380c2e1a-8906-40ab-bf4d-a091704ec257",  # オイさん
    "edd6f5a7-dd02-461a-8fe4-a3a74dd236ff",
    "965f9272-9625-4748-bbee-49dc78f4287e",
    "4c7f822d-ff39-4797-9b7b-8ebc205490f5",
    "eac198b8-eac4-49fd-bf77-8be0be12a028",  # さくらこさん - 問題あり
    "6c7dcf34-effe-41b8-b4a8-8b60ecb16bd0",
    "4d88d14d-81d7-44ee-9d6b-d0bd3e432091"
]

DATE = "2025-09-22"

print(f"[INFO] {DATE} の全16人の履歴レコードを確認\n")

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

# ユーザーごとにグループ化
from collections import defaultdict
user_records = defaultdict(list)
for rec in history_records:
    user_id = rec.get('user_id')
    if user_id:
        user_records[user_id].append(rec)

# 16人全員をチェック
for i, user_id in enumerate(PARTICIPANTS, 1):
    records = user_records.get(user_id, [])
    
    # 正式参加レコードがあるか
    has_official = False
    for rec in records:
        status = (rec.get('status') or '').lower()
        action = rec.get('action') or ''
        
        if status == 'registered':
            has_official = True
            break
        if status not in ('cancelled', 'tentative'):
            has_official = True
            break
        if status == 'tentative' and action != 'tara_join':
            has_official = True
            break
    
    status_str = "✓ 正式参加" if has_official else "✗ たら参加のみ"
    
    print(f"{i:2}. {user_id[:8]}... {status_str} (レコード数: {len(records)})")
    
    # たら参加のみの場合は詳細表示
    if not has_official and records:
        for rec in records:
            print(f"      status={rec.get('status')}, action={rec.get('action')}, joined_at={rec.get('joined_at')[:19]}")

print("\n[DONE]")
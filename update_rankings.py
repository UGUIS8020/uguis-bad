"""
アクティブユーザーのランキングを一括更新するスクリプト
ローカルで実行: python update_rankings.py
"""

import boto3
from boto3.dynamodb.conditions import Attr
from datetime import date, timedelta
from decimal import Decimal
from dotenv import load_dotenv
import os

load_dotenv()

dynamodb = boto3.resource(
    'dynamodb',
    region_name=os.getenv('AWS_REGION', 'ap-northeast-1'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
)

today = date.today()
cutoff = (today - timedelta(days=90)).strftime("%Y-%m-%d")
print(f"対象期間: {cutoff} 〜 {today}")

# 1. bad-users-history から直近90日の参加回数を集計
print("参加履歴を集計中...")
history_table = dynamodb.Table("bad-users-history")
user_count = {}
kwargs = {"FilterExpression": Attr("date").gte(cutoff)}
while True:
    resp = history_table.scan(**kwargs)
    for item in resp.get("Items", []):
        st = str(item.get("status", "")).lower()
        if st.startswith("cancel"):
            continue
        uid = item.get("user_id", "")
        if uid:
            user_count[uid] = user_count.get(uid, 0) + 1
    last = resp.get("LastEvaluatedKey")
    if not last:
        break
    kwargs["ExclusiveStartKey"] = last

# アクティブ判定（2回以上）
active_uids = {uid for uid, cnt in user_count.items() if cnt >= 2}
print(f"アクティブユーザー: {len(active_uids)} 人（直近90日に2回以上参加）")

# 2. bad-users から skill_score を持つユーザーを取得
print("ユーザー情報を取得中...")
user_table = dynamodb.Table("bad-users")
all_users = []
kwargs2 = {"FilterExpression": Attr("skill_score").exists()}
while True:
    resp = user_table.scan(**kwargs2)
    all_users.extend(resp.get("Items", []))
    last = resp.get("LastEvaluatedKey")
    if not last:
        break
    kwargs2["ExclusiveStartKey"] = last

print(f"skill_score 保持ユーザー: {len(all_users)} 人")

# アクティブユーザーのみ抽出してスコア降順ソート
active_users = [
    u for u in all_users
    if u.get("user#user_id") in active_uids
    and not str(u.get("user#user_id", "")).startswith("test_user_")
]
active_users.sort(key=lambda u: float(u.get("skill_score", 0)), reverse=True)
total = len(active_users)
print(f"ランキング対象: {total} 人")

# 3. 各ユーザーの rank を書き込む
print("ランキングを書き込み中...")
for rank, u in enumerate(active_users, 1):
    uid = u.get("user#user_id")
    name = u.get("display_name", uid)
    score = float(u.get("skill_score", 0))
    try:
        user_table.update_item(
            Key={"user#user_id": uid},
            UpdateExpression="SET #r = :r, active_rank_total = :t",
            ExpressionAttributeNames={"#r": "rank"},
            ExpressionAttributeValues={":r": Decimal(rank), ":t": Decimal(total)},
        )
        print(f"  {rank:3d}位 {name} ({score:.2f})")
    except Exception as e:
        print(f"  ❌ {name} 更新失敗: {e}")

print(f"\n✅ 完了: {total} 人のランキングを更新しました")

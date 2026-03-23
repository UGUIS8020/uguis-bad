import boto3
from decimal import Decimal

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
results_table = dynamodb.Table("bad-game-results")
users_table = dynamodb.Table("bad-users")

# 最新スナップショットを全ユーザー分集める
resp = results_table.scan()
latest_snap = {}
for r in sorted(resp["Items"], key=lambda x: x.get("match_id", "")):
    snap = r.get("skill_snapshot", {})
    for uid, vals in snap.items():
        latest_snap[uid] = vals

# bad-users と比較
mismatch = []
for uid, snap_vals in latest_snap.items():
    item = users_table.get_item(Key={"user#user_id": uid}).get("Item")
    if not item:
        mismatch.append((uid, "NOT FOUND", snap_vals.get("skill_score")))
        continue
    db_score = float(item.get("skill_score", 0))
    snap_score = float(snap_vals.get("skill_score", 0))
    if abs(db_score - snap_score) > 1.0:
        mismatch.append((uid, item.get("display_name"), db_score, snap_score))

for m in mismatch:
    print(m)
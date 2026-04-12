"""
bad-users-history の schedule_id 状況を調べるスクリプト
- 特定ユーザーの参加レコードを日付順に表示
- schedule_id の有無・重複・同日複数セッションを確認できる

使い方:
  python test.py                        # 全ユーザーのサマリー
  python test.py <display_name>         # 特定ユーザーの詳細（例: あらはる）
"""

import sys
import boto3
from collections import defaultdict
from dotenv import load_dotenv
import os

load_dotenv()

ddb = boto3.resource(
    'dynamodb',
    region_name=os.getenv('AWS_REGION', 'ap-northeast-1'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
)

history_table = ddb.Table("bad-users-history")  # type: ignore[attr-defined]
users_table   = ddb.Table("bad-users")          # type: ignore[attr-defined]

# ── ユーザー一覧取得（display_name → user_id マップ） ──────────────────
def get_user_map():
    users = {}
    resp = users_table.scan(ProjectionExpression="user_id, display_name, #uid",
                            ExpressionAttributeNames={"#uid": "user#user_id"})
    for item in resp.get("Items", []):
        uid  = item.get("user_id") or item.get("user#user_id", "")
        name = item.get("display_name", uid)
        users[uid] = name
    while resp.get("LastEvaluatedKey"):
        resp = users_table.scan(
            ProjectionExpression="user_id, display_name, #uid",
            ExpressionAttributeNames={"#uid": "user#user_id"},
            ExclusiveStartKey=resp["LastEvaluatedKey"]
        )
        for item in resp.get("Items", []):
            uid  = item.get("user_id") or item.get("user#user_id", "")
            name = item.get("display_name", uid)
            users[uid] = name
    return users

# ── 参加レコード取得（参加系のみ） ────────────────────────────────────
def get_history(user_id):
    from boto3.dynamodb.conditions import Key
    resp = history_table.query(
        KeyConditionExpression=Key("user_id").eq(user_id)
    )
    items = resp.get("Items", [])
    while resp.get("LastEvaluatedKey"):
        resp = history_table.query(
            KeyConditionExpression=Key("user_id").eq(user_id),
            ExclusiveStartKey=resp["LastEvaluatedKey"]
        )
        items.extend(resp.get("Items", []))
    # ポイントトランザクション（points#...）は除外
    return [i for i in items if not str(i.get("joined_at", "")).startswith("points#")]

# ── 1ユーザーの詳細表示 ───────────────────────────────────────────────
def show_user_detail(user_id, display_name=""):  # noqa: ARG001
    items = get_history(user_id)
    if not items:
        print(f"  レコードなし")
        return

    # 日付でグループ化
    by_date = defaultdict(list)
    for item in items:
        date = item.get("date") or item.get("event_date") or "不明"
        by_date[date].append(item)

    no_sid_count  = 0
    multi_session = 0

    print(f"\n{'日付':<12} {'件数':>4}  {'schedule_id の状況'}")
    print("-" * 70)

    for date in sorted(by_date.keys()):
        records = by_date[date]
        sids = [r.get("schedule_id") or "" for r in records]
        unique_sids = set(sids)

        has_no_sid   = "" in unique_sids
        real_sids    = unique_sids - {""}
        sid_count    = len(real_sids)

        if sid_count >= 2:
            multi_session += 1
            note = f"⚡ {sid_count}セッション別 {sorted(real_sids)}"
        elif sid_count == 1 and has_no_sid:
            no_sid_count += 1
            note = f"⚠️  schedule_idあり＋なし混在 (sid={list(real_sids)[0][:8]}...)"
        elif sid_count == 1:
            note = f"✓  schedule_id={list(real_sids)[0][:8]}..."
        else:
            no_sid_count += 1
            note = "❌ schedule_idなし（全レコード）"

        statuses = [r.get("status") or r.get("action") or "?" for r in records]
        print(f"  {date:<12} {len(records):>4}件  {note}  status={statuses}")

    print()
    print(f"  合計参加日数: {len(by_date)}")
    print(f"  schedule_idなし日: {no_sid_count}")
    print(f"  同日複数セッション日: {multi_session}")

# ── 全ユーザーサマリー ────────────────────────────────────────────────
def show_summary():
    user_map = get_user_map()
    print(f"\nユーザー数: {len(user_map)}")

    # 全履歴スキャン
    all_items = []
    resp = history_table.scan()
    all_items.extend(resp.get("Items", []))
    while resp.get("LastEvaluatedKey"):
        resp = history_table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
        all_items.extend(resp.get("Items", []))

    # ポイントトランザクション除外
    all_items = [i for i in all_items
                 if not str(i.get("joined_at", "")).startswith("points#")]

    total     = len(all_items)
    no_sid    = sum(1 for i in all_items if not (i.get("schedule_id") or ""))
    with_sid  = total - no_sid

    print(f"\n▼ bad-users-history 参加レコード合計: {total} 件")
    print(f"  schedule_idあり : {with_sid} 件 ({with_sid/total*100:.1f}%)")
    print(f"  schedule_idなし : {no_sid}  件 ({no_sid/total*100:.1f}%)")

    # ユーザーごとの schedule_idなし 件数
    by_user = defaultdict(lambda: {"no_sid": 0, "total": 0})
    for item in all_items:
        uid = item.get("user_id", "")
        by_user[uid]["total"] += 1
        if not (item.get("schedule_id") or ""):
            by_user[uid]["no_sid"] += 1

    problem_users = [(uid, d) for uid, d in by_user.items() if d["no_sid"] > 0]
    problem_users.sort(key=lambda x: -x[1]["no_sid"])

    if problem_users:
        print(f"\n▼ schedule_idなしレコードがあるユーザー ({len(problem_users)}人)")
        print(f"  {'名前':<16} {'なし':>6} {'合計':>6} {'割合':>6}")
        print("  " + "-" * 40)
        for uid, d in problem_users[:30]:
            name = user_map.get(uid, uid[:8])
            pct  = d["no_sid"] / d["total"] * 100
            print(f"  {name:<16} {d['no_sid']:>6} {d['total']:>6}  {pct:>5.1f}%")
    else:
        print("\n✅ schedule_idなしのレコードはありません")

# ── メイン ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) >= 2:
        target_name = sys.argv[1]
        user_map = get_user_map()
        # display_name で検索
        matches = [(uid, name) for uid, name in user_map.items()
                   if target_name in name]
        if not matches:
            print(f"'{target_name}' に一致するユーザーが見つかりません")
            sys.exit(1)
        for uid, name in matches:
            print(f"\n{'='*60}")
            print(f"ユーザー: {name}  (user_id: {uid})")
            show_user_detail(uid, name)
    else:
        show_summary()

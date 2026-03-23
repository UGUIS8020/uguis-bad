"""
migrate_history_to_results.py

bad-game-history の補足情報を bad-game-results に移行するスクリプト。
同じ match_id を持つ bad-game-results の各コートレコードに
pairing_mode / waiting_players / skill_snapshot を追記する。

使い方:
  python migrate_history_to_results.py          # 実行（確認なし）
  python migrate_history_to_results.py --dry    # ドライラン（書き込みなし）
"""

import boto3, json, argparse
from boto3.dynamodb.conditions import Attr
from decimal import Decimal

REGION = "ap-northeast-1"

def decimal_safe(obj):
    """DynamoDB の Decimal を float に変換"""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: decimal_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [decimal_safe(i) for i in obj]
    return obj

def fetch_all(table, **kwargs):
    items = []
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek
    return items

def main(dry_run: bool):
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    history_table = dynamodb.Table("bad-game-history")
    results_table = dynamodb.Table("bad-game-results")

    print("bad-game-history を取得中...")
    history_items = fetch_all(history_table)
    print(f"  → {len(history_items)} 件取得")

    print("bad-game-results を取得中...")
    results_items = fetch_all(results_table)
    print(f"  → {len(results_items)} 件取得")

    # bad-game-results を match_id でグループ化
    results_by_match = {}
    for r in results_items:
        mid = r["match_id"]
        results_by_match.setdefault(mid, []).append(r)

    ok = 0
    skip = 0
    not_found = 0

    for h in history_items:
        mid = h["match_id"]

        # すでに pairing_mode が入っているレコードはスキップ
        # （今日分など、すでに移行済み）
        target_results = results_by_match.get(mid, [])
        if not target_results:
            print(f"  [NOT FOUND] {mid} → bad-game-results にレコードなし（スキップ）")
            not_found += 1
            continue

        # history から補足情報を取得
        mode            = h.get("mode", "unknown")
        waiting_players = decimal_safe(h.get("waiting", []))
        skill_snapshot  = decimal_safe(h.get("skill_snapshot", {}))

        for r in target_results:
            result_id = r.get("result_id")
            if not result_id:
                print(f"  [WARN] result_id なし: {r}")
                continue

            # すでに pairing_mode が入っていればスキップ
            if r.get("pairing_mode"):
                skip += 1
                continue

            if dry_run:
                print(f"  [DRY] {mid} court={r.get('court_number')} mode={mode} waiting={len(waiting_players)}人 snap={len(skill_snapshot)}人分")
            else:
                results_table.update_item(
                    Key={"result_id": result_id},
                    UpdateExpression=(
                        "SET pairing_mode = :pm, "
                        "    waiting_players = :wp, "
                        "    skill_snapshot = :ss"
                    ),
                    ExpressionAttributeValues={
                        ":pm": mode,
                        ":wp": waiting_players,
                        ":ss": skill_snapshot,
                    },
                )
                ok += 1

    print()
    if dry_run:
        print("=== DRY RUN 完了（書き込みなし）===")
    else:
        print(f"=== 移行完了 ===")
        print(f"  更新: {ok} 件")
        print(f"  スキップ（既存）: {skip} 件")
        print(f"  対応なし: {not_found} 件")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry", action="store_true", help="ドライラン（書き込みなし）")
    args = parser.parse_args()
    main(dry_run=args.dry)
import os
import json
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Key, Attr

# ===== 設定（必要ならここだけ修正） =====
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
HISTORY_TABLE = os.getenv("DYNAMO_PART_HISTORY_TABLE", "bad-users-history")

USER_ID = "c1c59d06-f3e8-4dad-a81f-973aefdf4978" 
DATES = ["2025-11-04", "2025-11-08"]              # 問題の日付
# ======================================


def _json_default(o):
    if isinstance(o, Decimal):
        if o % 1 == 0:
            return int(o)
        return float(o)
    return str(o)


def pick(it, keys):
    return {k: it.get(k) for k in keys}


def query_by_date(table, user_id: str, event_date: str):
    """
    user_id をパーティションキーにして query できる前提。
    event_date は FilterExpression で絞る（GSI不要）。
    """
    resp = table.query(
        KeyConditionExpression=Key("user_id").eq(user_id),
        FilterExpression=Attr("event_date").eq(event_date),
    )
    items = resp.get("Items", [])
    # もしページングがある場合に備えて全取得
    while "LastEvaluatedKey" in resp:
        resp = table.query(
            KeyConditionExpression=Key("user_id").eq(user_id),
            FilterExpression=Attr("event_date").eq(event_date),
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp.get("Items", []))
    return items


def main():
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamodb.Table(HISTORY_TABLE)

    print(f"[INFO] region={AWS_REGION}")
    print(f"[INFO] history_table={HISTORY_TABLE}")
    print(f"[INFO] user_id={USER_ID}")
    print()

    for d in DATES:
        items = query_by_date(table, USER_ID, d)

        # 分類
        participations = [x for x in items if x.get("entity_type") == "event_participation"]
        spends = [x for x in items if x.get("entity_type") == "point_transaction" and x.get("kind") == "spend"]

        print("=" * 70)
        print(f"[DATE] {d}  total_items={len(items)}")
        print(f"  event_participation: {len(participations)}")
        print(f"  point_transaction(spend): {len(spends)}")

        # 重要なフィールドだけ表示（多すぎないように）
        show_keys = [
            "entity_type", "kind", "event_date", "schedule_id", "status", "action",
            "joined_at", "created_at", "paid_at", "tx_id", "reason", "payment_type"
        ]

        if participations:
            print("\n-- event_participation items --")
            for it in participations:
                print(json.dumps(pick(it, show_keys), ensure_ascii=False, default=_json_default, indent=2))

        if spends:
            print("\n-- spend items --")
            for it in spends:
                print(json.dumps(pick(it, show_keys), ensure_ascii=False, default=_json_default, indent=2))

        if not participations and not spends:
            print("\n  (no participation/spend items found for this date)")

        print()

    print("[DONE]")


if __name__ == "__main__":
    main()
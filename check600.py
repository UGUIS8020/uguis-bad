import os
import sys
import argparse
from datetime import datetime
import boto3
from boto3.dynamodb.conditions import Key


def list_tables(region: str, limit: int = 50):
    client = boto3.client("dynamodb", region_name=region)
    resp = client.list_tables(Limit=limit)
    tables = resp.get("TableNames", [])
    return tables


def audit_part_history_for_user(table, user_id: str):
    """
    user_id で Query して、変なレコード（参加履歴ではない / 欠損 / 形式崩れ）を表示
    """
    items = []
    resp = table.query(KeyConditionExpression=Key("user_id").eq(user_id))
    items.extend(resp.get("Items", []))

    while "LastEvaluatedKey" in resp:
        resp = table.query(
            KeyConditionExpression=Key("user_id").eq(user_id),
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp.get("Items", []))

    print(f"[INFO] items(query result): {len(items)}")

    suspicious = 0
    for it in items:
        reasons = []

        # 参加履歴として期待しているキー
        has_date = "date" in it
        has_any_ts = any(k in it for k in ("joined_at", "created_at", "registered_at"))

        # 支払い系が混ざっていないか
        looks_like_payment = ("payment_type" in it) or (it.get("entity_type") == "point_transaction")

        if looks_like_payment:
            reasons.append("looks_like_payment(point_transaction)")
        if not has_date:
            reasons.append("missing_date_field")
        if not has_any_ts:
            reasons.append("missing_timestamp_fields")

        # date の形式チェック
        if has_date:
            d = str(it.get("date") or "").strip()
            try:
                datetime.strptime(d, "%Y-%m-%d")
            except Exception:
                reasons.append(f"bad_date_format:{repr(d)}")

        # joined_at が points#... の形式なら混在の強い証拠
        ja = it.get("joined_at")
        if isinstance(ja, str) and ja.startswith("points#"):
            reasons.append("joined_at_points_format")

        if reasons:
            suspicious += 1
            print("\n[FOUND] suspicious item")
            print("reasons:", reasons)
            keys_to_show = [
                "user_id", "date", "event_date", "status",
                "joined_at", "created_at", "registered_at",
                "entity_type", "payment_type", "reason", "tx_id",
                "schedule_id",
            ]
            for k in keys_to_show:
                if k in it:
                    print(f"  {k}: {it[k]}")

    print("\n====================")
    print("Query summary")
    print("====================")
    print("total:", len(items))
    print("suspicious:", suspicious)


def audit_all_part_history_table(table, limit: int = 200):
    """
    テーブル全体を Scan して、混在・欠損などをサンプル抽出
    """
    scanned = 0
    suspicious = 0
    last_evaluated_key = None

    while True:
        kwargs = {"Limit": min(limit - scanned, 200)}
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key

        resp = table.scan(**kwargs)
        items = resp.get("Items", [])

        for it in items:
            scanned += 1
            reasons = []

            has_date = "date" in it
            has_any_ts = any(k in it for k in ("joined_at", "created_at", "registered_at"))
            looks_like_payment = ("payment_type" in it) or (it.get("entity_type") == "point_transaction")

            if looks_like_payment:
                reasons.append("looks_like_payment(point_transaction)")
            if not has_date:
                reasons.append("missing_date_field")
            if not has_any_ts:
                reasons.append("missing_timestamp_fields")

            if has_date:
                d = str(it.get("date") or "").strip()
                try:
                    datetime.strptime(d, "%Y-%m-%d")
                except Exception:
                    reasons.append(f"bad_date_format:{repr(d)}")

            ja = it.get("joined_at")
            if isinstance(ja, str) and ja.startswith("points#"):
                reasons.append("joined_at_points_format")

            if reasons:
                suspicious += 1
                print("\n[FOUND] suspicious item (scan)")
                print("reasons:", reasons)
                keys_to_show = [
                    "user_id", "date", "event_date", "status",
                    "joined_at", "created_at", "registered_at",
                    "entity_type", "payment_type", "reason", "tx_id",
                    "schedule_id",
                ]
                for k in keys_to_show:
                    if k in it:
                        print(f"  {k}: {it[k]}")

            if scanned >= limit:
                break

        if scanned >= limit:
            break

        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    print("\n====================")
    print("Scan summary")
    print("====================")
    print("scanned:", scanned)
    print("suspicious:", suspicious)


def main():
    print("[BOOT] check600.py started")

    parser = argparse.ArgumentParser(description="Audit participation history table")
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "ap-northeast-1"))
    parser.add_argument("--table", default=os.getenv("DYNAMO_PART_HISTORY_TABLE", ""))
    parser.add_argument("--user", default=os.getenv("USER_ID", ""))
    parser.add_argument("--scan", action="store_true", help="scan table sample (default: off)")
    parser.add_argument("--limit", type=int, default=200, help="scan sample limit (default: 200)")
    args = parser.parse_args()

    if not args.table:
        print("[ERROR] table name is empty.")
        print("Set env DYNAMO_PART_HISTORY_TABLE or pass --table <name>\n")
        print("[HINT] Tables in this region (first 50):")
        try:
            for t in list_tables(args.region, limit=50):
                print(" -", t)
        except Exception as e:
            print("[ERROR] failed to list tables:", e)
        return

    dynamodb = boto3.resource("dynamodb", region_name=args.region)
    table = dynamodb.Table(args.table)

    print(f"[INFO] region={args.region}")
    print(f"[INFO] table={args.table}")

    # user指定があれば userのquery監査
    if args.user:
        print(f"[INFO] auditing user_id={args.user}")
        audit_part_history_for_user(table, args.user)
    else:
        print("[WARN] --user not provided; skip Query audit (user_id)")

    # --scan が指定されたらテーブル全体サンプルスキャン
    if args.scan:
        print(f"[INFO] scanning table sample limit={args.limit}")
        audit_all_part_history_table(table, limit=args.limit)

    print("[DONE] check600.py finished")


if __name__ == "__main__":
    main()

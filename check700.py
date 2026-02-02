# audit_missing_timestamp_range.py
import os
import argparse
from collections import Counter
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

TS_FIELDS = ("joined_at", "created_at", "registered_at")

def safe_date(s: str):
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def has_any_timestamp(item: dict) -> bool:
    for k in TS_FIELDS:
        v = item.get(k)
        if v is None:
            continue
        if str(v).strip():
            return True
    return False

def audit(table_name: str, region: str, limit: int | None = None):
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    scanned = 0
    suspicious = 0

    min_d = None
    max_d = None
    month_counter = Counter()
    examples = []  # 最初の数件だけ保存

    eks = None
    while True:
        kwargs = {}
        if eks:
            kwargs["ExclusiveStartKey"] = eks

        try:
            resp = table.scan(**kwargs)
        except ClientError as e:
            print("[ERROR] scan failed:", e)
            return

        items = resp.get("Items", [])
        for it in items:
            scanned += 1
            d = it.get("date")  # 参加履歴テーブルは date を想定
            if not d:
                continue

            # timestamp 無しなら suspicious
            if not has_any_timestamp(it):
                suspicious += 1
                dd = safe_date(str(d))
                if dd:
                    if (min_d is None) or (dd < min_d):
                        min_d = dd
                    if (max_d is None) or (dd > max_d):
                        max_d = dd
                    month_counter[dd.strftime("%Y-%m")] += 1

                if len(examples) < 10:
                    examples.append({
                        "user_id": it.get("user_id"),
                        "date": it.get("date"),
                        "schedule_id": it.get("schedule_id"),
                        "keys_present": [k for k in TS_FIELDS if k in it],
                    })

            if limit and scanned >= limit:
                eks = None
                break

        if limit and scanned >= limit:
            break

        eks = resp.get("LastEvaluatedKey")
        if not eks:
            break

    print("===================================")
    print("Missing timestamp audit result")
    print("===================================")
    print(f"table: {table_name}")
    print(f"scanned: {scanned}")
    print(f"suspicious(missing timestamp): {suspicious}")
    print(f"min date: {min_d}")
    print(f"max date: {max_d}")

    if month_counter:
        print("\n--- counts by month (YYYY-MM) ---")
        for ym, cnt in sorted(month_counter.items()):
            print(f"{ym}: {cnt}")

    if examples:
        print("\n--- examples (first 10) ---")
        for ex in examples:
            print(ex)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default=os.getenv("DYNAMO_PART_HISTORY_TABLE") or "bad_participation_history")
    ap.add_argument("--region", default=os.getenv("AWS_REGION") or "ap-northeast-1")
    ap.add_argument("--limit", type=int, default=0, help="0 = no limit (scan all)")
    args = ap.parse_args()

    limit = args.limit if args.limit and args.limit > 0 else None
    audit(args.table, args.region, limit=limit)

if __name__ == "__main__":
    main()

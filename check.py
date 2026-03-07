# check.py
# DynamoDB の bad-game-matches を確認するスクリプト
# 使い方:
#   python check.py
#   python check.py --match-id rest_queue
#   python check.py --match-id meta#rest_queue
#   python check.py --prefix meta#
#   python check.py --limit 50
#   python check.py --all

import os
import sys
import json
import argparse
from decimal import Decimal
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr


TABLE_NAME_DEFAULT = "bad-game-matches"


def _json_default(o):
    if isinstance(o, Decimal):
        # Decimal -> int or float
        if o % 1 == 0:
            return int(o)
        return float(o)
    return str(o)


def _shorten(s, n=120):
    if s is None:
        return None
    s = str(s)
    return s if len(s) <= n else s[:n] + "..."


def _print_item(item):
    match_id = item.get("match_id")
    print("=" * 80)
    print(f"match_id: {match_id}")

    # よく使うメタ項目があれば見やすく表示
    keys = [
        "status",
        "current_match_id",
        "court_count",
        "queue",
        "generation",
        "version",
        "cycle_started_at",
        "updated_at",
        "locked_at",
        "players",
    ]
    for k in keys:
        if k in item:
            v = item.get(k)
            if k == "queue" and isinstance(v, list):
                print(f"{k}: (len={len(v)}) {_shorten(v)}")
            elif k == "players" and isinstance(v, list):
                print(f"{k}: (len={len(v)}) {_shorten(v)}")
            else:
                print(f"{k}: {_shorten(v)}")

    # 全体も確認したい場合用（必要ならコメントアウトを外す）
    # print(json.dumps(item, ensure_ascii=False, indent=2, default=_json_default))


def _describe_table(dynamodb_client, table_name: str):
    try:
        desc = dynamodb_client.describe_table(TableName=table_name)["Table"]
        print(f"\n=== {table_name} ===")
        print("TableStatus:", desc.get("TableStatus"))
        print("ItemCount  :", desc.get("ItemCount"), "(eventually consistent)")
        print("KeySchema  :", desc.get("KeySchema"))
        print("AttrDefs   :", desc.get("AttributeDefinitions"))
    except ClientError as e:
        print("[ERROR] describe_table failed:", e)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default=TABLE_NAME_DEFAULT, help="DynamoDB table name")
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "ap-northeast-1"), help="AWS region")
    parser.add_argument("--profile", default=os.getenv("AWS_PROFILE"), help="AWS profile (optional)")
    parser.add_argument("--match-id", default=None, help="get_item で特定 match_id を取得")
    parser.add_argument("--prefix", default=None, help="scan で match_id の prefix を探す（例: meta#）")
    parser.add_argument("--limit", type=int, default=30, help="scan 件数上限")
    parser.add_argument("--all", action="store_true", help="scan を全件走査（件数が多いと遅い）")
    parser.add_argument("--keys-only", action="store_true", help="match_id のみ表示")
    args = parser.parse_args()

    # セッション
    if args.profile:
        session = boto3.Session(profile_name=args.profile, region_name=args.region)
    else:
        session = boto3.Session(region_name=args.region)

    dynamodb = session.resource("dynamodb")
    dynamodb_client = session.client("dynamodb")
    table = dynamodb.Table(args.table)

    _describe_table(dynamodb_client, args.table)

    # まずは最重要：rest_queue / meta#rest_queue の両方を get_item で確認
    print("\n--- Quick checks (rest_queue / meta#rest_queue) ---")
    for key in ["rest_queue", "meta#rest_queue", "meta#current", "current", "meta#rest-queue"]:
        try:
            r = table.get_item(Key={"match_id": key}, ConsistentRead=True)
            item = r.get("Item")
            if item:
                print(f"[FOUND] match_id={key}  keys={list(item.keys())}")
                if not args.keys_only:
                    _print_item(item)
            else:
                print(f"[MISS ] match_id={key}")
        except ClientError as e:
            print(f"[ERROR] get_item match_id={key}: {e}")

    # 特定 match_id 指定があれば表示して終了
    if args.match_id:
        print(f"\n--- get_item (match_id={args.match_id}) ---")
        try:
            r = table.get_item(Key={"match_id": args.match_id}, ConsistentRead=True)
            item = r.get("Item")
            if not item:
                print("[MISS] not found")
                return
            if args.keys_only:
                print(item.get("match_id"))
            else:
                _print_item(item)
        except ClientError as e:
            print("[ERROR] get_item failed:", e)
        return

    # prefix scan（または軽い一覧）
    print("\n--- scan ---")
    scanned = 0
    shown = 0
    start_key = None

    # 条件
    filter_expr = None
    if args.prefix:
        filter_expr = Attr("match_id").begins_with(args.prefix)

    # scan 実行
    try:
        while True:
            scan_kwargs = {}
            if filter_expr is not None:
                scan_kwargs["FilterExpression"] = filter_expr
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key

            resp = table.scan(**scan_kwargs)
            items = resp.get("Items", [])
            scanned += resp.get("ScannedCount", 0)

            for it in items:
                if args.keys_only:
                    print(it.get("match_id"))
                else:
                    _print_item(it)
                shown += 1

                if not args.all and shown >= args.limit:
                    print(f"\n[STOP] shown limit reached: {shown} (scanned={scanned})")
                    return

            start_key = resp.get("LastEvaluatedKey")
            if not start_key:
                break

        print(f"\n[DONE] shown={shown} scanned={scanned}")
        if args.prefix and shown == 0:
            print(f"[HINT] prefix '{args.prefix}' に一致する match_id が見つかりませんでした。")

    except ClientError as e:
        print("[ERROR] scan failed:", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
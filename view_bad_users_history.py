import os
import argparse
from decimal import Decimal
from datetime import datetime
import boto3
from boto3.dynamodb.conditions import Key, Attr


def _to_jsonable(x):
    if isinstance(x, Decimal):
        if x % 1 == 0:
            return int(x)
        return float(x)
    if isinstance(x, dict):
        return {k: _to_jsonable(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_to_jsonable(v) for v in x]
    return x


def _parse_iso(dt_str: str):
    # '2025-12-25T05:47:42.506386' 想定
    try:
        return datetime.fromisoformat(dt_str)
    except Exception:
        return None


def query_by_user_id(table, user_id: str, limit: int | None = None):
    items = []
    kwargs = {
        "KeyConditionExpression": Key("user_id").eq(user_id),
        "ScanIndexForward": False,
    }
    if limit:
        kwargs["Limit"] = limit

    resp = table.query(**kwargs)
    items.extend(resp.get("Items", []))

    while resp.get("LastEvaluatedKey") and (limit is None):
        resp = table.query(**kwargs, ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))

    return items


def scan_with_filters(table, user_id=None, status=None, start_date=None, end_date=None, limit=None):
    fe = None
    if user_id:
        fe = Attr("user_id").eq(user_id) if fe is None else fe & Attr("user_id").eq(user_id)
    if status:
        fe = Attr("status").eq(status) if fe is None else fe & Attr("status").eq(status)
    if start_date:
        fe = Attr("date").gte(start_date) if fe is None else fe & Attr("date").gte(start_date)
    if end_date:
        fe = Attr("date").lte(end_date) if fe is None else fe & Attr("date").lte(end_date)

    kwargs = {}
    if fe is not None:
        kwargs["FilterExpression"] = fe
    if limit:
        kwargs["Limit"] = limit

    items = []
    resp = table.scan(**kwargs)
    items.extend(resp.get("Items", []))

    while resp.get("LastEvaluatedKey") and (limit is None):
        resp = table.scan(**kwargs, ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))

    return items


def effective_by_date(items: list[dict], exclude_cancelled: bool = False):
    """
    同一 date が複数ある場合、joined_at が最も新しいもの1件に圧縮
    """
    best = {}  # date -> item
    for it in items:
        date = it.get("date")
        joined_at = it.get("joined_at")
        if not date or not joined_at:
            continue

        dt = _parse_iso(joined_at)
        if dt is None:
            continue

        cur = best.get(date)
        if cur is None:
            best[date] = it
            continue

        cur_dt = _parse_iso(cur.get("joined_at", ""))
        if cur_dt is None or dt > cur_dt:
            best[date] = it

    out = list(best.values())
    # date 昇順で並べる（見やすい）
    out.sort(key=lambda x: x.get("date", ""))

    if exclude_cancelled:
        out = [x for x in out if x.get("status") != "cancelled"]

    return out


def print_items(items: list[dict]):
    for i, it in enumerate(items, 1):
        itj = _to_jsonable(it)
        date = itj.get("date")
        joined_at = itj.get("joined_at")
        uid = itj.get("user_id")
        status = itj.get("status")
        action = itj.get("action")
        schedule_id = itj.get("schedule_id")
        location = itj.get("location")
        print(f"[{i}] date={date} joined_at={joined_at} user_id={uid} status={status} action={action} schedule_id={schedule_id} location={location}")
        print(itj)
        print("-" * 80)


def main():
    parser = argparse.ArgumentParser(description="View DynamoDB bad-users-history table data")
    parser.add_argument("--table", default=os.getenv("DYNAMO_BAD_USERS_HISTORY_TABLE", "bad-users-history"))
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "ap-northeast-1"))

    parser.add_argument("--user-id", help="Filter by user_id")
    parser.add_argument("--status", help="Filter by status (registered/cancelled)")
    parser.add_argument("--start", help="Start date YYYY-MM-DD (scan filter)")
    parser.add_argument("--end", help="End date YYYY-MM-DD (scan filter)")
    parser.add_argument("--limit", type=int, help="Limit number of items (query/scan)")

    parser.add_argument("--mode", choices=["auto", "query", "scan"], default="auto",
                        help="auto: if --user-id then query else scan")

    # 追加：日付ごとの最終状態に圧縮
    parser.add_argument("--effective", action="store_true",
                        help="Collapse records into 1 item per date using latest joined_at")
    parser.add_argument("--exclude-cancelled", action="store_true",
                        help="(with --effective) exclude dates whose final status is cancelled")

    args = parser.parse_args()

    ddb = boto3.resource("dynamodb", region_name=args.region)
    table = ddb.Table(args.table)

    if args.mode == "query" or (args.mode == "auto" and args.user_id):
        if not args.user_id:
            raise SystemExit("--mode query requires --user-id")
        items = query_by_user_id(table, args.user_id, limit=args.limit)
        # query だと start/end/status のサーバ側フィルタはしない（必要ならローカルで）
        if args.start:
            items = [x for x in items if x.get("date") and x["date"] >= args.start]
        if args.end:
            items = [x for x in items if x.get("date") and x["date"] <= args.end]
        if args.status:
            items = [x for x in items if x.get("status") == args.status]
    else:
        items = scan_with_filters(
            table,
            user_id=args.user_id,
            status=args.status,
            start_date=args.start,
            end_date=args.end,
            limit=args.limit
        )

    print(f"\n=== {args.table}: raw {len(items)} items ===\n")

    if args.effective:
        eff = effective_by_date(items, exclude_cancelled=args.exclude_cancelled)
        print(f"=== effective-by-date: {len(eff)} items (exclude_cancelled={args.exclude_cancelled}) ===\n")
        print_items(eff)
    else:
        print_items(items)


if __name__ == "__main__":
    main()

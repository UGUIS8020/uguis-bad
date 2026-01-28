import os
import json
import argparse
from collections import Counter
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr, Key
from dotenv import load_dotenv

load_dotenv()

TABLE_NAME = os.getenv("POSTS_TABLE", "uguu_post")  # 環境変数がなければ posts

# DynamoDBのDecimalをJSON化するためのエンコーダ
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            # 整数っぽいならintに
            if o % 1 == 0:
                return int(o)
            return float(o)
        return super().default(o)

def get_table():
    dynamodb = boto3.resource(
        "dynamodb",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    return dynamodb.Table(TABLE_NAME)

def scan_all(table, *, filter_expr=None, expr_attr_vals=None, limit=None):
    """テーブルをscanで全部（またはlimitまで）取得"""
    items = []
    kwargs = {}
    if filter_expr is not None:
        kwargs["FilterExpression"] = filter_expr
    if expr_attr_vals is not None:
        kwargs["ExpressionAttributeValues"] = expr_attr_vals

    last_evaluated_key = None
    while True:
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key

        res = table.scan(**kwargs)
        batch = res.get("Items", [])
        items.extend(batch)

        last_evaluated_key = res.get("LastEvaluatedKey")
        if limit and len(items) >= limit:
            return items[:limit]
        if not last_evaluated_key:
            break

    return items

def cmd_summary(args):
    table = get_table()
    # まずは全件が重い場合に備え、sample scanもできるように
    items = scan_all(table, limit=args.sample)

    print(f"[TABLE] {TABLE_NAME}")
    print(f"[ITEMS] fetched = {len(items)} (sample={args.sample})")

    pk_prefix = Counter()
    sk_prefix = Counter()

    for it in items:
        pk = str(it.get("PK", ""))
        sk = str(it.get("SK", ""))

        pk_prefix[pk.split("#", 1)[0] if pk else "(none)"] += 1
        sk_prefix[sk.split("#", 1)[0] if sk else "(none)"] += 1

    print("\n[PK prefix counts]")
    for k, v in pk_prefix.most_common(20):
        print(f"  {k:12s} : {v}")

    print("\n[SK prefix counts]")
    for k, v in sk_prefix.most_common(20):
        print(f"  {k:12s} : {v}")

def cmd_list_metadata(args):
    table = get_table()

    # SK が METADATA# で始まるもの（投稿メタデータ）だけ
    items = scan_all(
        table,
        filter_expr=Attr("SK").begins_with("METADATA#"),
        limit=args.sample,
    )

    # created_at / updated_at で並べて上位表示
    items_sorted = sorted(
        items,
        key=lambda x: str(x.get("updated_at") or x.get("created_at") or ""),
        reverse=True,
    )

    print(f"[METADATA] items={len(items_sorted)} (sample={args.sample})\n")
    for it in items_sorted[: args.n]:
        post_id = it.get("post_id")
        pk = it.get("PK")
        sk = it.get("SK")
        created_at = it.get("created_at")
        updated_at = it.get("updated_at")
        user_id = it.get("user_id")
        content = it.get("content", "")
        content_preview = (content[:40] + "...") if isinstance(content, str) and len(content) > 40 else content

        print("-" * 80)
        print(f"post_id    : {post_id}")
        print(f"PK / SK    : {pk} / {sk}")
        print(f"user_id    : {user_id}")
        print(f"created_at : {created_at}")
        print(f"updated_at : {updated_at}")
        print(f"content    : {content_preview}")
        # feed確認用
        if "feed_pk" in it or "feed_sk" in it:
            print(f"feed_pk/sk : {it.get('feed_pk')} / {it.get('feed_sk')}")

def cmd_show_post(args):
    table = get_table()
    post_id = args.post_id

    pk = f"POST#{post_id}"
    res = table.query(
        KeyConditionExpression=Key("PK").eq(pk)
    )
    items = res.get("Items", [])

    print(f"[POST] PK={pk} items={len(items)}\n")
    # SK順に表示
    items = sorted(items, key=lambda x: str(x.get("SK", "")))

    for it in items:
        print("-" * 80)
        print(json.dumps(it, ensure_ascii=False, indent=2, cls=DecimalEncoder))

def main():
    parser = argparse.ArgumentParser(description="Inspect DynamoDB posts table")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("summary", help="show PK/SK prefix distribution")
    p1.add_argument("--sample", type=int, default=200, help="scan sample size (None = all, but not recommended)")
    p1.set_defaults(func=cmd_summary)

    p2 = sub.add_parser("metadata", help="list METADATA# items sorted by time")
    p2.add_argument("-n", type=int, default=10, help="how many rows to print")
    p2.add_argument("--sample", type=int, default=500, help="how many items to scan (sample)")
    p2.set_defaults(func=cmd_list_metadata)

    p3 = sub.add_parser("post", help="show all items under a specific POST#<post_id>")
    p3.add_argument("post_id", help="post_id (uuid)")
    p3.set_defaults(func=cmd_show_post)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()

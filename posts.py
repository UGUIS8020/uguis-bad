import os
import argparse
from typing import Any, Dict, List

import boto3
from boto3.dynamodb.conditions import Attr
from dotenv import load_dotenv

load_dotenv()

def dynamodb_resource():
    return boto3.resource(
        "dynamodb",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )

def table_exists(dynamodb, name: str) -> bool:
    try:
        dynamodb.Table(name).load()
        return True
    except Exception:
        return False

def create_table(dynamodb, name: str):
    print(f"[CREATE] Creating table: {name}")
    tbl = dynamodb.create_table(
        TableName=name,
        KeySchema=[
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",  # オンデマンド
    )
    tbl.wait_until_exists()
    print(f"[OK] Table created: {name}")

def scan_all(table, filter_expr=None) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    eks = None
    while True:
        kwargs: Dict[str, Any] = {}
        if filter_expr is not None:
            kwargs["FilterExpression"] = filter_expr
        if eks:
            kwargs["ExclusiveStartKey"] = eks

        res = table.scan(**kwargs)
        items.extend(res.get("Items", []))
        eks = res.get("LastEvaluatedKey")
        if not eks:
            break
    return items

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="posts")
    parser.add_argument("--dest", default="uguu_post")
    parser.add_argument("--create-dest", action="store_true")
    parser.add_argument("--metadata-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    dynamodb = dynamodb_resource()
    src_table = dynamodb.Table(args.source)

    if args.create_dest and not table_exists(dynamodb, args.dest):
        create_table(dynamodb, args.dest)

    if not table_exists(dynamodb, args.dest):
        raise RuntimeError(f"Destination table '{args.dest}' not found. Use --create-dest or create it in console.")

    dst_table = dynamodb.Table(args.dest)

    filter_expr = Attr("SK").begins_with("METADATA#") if args.metadata_only else None
    items = scan_all(src_table, filter_expr=filter_expr)

    print(f"[SOURCE] {args.source} items_to_copy={len(items)}")
    if items:
        print(f"[SAMPLE] PK={items[0].get('PK')} SK={items[0].get('SK')}")

    if args.dry_run:
        print("[DRY-RUN] done.")
        return

    written = 0
    with dst_table.batch_writer(overwrite_by_pkeys=["PK", "SK"]) as batch:
        for it in items:
            batch.put_item(Item=dict(it))
            written += 1

    print(f"[DONE] written={written} to {args.dest}")

if __name__ == "__main__":
    main()

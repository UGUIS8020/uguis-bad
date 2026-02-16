import os
from dotenv import load_dotenv
import boto3
from boto3.dynamodb.conditions import Attr

load_dotenv()

REGION = os.getenv("AWS_REGION", "ap-northeast-1")
TABLE = "bad-game-match_entries"

dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE)

def scan_all(table, limit_pages=50):
    """Scanをページングして全部集める（過剰に回さないよう上限あり）"""
    items = []
    last_key = None
    page = 0

    while True:
        page += 1
        if page > limit_pages:
            break

        kwargs = {}
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key

        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    return items

def scan_all_with_filter(table, filter_expr, limit_pages=50):
    """FilterExpression付きでScan（ページング）"""
    items = []
    last_key = None
    page = 0

    while True:
        page += 1
        if page > limit_pages:
            break

        kwargs = {"FilterExpression": filter_expr}
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key

        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    return items

# --- 1) まずは entry_status があるものだけ取って、キーを確認 ---
items = scan_all_with_filter(table, Attr("entry_status").exists())

print(f"items(filtered) = {len(items)}")

if items:
    print("sample keys:", sorted(items[0].keys()))
    # court系キーを探して表示
    court_keys = [k for k in items[0].keys() if "court" in k.lower()]
    print("court-like keys in first item:", court_keys)
    for k in court_keys:
        print(f"  {k} = {items[0].get(k)}")
else:
    print("no items (filtered). try scan_all without filter...")
    all_items = scan_all(table)
    print(f"items(all) = {len(all_items)}")
    if all_items:
        print("sample keys(all):", sorted(all_items[0].keys()))

# --- 2) STSで実行主体も確認 ---
sts = boto3.client("sts", region_name=REGION)
print("CallerIdentity:", sts.get_caller_identity())
print("AWS_REGION:", REGION)
print("TABLE:", TABLE)

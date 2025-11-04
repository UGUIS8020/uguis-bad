import os, csv
import boto3
from decimal import Decimal
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

load_dotenv()

TABLE_NAME = "bad-users-history"
REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-northeast-1"

# オプション
NUMERIC_ONLY = True             # True: 数値として 600 のみ。False: 文字列に "600" を含むもマッチ
EXCLUDE_FIELDS = {"joined_at"}  # ここに含まれる属性名は文字列照合をスキップ（NUMERIC_ONLY=Falseのとき有効）
EXPORT_CSV = False              # True で CSV 出力
CSV_PATH = "hits_600.csv"

def _is_num_600(v):
    if isinstance(v, (int, float, Decimal)):
        try:
            return Decimal(v) == Decimal(600)
        except Exception:
            return False
    return False

def _is_str_contains_600(v):
    return isinstance(v, str) and ("600" in v)

def _walk(item, path=""):
    """
    アイテムを再帰的に探索し、マッチした (path, value) を yield
    path 例: "points", "meta.scores[2].value"
    """
    if isinstance(item, dict):
        for k, v in item.items():
            subpath = f"{path}.{k}" if path else k
            yield from _walk(v, subpath)
    elif isinstance(item, list):
        for i, v in enumerate(item):
            subpath = f"{path}[{i}]"
            yield from _walk(v, subpath)
    else:
        # 葉ノード
        if NUMERIC_ONLY:
            if _is_num_600(item):
                yield (path, item)
        else:
            # 数値 600 も OK、文字列 "600" も OK（ただし除外フィールドの直下は除く）
            if _is_num_600(item):
                yield (path, item)
            else:
                # 除外フィールド直下の文字列はスキップ
                direct_field = path.split(".", 1)[0] if path else ""
                if direct_field not in EXCLUDE_FIELDS and _is_str_contains_600(item):
                    yield (path, item)

def key_str(item, key_schema):
    parts = []
    for ks in key_schema:
        name = ks["AttributeName"]
        parts.append(f"{name}={item.get(name)}")
    return ", ".join(parts) if parts else "(no key info)"

def main():
    session = boto3.session.Session(region_name=REGION)
    creds = session.get_credentials()
    if not creds or not creds.access_key:
        raise NoCredentialsError()

    dynamodb = session.resource("dynamodb", config=Config(retries={"max_attempts": 10, "mode": "standard"}))
    table = dynamodb.Table(TABLE_NAME)
    key_schema = table.key_schema
    client = table.meta.client
    paginator = client.get_paginator("scan")

    print(f"=== SCAN {TABLE_NAME} (region={REGION}) ===")
    total_hits = 0
    csv_rows = []

    try:
        for page in paginator.paginate(TableName=TABLE_NAME, ReturnConsumedCapacity="NONE"):
            for item in page.get("Items", []):
                matches = list(_walk(item))
                if matches:
                    total_hits += 1
                    print(f"- HIT #{total_hits} @ {key_str(item, key_schema)}")
                    for (p, v) in matches:
                        print(f"    • {p} -> {v}")
                        if EXPORT_CSV:
                            csv_rows.append({
                                "hit_index": total_hits,
                                "key": key_str(item, key_schema),
                                "path": p,
                                "value": str(v)
                            })
        print(f"-> {total_hits} item(s) with 600 in {TABLE_NAME}")

        if EXPORT_CSV and csv_rows:
            with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["hit_index", "key", "path", "value"])
                writer.writeheader()
                writer.writerows(csv_rows)
            print(f"[Saved] {CSV_PATH}")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        msg = e.response["Error"]["Message"]
        print(f"[ERROR] {code}: {msg}")
        if code in ("AccessDeniedException", "UnrecognizedClientException"):
            print("※ dynamodb:Scan 権限とリージョン/アカウントをご確認ください。")

if __name__ == "__main__":
    main()
"""
bad-users テーブルの PK値から user# プレフィックスを除去する移行スクリプト
user#abc123 → abc123
"""
import boto3
import json
import time
from decimal import Decimal

# ---- 設定 ----
TABLE_NAME = "bad-users"
REGION = "ap-northeast-1"

dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)

def scan_all():
    """全アイテムをスキャン"""
    items = []
    resp = table.scan()
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))
    return items

def migrate():
    print("スキャン開始...")
    items = scan_all()
    print(f"総アイテム数: {len(items)}")

    need_migration = [i for i in items if str(i.get("user#user_id", "")).startswith("user#")]
    already_plain  = [i for i in items if not str(i.get("user#user_id", "")).startswith("user#")]

    print(f"移行対象（user# あり）: {len(need_migration)}")
    print(f"スキップ（既にプレフィックスなし）: {len(already_plain)}")

    migrated = 0
    errors = 0

    for item in need_migration:
        old_pk = item["user#user_id"]           # user#abc123
        new_pk = old_pk[len("user#"):]          # abc123

        # 新しいアイテムを作成（PKだけ変更、他は全コピー）
        new_item = dict(item)
        new_item["user#user_id"] = new_pk

        try:
            # 新アイテムを put
            table.put_item(Item=new_item)

            # 旧アイテムを削除
            table.delete_item(Key={"user#user_id": old_pk})

            migrated += 1
            if migrated % 10 == 0:
                print(f"  進捗: {migrated}/{len(need_migration)}")

        except Exception as e:
            print(f"  [ERROR] {old_pk}: {e}")
            errors += 1

        time.sleep(0.05)  # レートリミット対策

    print(f"\n完了: 移行={migrated}, エラー={errors}, スキップ={len(already_plain)}")

if __name__ == "__main__":
    migrate()
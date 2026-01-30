# uguu/reconcile_practice_count.py
import os
import boto3

from .dynamo import db  # ← 同じパッケージ内なのでこれが使える

def _scan_all(table):
    items = []
    eks = None
    while True:
        kwargs = {}
        if eks:
            kwargs["ExclusiveStartKey"] = eks
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        eks = resp.get("LastEvaluatedKey")
        if not eks:
            break
    return items

def main():
    region = os.getenv("AWS_REGION", "ap-northeast-1")
    users_table_name = os.getenv("DYNAMO_USERS_TABLE", "bad-users")  # ← bad_users固定でもOK

    dynamodb = boto3.resource("dynamodb", region_name=region)
    users_table = dynamodb.Table(users_table_name)

    print(f"[reconcile] region={region} users_table={users_table_name}")

    users = _scan_all(users_table)
    print(f"[reconcile] scanned users={len(users)}")

    checked = 0
    updated = 0
    errors = 0

    for u in users:
        user_id = u.get("user#user_id")
        if not user_id:
            continue

        checked += 1
        try:
            records = db.get_user_participation_history_with_timestamp(str(user_id)) or []
            correct = len(records)

            cur_raw = u.get("practice_count")
            try:
                cur = int(cur_raw) if cur_raw is not None else None
            except Exception:
                cur = None

            if cur != correct:
                users_table.update_item(
                    Key={"user#user_id": str(user_id)},
                    UpdateExpression="SET practice_count = :c",
                    ExpressionAttributeValues={":c": int(correct)},
                )
                updated += 1

        except Exception as e:
            errors += 1
            print(f"[reconcile][WARN] user_id={user_id} error={e}")

    print(f"[reconcile] checked={checked} updated={updated} errors={errors}")

if __name__ == "__main__":
    main()

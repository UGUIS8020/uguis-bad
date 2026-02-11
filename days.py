import boto3
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key

REGION = "ap-northeast-1"
USERS_TABLE = "bad-users"
HISTORY_TABLE = "bad-users-history"

dynamodb = boto3.resource("dynamodb", region_name=REGION)
users = dynamodb.Table(USERS_TABLE)
history = dynamodb.Table(HISTORY_TABLE)

def scan_all(table):
    items = []
    resp = table.scan()
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))
    return items

def query_all_history(user_id: str):
    items = []
    resp = history.query(KeyConditionExpression=Key("user_id").eq(user_id))
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = history.query(
            KeyConditionExpression=Key("user_id").eq(user_id),
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp.get("Items", []))
    return items

def get_last_registered_date(user_id: str):
    today_jst = datetime.now(JST).date()

    dates = []
    for it in query_all_history(user_id):
        if it.get("status") != "registered":
            continue

        d = it.get("date")  # "YYYY-MM-DD"
        if not d:
            continue

        try:
            d_date = datetime.strptime(d, "%Y-%m-%d").date()
        except ValueError:
            continue

        # ★未来は除外
        if d_date > today_jst:
            continue

        dates.append(d)

    return max(dates) if dates else None

def main():
    now = datetime.now(timezone.utc).isoformat()
    all_users = scan_all(users)
    print("users:", len(all_users))

    updated_hist = 0
    updated_nohist = 0

    for u in all_users:
        pk = u.get("user#user_id")
        if not pk:
            continue
        user_id = pk.replace("user#", "")

        last_date = get_last_registered_date(user_id)

        if last_date:
            # 参加履歴あり → "1#" を付けて上位固定
            recent_sk = f"1#{last_date}#{user_id}"
            last_participation_date = last_date
            updated_hist += 1
        else:
            # 履歴なし → "0#" で下にまとめる（created_atが無ければ1900）
            base_date = (u.get("created_at") or "1900-01-01")[:10]
            recent_sk = f"0#{base_date}#{user_id}"
            last_participation_date = base_date
            updated_nohist += 1

        users.update_item(
            Key={"user#user_id": pk},
            UpdateExpression="SET recent_pk=:pk, recent_sk=:sk, last_participation_date=:d, last_participation_updated_at=:u",
            ExpressionAttributeValues={
                ":pk": "recent",
                ":sk": recent_sk,
                ":d": last_participation_date,
                ":u": now,
            },
        )

    print("updated(with history):", updated_hist)
    print("updated(no history):", updated_nohist)

if __name__ == "__main__":
    main()

# check_duplicates.py
import os
from collections import defaultdict
from dotenv import load_dotenv
import boto3

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
TABLE_NAME = os.getenv("DYNAMO_TABLE_NAME", "bad-users")

dynamodb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)
table = dynamodb.Table(TABLE_NAME)


def scan_all(table):
    """DynamoDBを最後までスキャンするジェネレータ"""
    params = {}
    while True:
        resp = table.scan(**params)
        for item in resp.get("Items", []):
            yield item
        if "LastEvaluatedKey" not in resp:
            break
        params["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def normalize_email(v: str | None) -> str | None:
    if not v:
        return None
    return v.strip().lower()


def normalize_name(v: str | None) -> str | None:
    if not v:
        return None
    return v.strip()


def extract_surname(v: str | None) -> str | None:
    """
    苗字っぽいところをざっくり抜く。
    - 全角スペース/半角スペースがあれば最初の要素を苗字とみなす
    - なければ先頭2〜3文字を返す（和名をざっくりグルーピングするため）
    """
    if not v:
        return None
    v = v.strip()
    # スペース区切り
    for sep in (" ", "　"):
        if sep in v:
            return v.split(sep)[0]
    # スペースがない場合は先頭2文字くらいでグルーピング
    return v[:2]


def main():
    # 1. それぞれのキーでグルーピング
    by_email = defaultdict(list)
    by_display_name = defaultdict(list)
    by_user_name = defaultdict(list)
    by_surname = defaultdict(list)

    for item in scan_all(table):
        pk = item.get("user#user_id") or item.get("user_id") or "(no-pk)"
        email = normalize_email(item.get("email"))
        display_name = normalize_name(item.get("display_name"))
        user_name = normalize_name(item.get("user_name"))
        surname = extract_surname(user_name or display_name)

        if email:
            by_email[email].append(item)
        if display_name:
            by_display_name[display_name].append(item)
        if user_name:
            by_user_name[user_name].append(item)
        if surname:
            by_surname[surname].append(item)

    print("=== 重複候補: メールアドレス完全一致 ===")
    for email, items in by_email.items():
        if len(items) > 1:
            print(f"- {email}")
            for it in items:
                print(
                    "   ",
                    it.get("user#user_id") or it.get("user_id"),
                    it.get("display_name"),
                    it.get("user_name"),
                    it.get("created_at"),
                )

    print("\n=== 重複候補: display_name 完全一致 ===")
    for name, items in by_display_name.items():
        if len(items) > 1:
            print(f"- {name}")
            for it in items:
                print(
                    "   ",
                    it.get("user#user_id") or it.get("user_id"),
                    it.get("email"),
                    it.get("user_name"),
                    it.get("created_at"),
                )

    print("\n=== 重複候補: user_name 完全一致 ===")
    for name, items in by_user_name.items():
        if len(items) > 1:
            print(f"- {name}")
            for it in items:
                print(
                    "   ",
                    it.get("user#user_id") or it.get("user_id"),
                    it.get("email"),
                    it.get("display_name"),
                    it.get("created_at"),
                )

    print("\n=== 重複候補: 苗字っぽい部分が同じ（ゆるめ） ===")
    for s, items in by_surname.items():
        # 苗字が同じ人は偶然もあるので、2件以上にしておく
        if len(items) > 1:
            print(f"- {s} ... {len(items)}件")
            for it in items:
                print(
                    "   ",
                    it.get("user#user_id") or it.get("user_id"),
                    it.get("email"),
                    it.get("display_name"),
                    it.get("user_name"),
                )


if __name__ == "__main__":
    main()

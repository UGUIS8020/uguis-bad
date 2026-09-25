#!/usr/bin/env python
"""
テストコート(game2)の手動テスト用に、実際にブラウザでログインできる
テストユーザーを bad-users に作成するスクリプト。

- 自動テスト(smoke_test_game2.py)が使う【TEST_GAME2】プレフィックスとは
  衝突しないよう、【手動T】プレフィックスを使う（自動テストのクリーン
  アップに巻き込まれて消えないようにするため）。
- 全員共通パスワードでログイン可能。
- 実力にばらつきを持たせ、組み合わせのテストがしやすいようにする。

使い方:
  python create_game2_manual_testusers.py --count 16
  python create_game2_manual_testusers.py --delete   # 作成したテストユーザーを削除
"""
import argparse
import os
import random
import uuid
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from werkzeug.security import generate_password_hash

PREFIX = "【手動T】"
PASSWORD = "uguis-test-2026"
REGION = os.getenv("AWS_REGION", "ap-northeast-1")


def get_users_table():
    ddb = boto3.resource("dynamodb", region_name=REGION)
    return ddb.Table("bad-users")


def create_users(count):
    users_table = get_users_table()
    hashed = generate_password_hash(PASSWORD, method="pbkdf2:sha256")

    created = []
    for i in range(1, count + 1):
        uid = str(uuid.uuid4())
        gender = "male" if i % 2 == 1 else "female"
        skill_score = Decimal(str(random.randint(15, 70)))
        email = f"manual_test_{i:02d}_{uid[:6]}@test.invalid"
        display_name = f"{PREFIX}選手{i:02d}"

        users_table.put_item(Item={
            "user#user_id": uid,
            "display_name": display_name,
            "user_name": display_name,
            "email": email,
            "password": hashed,
            "gender": gender,
            "administrator": False,
            "skill_score": skill_score,
            "skill_sigma": Decimal("8.333"),
            "organization": "other",
        })
        created.append((display_name, email))
        print(f"  作成: {display_name}  email={email}")

    print(f"\n作成完了: {len(created)}人")
    print(f"共通パスワード: {PASSWORD}")
    return created


def delete_users():
    users_table = get_users_table()
    resp = users_table.scan(FilterExpression=Attr("display_name").begins_with(PREFIX))
    items = resp.get("Items", [])
    while resp.get("LastEvaluatedKey"):
        resp = users_table.scan(FilterExpression=Attr("display_name").begins_with(PREFIX), ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))

    for u in items:
        users_table.delete_item(Key={"user#user_id": u["user#user_id"]})
        print(f"  削除: {u.get('display_name')}")
    print(f"\n削除完了: {len(items)}人")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=16)
    parser.add_argument("--delete", action="store_true")
    args = parser.parse_args()

    if args.delete:
        delete_users()
    else:
        create_users(args.count)

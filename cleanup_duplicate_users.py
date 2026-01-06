#!/usr/bin/env python
"""
重複したユーザーを「新しい方」に統合するスクリプト

今回の想定:
- 残す(親): 274815ec-14f4-48d2-8c9d-5dd8e10c22cf  ← 2025-08-30 の新しい方
- 移す(子): 8f3e2154-66e3-470e-9159-f905ace2d1b8  ← 2025-06-27 の古い方

やること:
1. 親ユーザーを取得
2. 子ユーザーを取得
3. 子の参加履歴を親の user_id に付け替え
   - 親に同じ schedule_id がある場合は二重登録しないで子だけ削除
4. 子ユーザー自体を削除（不要なら）
5. 親に子のメールを控えとして残す（other_emails）
"""

import os
import sys
from datetime import datetime
from typing import List

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")

# 今回は「新しい方を親」にする
PARENT_USER_ID = "274815ec-14f4-48d2-8c9d-5dd8e10c22cf"  # 残す
CHILD_USER_ID = "8f3e2154-66e3-470e-9159-f905ace2d1b8"   # こっちを移す

USER_TABLE_CANDIDATES: List[str] = [
    os.getenv("DYNAMO_USERS_TABLE"),
    os.getenv("DYNAMO_TABLE_NAME"),
    "bad-users",
    "uguis_users",
]

PARTICIPATION_TABLE_CANDIDATES: List[str] = [
    os.getenv("DYNAMO_PARTICIPATION_TABLE"),
    "bad_participation_history",
    "uguis_participations",
]

dynamodb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)


def pick_existing_table(candidates: List[str], label: str):
    for name in candidates:
        if not name:
            continue
        t = dynamodb.Table(name)
        try:
            t.load()
            print(f"[OK] {label}テーブルとして使用: {name}")
            return t
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                continue
            raise
    raise RuntimeError(f"[NG] {label}テーブルが見つかりませんでした: {candidates}")


def main():
    user_table = pick_existing_table(USER_TABLE_CANDIDATES, "ユーザー")
    part_table = pick_existing_table(PARTICIPATION_TABLE_CANDIDATES, "参加履歴")

    pk_name = user_table.key_schema[0]["AttributeName"]

    # 親・子の取得
    try:
        parent_res = user_table.get_item(Key={pk_name: PARENT_USER_ID})
        child_res = user_table.get_item(Key={pk_name: CHILD_USER_ID})
    except ClientError as e:
        print(f"[ERR] ユーザー取得に失敗しました: {e}")
        sys.exit(1)

    parent_item = parent_res.get("Item")
    child_item = child_res.get("Item")

    if not parent_item:
        print(f"[ERR] 親ユーザーが見つかりません: {PARENT_USER_ID}")
        sys.exit(1)
    if not child_item:
        print(f"[ERR] 子ユーザーが見つかりません: {CHILD_USER_ID}")
        sys.exit(1)

    print("\n--- マージ前 ---")
    print("親(残す):", parent_item)
    print("子(移す):", child_item)

    # 親に子のメールを控えとして持たせる
    parent_email = parent_item.get("email")
    child_email = child_item.get("email")
    updated_parent = dict(parent_item)

    if child_email and child_email != parent_email:
        other_emails = updated_parent.get("other_emails", [])
        if child_email not in other_emails:
            other_emails.append(child_email)
        updated_parent["other_emails"] = other_emails

    updated_parent["updated_at"] = datetime.now().isoformat()

    try:
        user_table.put_item(Item=updated_parent)
        print("[OK] 親ユーザー情報を更新しました")
    except ClientError as e:
        print(f"[ERR] 親ユーザー更新に失敗しました: {e}")
        sys.exit(1)

    # 親がすでに持っている参加履歴の schedule_id を先に集めておく
    try:
        resp_parent_parts = part_table.query(
            KeyConditionExpression=Key("user_id").eq(PARENT_USER_ID)
        )
        parent_parts = resp_parent_parts.get("Items", [])
        parent_schedule_ids = {p["schedule_id"] for p in parent_parts}
        print(f"[INFO] 親ユーザーの既存参加履歴: {len(parent_parts)}件")
    except ClientError as e:
        print(f"[ERR] 親の参加履歴取得に失敗しました: {e}")
        sys.exit(1)

    # 子の参加履歴を取得
    try:
        resp_child_parts = part_table.query(
            KeyConditionExpression=Key("user_id").eq(CHILD_USER_ID)
        )
        child_parts = resp_child_parts.get("Items", [])
        print(f"[INFO] 子ユーザーの参加履歴: {len(child_parts)}件")
    except ClientError as e:
        print(f"[ERR] 子の参加履歴取得に失敗しました: {e}")
        sys.exit(1)

    migrated = 0
    skipped = 0

    for item in child_parts:
        old_user_id = item["user_id"]
        schedule_id = item["schedule_id"]

        # 親に同じ schedule があれば重複になるので、子の方だけ消す
        if schedule_id in parent_schedule_ids:
            try:
                part_table.delete_item(
                    Key={"user_id": old_user_id, "schedule_id": schedule_id}
                )
                print(f"[SKIP] 親に同じscheduleがあるため削除のみ: {schedule_id}")
                skipped += 1
            except ClientError as e:
                print(f"[WARN] 子の重複削除に失敗: {schedule_id} err={e}")
            continue

        # 親に付け替え
        new_item = dict(item)
        new_item["user_id"] = PARENT_USER_ID

        try:
            part_table.put_item(Item=new_item)
            part_table.delete_item(
                Key={"user_id": old_user_id, "schedule_id": schedule_id}
            )
            print(f"[OK] 参加履歴を移行: {CHILD_USER_ID} -> {PARENT_USER_ID}, schedule={schedule_id}")
            migrated += 1
        except ClientError as e:
            print(f"[ERR] 参加履歴の移行に失敗: schedule={schedule_id} err={e}")

    print(f"[RESULT] 移行件数: {migrated}件, 重複で削除のみ: {skipped}件")

    # 子ユーザー本体を削除（本当に統合するなら）
    try:
        user_table.delete_item(Key={pk_name: CHILD_USER_ID})
        print(f"[OK] 子ユーザーを削除しました: {CHILD_USER_ID}")
    except ClientError as e:
        print(f"[WARN] 子ユーザーの削除に失敗しました: {e}")

    print("\n=== 完了しました ===")
    print(f"残したユーザーID: {PARENT_USER_ID}")


if __name__ == "__main__":
    main()

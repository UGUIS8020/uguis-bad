#!/usr/bin/env python
"""
45日ルールテスト用のユーザーを2名作成するスクリプト
- ユーザー1: 途中で53日あけてリセットになる想定
- ユーザー2: 40日以内で継続する想定
"""

import os
import uuid
from datetime import datetime, timedelta
from typing import List, Tuple

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# ------------------------------------------------------------
# 環境変数読み込み
# ------------------------------------------------------------
load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")

# Flask 側と揃えておくために、まずは環境変数を見る
# なければあなたの実環境で存在している bad-* を使う
USERS_TABLE_CANDIDATES: List[str] = [
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

SCHEDULE_TABLE_CANDIDATES: List[str] = [
    os.getenv("DYNAMO_SCHEDULES_TABLE"),
    "bad_schedules",
    "uguis_schedules",
]

# ------------------------------------------------------------
# DynamoDB クライアント
# ------------------------------------------------------------
dynamodb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)


# ------------------------------------------------------------
# 共通: 存在するテーブルを上から探す
# ------------------------------------------------------------
def pick_existing_table(candidates: List[str], label: str):
    """候補の中から存在するテーブルを1つ返す。なければ例外。"""
    for name in candidates:
        if not name:
            continue
        table = dynamodb.Table(name)
        try:
            table.load()  # DescribeTable
            print(f"[OK] {label}テーブルとして使用: {name}")
            return table
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "ResourceNotFoundException":
                # 次の候補を見る
                continue
            raise  # それ以外は想定外なので上に投げる

    raise RuntimeError(f"[NG] {label}テーブルが見つかりませんでした。候補: {candidates}")


# ------------------------------------------------------------
# ユーザーItemの組み立て
# ------------------------------------------------------------
def build_user_item(pk_name: str, user_id: str, name: str, email: str, birth_date: str):
    return {
        pk_name: user_id,
        "display_name": name,
        "email": email,
        "date_of_birth": birth_date,
        "created_at": datetime.now().isoformat(),
        "administrator": False,
    }


# ------------------------------------------------------------
# スケジュールを探して、なければ作る
# ------------------------------------------------------------
def get_or_create_schedule(schedules_table, event_date: str) -> str:
    """指定日のスケジュールIDを返す。なければその場で作る。"""
    # 1) 同じ日付のものがあるか探す
    res = schedules_table.scan(
        FilterExpression="#d = :date",
        ExpressionAttributeNames={"#d": "date"},
        ExpressionAttributeValues={":date": event_date},
    )
    items = res.get("Items") or []
    if items:
        return items[0]["schedule_id"]

    # 2) なければ作る
    schedule_id = str(uuid.uuid4())
    dt = datetime.strptime(event_date, "%Y-%m-%d")
    day_of_week = ["月", "火", "水", "木", "金", "土", "日"][dt.weekday()]

    schedule_data = {
        "schedule_id": schedule_id,
        "date": event_date,
        "day_of_week": day_of_week,
        "start_time": "19:00",
        "end_time": "21:00",
        "location": "越谷市立地域スポーツセンター",
        "max_participants": 30,
        "created_at": datetime.now().isoformat(),
    }

    schedules_table.put_item(Item=schedule_data)
    print(f"  [SCHEDULE] 作成しました: {event_date} -> {schedule_id}")
    return schedule_id


# ------------------------------------------------------------
# メインロジック
# ------------------------------------------------------------
def create_test_users():
    print("=" * 80)
    print("【45日ルールテスト用ユーザー作成】")
    print("=" * 80)
    print("DynamoDBに接続中...")

    # テーブルを確定させる
    users_table = pick_existing_table(USERS_TABLE_CANDIDATES, "ユーザー")
    participations_table = pick_existing_table(PARTICIPATION_TABLE_CANDIDATES, "参加履歴")
    schedules_table = pick_existing_table(SCHEDULE_TABLE_CANDIDATES, "スケジュール")

    print("[OK] DynamoDB接続成功")

    # ユーザーPK名を取得 (例: "user#user_id" か "user_id")
    user_pk_name = users_table.key_schema[0]["AttributeName"]

    # ========================================================
    # ユーザー1: 40日以上空けるパターン
    # ========================================================
    user1_id = str(uuid.uuid4())
    user1_name = "【TEST】40日リセット太郎"
    user1_email = f"test1_{user1_id[:8]}@test.com"
    user1_birth = "1990-01-01"

    print("=" * 80)
    print("【ユーザー1作成中】")
    print("=" * 80)
    print(f"名前:   {user1_name}")
    print(f"user_id: {user1_id}")

    user1_item = build_user_item(user_pk_name, user1_id, user1_name, user1_email, user1_birth)
    users_table.put_item(Item=user1_item)
    print("[OK] ユーザー1を作成しました")

    # 日付とポイントのパターン
    user1_participations: List[Tuple[str, int, str]] = [
        ("2025-08-07", 3, "50P"),
        ("2025-08-12", 7, "100P"),
        ("2025-08-14", 5, "50P"),
        # ここで53日の空白をあける想定
        ("2025-10-06", 7, "100P"),
        ("2025-10-20", 4, "50P"),
        ("2025-10-24", 2, "20P"),
        ("2025-10-28", 6, "50P"),
    ]

    created_count = 0
    print("[INFO] 【参加履歴作成中】")
    for i, (event_date, days_before, point_label) in enumerate(user1_participations, 1):
        try:
            schedule_id = get_or_create_schedule(schedules_table, event_date)

            event_dt = datetime.strptime(event_date, "%Y-%m-%d")
            joined_dt = event_dt - timedelta(days=days_before)
            joined_at = joined_dt.strftime("%Y-%m-%dT%H:%M:%S.000000")

            participations_table.put_item(
                Item=
                {
                  "user_id": "...",
                  "schedule_id": "...",
                  "date": "2025-10-06",
                  "joined_at": "2025-09-29T00:00:00.000000",
                  "status": "confirmed",
                  "location": "越谷市立地域スポーツセンター"
                }
            )
            status = "OK"
            created_count += 1
        except ClientError as e:
            status = f"NG: {e.response['Error']['Code']}"

        if i == 3:
            print(f"  {i}. {event_date} - {days_before}日前登録 ({point_label}) {status}")
            print("      ↓")
            print("  【53日の空白期間】← 45日ルール発動想定")
            print("      ↓")
        else:
            print(f"  {i}. {event_date} - {days_before}日前登録 ({point_label}) {status}")

    print(f"[RESULT] ユーザー1 参加履歴: {created_count}/{len(user1_participations)}件 作成")

    # ========================================================
    # ユーザー2: 40日以内で継続するパターン (中学生想定)
    # ========================================================
    user2_id = str(uuid.uuid4())
    user2_name = "【TEST】正常継続花子"
    user2_email = f"test2_{user2_id[:8]}@test.com"
    user2_birth = "2012-04-01"

    print("=" * 80)
    print("【ユーザー2作成中】")
    print("=" * 80)
    print(f"名前:   {user2_name}")
    print(f"user_id: {user2_id}")

    user2_item = build_user_item(user_pk_name, user2_id, user2_name, user2_email, user2_birth)
    users_table.put_item(Item=user2_item)
    print("[OK] ユーザー2を作成しました（中学生想定）")

    user2_participations: List[Tuple[str, int, str]] = [
        ("2025-09-07", 7, "100P→50P"),
        ("2025-09-08", 6, "50P→25P"),
        ("2025-09-11", 7, "100P→50P"),
        ("2025-09-16", 8, "100P→50P"),
        ("2025-09-20", 2, "20P→10P"),
        ("2025-09-22", 1, "20P→10P"),
        ("2025-09-23", 7, "100P→50P"),
        ("2025-09-25", 5, "50P→25P"),
        ("2025-09-26", 7, "100P→50P"),
        ("2025-09-29", 4, "50P→25P"),
    ]

    created_count = 0
    print("[INFO] 【参加履歴作成中】")
    for i, (event_date, days_before, point_label) in enumerate(user2_participations, 1):
        try:
            schedule_id = get_or_create_schedule(schedules_table, event_date)

            event_dt = datetime.strptime(event_date, "%Y-%m-%d")
            joined_dt = event_dt - timedelta(days=days_before)
            joined_at = joined_dt.strftime("%Y-%m-%dT%H:%M:%S.000000")

            participations_table.put_item(
                Item={
                    "user_id": user2_id,
                    "schedule_id": schedule_id,
                    "date": event_date,
                    "joined_at": joined_at,
                    "status": "confirmed",
                    "location": "越谷市立地域スポーツセンター",
                }
            )
            status = "OK"
            created_count += 1
        except ClientError as e:
            status = f"NG: {e.response['Error']['Code']}"

        print(f"  {i:2d}. {event_date} - {days_before}日前登録 ({point_label}) {status}")

    print(f"[RESULT] ユーザー2 参加履歴: {created_count}/{len(user2_participations)}件 作成")

    print("=" * 80)
    print("作成完了")
    print("=" * 80)

    return {
        "user1": {
            "user_id": user1_id,
            "name": user1_name,
            "url": f"http://localhost:5000/uguu/user/{user1_id}",
        },
        "user2": {
            "user_id": user2_id,
            "name": user2_name,
            "url": f"http://localhost:5000/uguu/user/{user2_id}",
        },
    }


# ------------------------------------------------------------
# エントリーポイント
# ------------------------------------------------------------
if __name__ == "__main__":
    print("テストユーザー作成スクリプトを起動します")
    result = create_test_users()
    if result:
        print("作成されたユーザーのURL:")
        print(f"  ユーザー1: {result['user1']['url']}")
        print(f"  ユーザー2: {result['user2']['url']}")
    else:
        print("エラーが発生しました。上のログを確認してください。")

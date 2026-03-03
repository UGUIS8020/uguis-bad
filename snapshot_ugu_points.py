"""
snapshot_ugu_points.py

目的:
    2026-03-03 時点の旧ポイントルール集計結果を、
    DynamoDB の `ugu_points` テーブルへスナップショット保存するためのスクリプト。

概要:
    - bad-users からユーザー一覧を取得
    - bad-users-history から各ユーザーの参加履歴 / 使用ポイント履歴を取得
    - 旧ルール(v1)で以下を集計
        - current_points
        - total_earned
        - total_spent
        - cumulative_count
        - current_streak
        - max_streak
        - early_registration_count
        - direct_registration_count
        - monthly_bonus_points
        - streak_points
        - participation_points
        - cumulative_bonus_points
    - 集計結果を `ugu_points` に 1ユーザー1レコードで保存

前提:
    - スナップショット基準日は `SNAPSHOT_DATE` で管理する
    - `SNAPSHOT_DATE` より後の参加履歴 / スケジュールは集計対象外
    - `ugu_points` テーブルは事前作成済みであること
    - 履歴テーブルには、参加履歴とポイント利用履歴が混在しているため、
      スクリプト内で参加履歴用 / 使用ポイント用に判別して集計する

用途:
    - 旧ルール終了時点のポイント状態を固定保存する
    - 2026-03-04 以降の新ルール計算の基準値として使う
    - ルール変更前後の比較・検証用データとして保持する

注意:
    - 本スクリプトは旧ルール(v1)の保存用
    - 新ルールの加算処理は別ロジックで行うこと
    - 再実行すると同じ user_id のレコードは上書きされる
"""

from __future__ import annotations

import boto3
from decimal import Decimal
from datetime import datetime
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from utils.timezone import JST

# あなたの既存関数を import
from uguu.point import (
    PointRules,
    normalize_participation_history,
    calc_reset_index,
    slice_records_for_points,
    calc_participation_and_cumulative,
    calc_monthly_bonus,
    calc_streak_points,
    get_point_multiplier,
)

AWS_REGION = "ap-northeast-1"

USERS_TABLE = "bad-users"
POINTS_TABLE = "ugu_points"
HISTORY_TABLE = "bad-users-history"
SCHEDULES_TABLE = "bad_schedules"

SNAPSHOT_DATE = "2026-03-03"
SNAPSHOT_RULE_VERSION = "v1"
CURRENT_RULE_VERSION = "v1"

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
users_table = dynamodb.Table(USERS_TABLE)
points_table = dynamodb.Table(POINTS_TABLE)
history_table = dynamodb.Table(HISTORY_TABLE)
schedules_table = dynamodb.Table(SCHEDULES_TABLE)


# def debug_one_user_history(user_id: str):
#     resp = history_table.query(
#         KeyConditionExpression=boto3.dynamodb.conditions.Key("user_id").eq(user_id)
#     )
#     items = resp.get("Items", [])
#     print(f"[DEBUG history] user_id={user_id} count={len(items)}")
#     for x in items[:10]:
#         print(x)


def to_int(value, default=0):
    try:
        if value is None:
            return default
        if isinstance(value, Decimal):
            return int(value)
        return int(value)
    except Exception:
        return default


def scan_all(table, **kwargs):
    items = []
    resp = table.scan(**kwargs)
    items.extend(resp.get("Items", []))

    while "LastEvaluatedKey" in resp:
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))

    return items


def get_all_users():
    items = scan_all(users_table)
    out = []
    for u in items:
        user_id = u.get("user#user_id") or u.get("user_id")
        if not user_id:
            continue
        out.append(u)
    return out


def get_user_history(user_id: str):
    resp = history_table.query(
        KeyConditionExpression=Key("user_id").eq(user_id)
    )
    items = resp.get("Items", [])

    # 1件だけ spend 履歴を確認
    for x in items:
        marker = str(
            x.get("history_id")
            or x.get("sk")
            or x.get("registered_at")
            or x.get("joined_at")
            or x.get("created_at")
            or ""
        )
        if "points#spend#" in marker:
            print("[DEBUG spend item]", x)
            break

    return items


def get_all_schedules():
    items = scan_all(schedules_table)
    schedules = []

    for s in items:
        schedule_date = s.get("date") or s.get("event_date")
        if not schedule_date:
            continue
        schedules.append({"date": schedule_date})

    schedules.sort(key=lambda x: x["date"])
    return schedules


def build_participation_history_from_history_items(history_items):
    raw_history = []

    for h in history_items:
        action = h.get("action")
        status = h.get("status", "registered")

        event_date = h.get("event_date") or h.get("date")
        registered_at = h.get("registered_at") or h.get("joined_at") or h.get("created_at")

        if not event_date or not registered_at:
            continue

        reg_str = str(registered_at)

        if reg_str.startswith("points#earn#") or reg_str.startswith("points#spend#"):
            continue

        raw_history.append({
            "event_date": event_date,
            "registered_at": registered_at,
            "status": status,
            "action": action,
        })

    return raw_history


def sum_total_spent(history_items):
    total_spent = 0

    for h in history_items:
        marker = str(
            h.get("joined_at")
            or h.get("history_id")
            or h.get("sk")
            or h.get("created_at")
            or ""
        )

        kind = str(h.get("kind") or "").lower()

        raw_points = (
            h.get("points_used")
            or h.get("points")
            or h.get("point")
            or h.get("amount")
            or h.get("delta_points")
            or 0
        )

        try:
            points = int(raw_points)
        except Exception:
            try:
                points = int(float(raw_points))
            except Exception:
                points = 0

        if kind == "spend" or "points#spend#" in marker:
            total_spent += abs(points)

    return total_spent


def is_early_registration_fn(item):
    """
    既存の is_early_registration があるならそれを使ってください。
    仮実装:
      - 早期登録: 100
      - 通常登録: 50
      - その他: 10
    """
    event_date = item["event_date"]
    registered_at = item["registered_at"]

    if isinstance(event_date, datetime):
        event_date = event_date.strftime("%Y-%m-%d")
    if isinstance(registered_at, datetime):
        registered_at = registered_at.strftime("%Y-%m-%d %H:%M:%S")

    try:
        event_dt = datetime.strptime(event_date, "%Y-%m-%d")
        reg_dt = datetime.strptime(registered_at, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return 10

    diff_days = (event_dt.date() - reg_dt.date()).days
    if diff_days >= 1:
        return 100
    return 50


def calc_user_snapshot(user_info, all_schedules):
    user_id = user_info.get("user#user_id") or user_info.get("user_id")
    gender = (user_info.get("gender") or "").lower()
    birth_date = user_info.get("date_of_birth")

    if isinstance(birth_date, str):
        birth_date = datetime.strptime(birth_date, "%Y-%m-%d").date()

    point_multiplier = 1.0
    if birth_date:
        point_multiplier = get_point_multiplier(birth_date, gender)

    print(f"[DEBUG] user_id={user_id} gender={gender} birth_date={birth_date} point_multiplier={point_multiplier}")

    history_items = get_user_history(user_id)
    print(f"[DEBUG] user_id={user_id} history_count={len(history_items)}")
    if history_items:
        print(f"[DEBUG] first_history={history_items[0]}")

    raw_history = build_participation_history_from_history_items(history_items)
    print(f"[DEBUG] user_id={user_id} raw_history_count={len(raw_history)}")
    if raw_history:
        print(f"[DEBUG] first_raw_history={raw_history[0]}")

    records_all = normalize_participation_history(raw_history)

    snapshot_cutoff = datetime.strptime(SNAPSHOT_DATE, "%Y-%m-%d").date()
    records_all = [
        r for r in records_all
        if r.event_date.date() <= snapshot_cutoff
    ]

    print(f"[DEBUG] user_id={user_id} records_all_count={len(records_all)}")
    if records_all:
        print(f"[DEBUG] first_record={records_all[0]}")

    filtered_schedules = [
        s for s in all_schedules
        if s["date"] <= SNAPSHOT_DATE
    ]
    print(f"[DEBUG] filtered_schedules_count={len(filtered_schedules)}")

    rules = PointRules()

    last_reset_index, is_reset = calc_reset_index(records_all, rules.reset_days)
    print(f"[DEBUG] user_id={user_id} last_reset_index={last_reset_index} is_reset={is_reset}")

    records_for_points = slice_records_for_points(records_all, last_reset_index)
    print(f"[DEBUG] user_id={user_id} records_for_points_count={len(records_for_points)}")

    participation_result = calc_participation_and_cumulative(
        records_all=records_all,
        records_for_points=records_for_points,
        rules=rules,
        point_multiplier=point_multiplier,
        is_early_registration_fn=is_early_registration_fn,
    )
    print(f"[DEBUG] participation_result={participation_result}")

    monthly_bonus_points, monthly_bonuses = calc_monthly_bonus(
        records_for_points=records_for_points,
        point_multiplier=point_multiplier,
    )
    print(f"[DEBUG] monthly_bonus_points={monthly_bonus_points}")
    print(f"[DEBUG] monthly_bonuses={monthly_bonuses}")

    streak_points, current_streak, max_streak, streak_start_date = calc_streak_points(
        records_for_points=records_for_points,
        all_schedules=filtered_schedules,
        rules=rules,
        point_multiplier=point_multiplier,
    )
    print(
        f"[DEBUG] streak_points={streak_points} "
        f"current_streak={current_streak} max_streak={max_streak} "
        f"streak_start_date={streak_start_date}"
    )

    total_earned = (
        participation_result["participation_points"]
        + participation_result["cumulative_bonus_points"]
        + monthly_bonus_points
        + streak_points
    )

    total_spent = sum_total_spent(history_items)
    current_points = total_earned - total_spent

    print(
        f"[DEBUG] total_earned={total_earned} "
        f"total_spent={total_spent} current_points={current_points}"
    )

    now_str = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")

    item = {
        "user_id": user_id,
        "display_name": user_info.get("display_name", ""),
        "current_points": current_points,
        "total_earned": total_earned,
        "total_spent": total_spent,
        "cumulative_count": participation_result["cumulative_count"],
        "current_streak": current_streak,
        "max_streak": max_streak,
        "early_registration_count": participation_result["early_registration_count"],
        "direct_registration_count": participation_result["direct_registration_count"],
        "monthly_bonus_points": monthly_bonus_points,
        "streak_points": streak_points,
        "participation_points": participation_result["participation_points"],
        "cumulative_bonus_points": participation_result["cumulative_bonus_points"],
        "point_multiplier": Decimal(str(point_multiplier)),
        "is_reset": is_reset,
        "snapshot_rule_version": SNAPSHOT_RULE_VERSION,
        "current_rule_version": CURRENT_RULE_VERSION,
        "snapshot_date": SNAPSHOT_DATE,
        "snapshot_at": now_str,
        "updated_at": now_str,
    }

    return item


def save_snapshot(item):
    points_table.put_item(Item=item)


def main():
    all_users = get_all_users()
    all_schedules = get_all_schedules()

    print(f"[INFO] users={len(all_users)} schedules={len(all_schedules)}")

    ok = 0
    ng = 0

    for user_info in all_users:
        user_id = user_info.get("user#user_id") or user_info.get("user_id")
        try:
            item = calc_user_snapshot(user_info, all_schedules)
            save_snapshot(item)
            ok += 1
            print(
                f"[OK] user_id={user_id} current_points={item['current_points']} "
                f"earned={item['total_earned']} spent={item['total_spent']}"
            )
        except Exception as e:
            ng += 1
            print(f"[NG] user_id={user_id} error={e}")

    print(f"[DONE] ok={ok} ng={ng}")


# def main():
#     all_schedules = get_all_schedules()

#     test_user_id = "52b9d36e-1413-49c3-8362-9130016df2d4"
#     user_info = get_one_user(test_user_id)

#     if not user_info:
#         print(f"[NG] user not found: {test_user_id}")
#         return

#     item = calc_user_snapshot(user_info, all_schedules)
#     print(item)

# def scan_all(table, **kwargs):
#     items = []
#     resp = table.scan(**kwargs)
#     items.extend(resp.get("Items", []))

#     while "LastEvaluatedKey" in resp:
#         kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
#         resp = table.scan(**kwargs)
#         items.extend(resp.get("Items", []))

#     return items


# def get_all_schedules():
#     items = scan_all(schedules_table)
#     schedules = []

#     for s in items:
#         schedule_date = s.get("date") or s.get("event_date")
#         if not schedule_date:
#             continue
#         schedules.append({"date": schedule_date})

#     schedules.sort(key=lambda x: x["date"])
#     return schedules


# def get_one_user(user_id: str):
#     resp = users_table.get_item(Key={"user#user_id": user_id})
#     return resp.get("Item")


if __name__ == "__main__":
    main()
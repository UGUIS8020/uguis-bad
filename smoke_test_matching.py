#!/usr/bin/env python
"""
マッチングシステムの自動スモークテスト

app.py の Flask アプリ（app.test_client）を通して、実際のルート
（/game/entry, /game/create_pairings, /game/create_pairings_skilled,
/game/submit_score, /game/finish_current_match）を本番と同じ DynamoDB に
対して呼び出し、チェックイン→組み合わせ作成→スコア入力→試合終了、を
複数ラウンド走らせてエラーが出ないか確認する。

このプロジェクトにはテスト専用の別DB環境が無く、ローカルのFlaskも本番と
同じテーブル（bad-users, bad-game-matches 等）を見に行く。そのため:
  - 実際の練習が進行中（本物の参加者がエントリー済み）なら安全のため中断する
  - 作成するユーザー・エントリー・試合結果はすべて「【TEST_SMOKE】」プレフィックス
    で明示し、終了後に完全に削除する
  - meta#current / meta#pairing はテスト開始前の状態をスナップショットし、
    終了後に そのまま書き戻して復元する

使い方:
  python smoke_test_matching.py --yes                  # 12人 x 3ラウンド(random/full_random/ai) + skilled_ai 1回
  python smoke_test_matching.py --yes --players 8 --courts 2
  python smoke_test_matching.py --cleanup-only --yes    # 前回の異常終了などで残ったテストデータだけ掃除する
"""
import argparse
import os
import random
import sys
import traceback
import uuid
from decimal import Decimal

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

import boto3
from boto3.dynamodb.conditions import Attr, Key

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

TEST_PREFIX = "【TEST_SMOKE】"
REGION = os.getenv("AWS_REGION", "ap-northeast-1")


def get_dynamodb():
    return boto3.resource("dynamodb", region_name=REGION)


def scan_all(table, **kwargs):
    items = []
    resp = table.scan(**kwargs)
    items.extend(resp.get("Items", []))
    while resp.get("LastEvaluatedKey"):
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"], **kwargs)
        items.extend(resp.get("Items", []))
    return items


def check_safe_to_run(dynamodb):
    """本物の参加者がエントリー中でないか確認する。進行中なら中断する。"""
    entry_table = dynamodb.Table("bad-game-match_entries")
    items = scan_all(entry_table, FilterExpression=Attr("entry_status").is_in(["playing", "pending", "resting"]))

    non_test = [i for i in items if not str(i.get("display_name", "")).startswith(TEST_PREFIX)]
    if non_test:
        names = sorted({str(i.get("display_name", "?")) for i in non_test})
        print(f"[中断] 実際の参加者がすでにエントリーしています: {names}")
        print("       実際の練習中とみなし、安全のため実行しません。")
        return False

    meta_table = dynamodb.Table("bad-game-matches")
    meta_current = meta_table.get_item(Key={"match_id": "meta#current"}, ConsistentRead=True).get("Item", {}) or {}
    if meta_current.get("status") == "playing":
        print("[中断] meta#current が playing 状態です。実際の練習中の可能性があるため実行しません。")
        return False

    return True


def cleanup_test_data(dynamodb, verbose=True):
    """【TEST_SMOKE】関連のデータをすべて削除する。"""
    def log(msg):
        if verbose:
            print(msg)

    users_table = dynamodb.Table("bad-users")
    entry_table = dynamodb.Table("bad-game-match_entries")
    results_table = dynamodb.Table("bad-game-results")
    history_table = dynamodb.Table("bad-users-history")

    test_users = scan_all(users_table, FilterExpression=Attr("display_name").begins_with(TEST_PREFIX))
    test_uids = [u.get("user#user_id") for u in test_users if u.get("user#user_id")]
    log(f"[cleanup] テストユーザー: {len(test_users)}件")

    test_entries = scan_all(entry_table, FilterExpression=Attr("display_name").begins_with(TEST_PREFIX))
    for e in test_entries:
        entry_table.delete_item(Key={"entry_id": e["entry_id"]})
    log(f"[cleanup] match_entries: {len(test_entries)}件削除")

    test_result_ids = set()
    all_results = scan_all(results_table)
    for r in all_results:
        a_names = [p.get("display_name", "") for p in (r.get("team_a") or [])]
        b_names = [p.get("display_name", "") for p in (r.get("team_b") or [])]
        if any(str(n).startswith(TEST_PREFIX) for n in a_names + b_names):
            test_result_ids.add(r["result_id"])

    for rid in test_result_ids:
        results_table.delete_item(Key={"result_id": rid})
    log(f"[cleanup] game-results: {len(test_result_ids)}件削除")

    hist_count = 0
    for uid in test_uids:
        items = history_table.query(KeyConditionExpression=Key("user_id").eq(uid)).get("Items", [])
        for h in items:
            history_table.delete_item(Key={"user_id": uid, "joined_at": h["joined_at"]})
            hist_count += 1
    log(f"[cleanup] users-history: {hist_count}件削除")

    for u in test_users:
        users_table.delete_item(Key={"user#user_id": u["user#user_id"]})
    log(f"[cleanup] users: {len(test_users)}件削除")


def create_test_users(dynamodb, n_players):
    users_table = dynamodb.Table("bad-users")
    users = []

    admin_uid = str(uuid.uuid4())
    users_table.put_item(Item={
        "user#user_id": admin_uid,
        "display_name": f"{TEST_PREFIX}admin",
        "user_name": "smoke_admin",
        "email": f"smoke_admin_{admin_uid[:8]}@test.invalid",
        "gender": "male",
        "administrator": True,
        "skill_score": Decimal("50"),
        "skill_sigma": Decimal("8.333"),
    })
    users.append({"user_id": admin_uid, "is_admin": True})

    for i in range(n_players):
        uid = str(uuid.uuid4())
        skill_score = Decimal(str(random.randint(15, 70)))
        gender = "male" if i % 2 == 0 else "female"
        users_table.put_item(Item={
            "user#user_id": uid,
            "display_name": f"{TEST_PREFIX}選手{i+1:02d}",
            "user_name": f"smoke_player_{i+1}",
            "email": f"smoke_player_{i+1}_{uid[:8]}@test.invalid",
            "gender": gender,
            "administrator": False,
            "skill_score": skill_score,
            "skill_sigma": Decimal("8.333"),
        })
        users.append({"user_id": uid, "is_admin": False})

    return users


def make_client_for_user(flask_app, user_id):
    client = flask_app.test_client()
    with client.session_transaction() as sess:
        sess["_user_id"] = user_id
        sess["_fresh"] = True
    return client


def run_round(admin_client, round_label, max_courts, route="/game/create_pairings"):
    print(f"\n--- ラウンド: {round_label} ---")
    resp = admin_client.post(route, data={"max_courts": str(max_courts)}, follow_redirects=False)
    if resp.status_code not in (302, 200):
        print(f"[NG] {route} status={resp.status_code} body={resp.get_data(as_text=True)[:300]}")
        return False

    dynamodb = get_dynamodb()
    meta_table = dynamodb.Table("bad-game-matches")
    meta_current = meta_table.get_item(Key={"match_id": "meta#current"}, ConsistentRead=True).get("Item", {}) or {}

    if meta_current.get("status") != "playing":
        print(f"[NG] create_pairings後もmeta#current.status=playingになっていません: {meta_current}")
        return False

    match_id = meta_current.get("current_match_id")
    court_count = int(meta_current.get("court_count", 0))
    print(f"  match_id={match_id} court_count={court_count} mode={meta_current.get('pairing_mode')}")

    entry_table = dynamodb.Table("bad-game-match_entries")
    entries = scan_all(entry_table, FilterExpression=Attr("match_id").eq(str(match_id)))
    by_court = {}
    for e in entries:
        by_court.setdefault(int(e["court_number"]), []).append(e)

    all_ok = True
    for court_num in range(1, court_count + 1):
        court_entries = by_court.get(court_num, [])
        names = [(e.get("display_name"), e.get("team")) for e in court_entries]
        if len(court_entries) != 4:
            print(f"  [NG] court{court_num}: エントリー数が4人ではありません: {names}")
            all_ok = False
            continue

        t1, t2 = (21, random.randint(10, 19))
        resp = admin_client.post(
            f"/game/submit_score/{match_id}/court/{court_num}",
            data={"team1_score": str(t1), "team2_score": str(t2)},
        )
        status = "OK" if resp.status_code == 200 else f"NG({resp.status_code}) {resp.get_data(as_text=True)[:200]}"
        print(f"  court{court_num}: {names} -> {t1}-{t2}  [{status}]")
        if resp.status_code != 200:
            all_ok = False

    resp = admin_client.post("/game/finish_current_match")
    if resp.status_code not in (200, 302):
        print(f"[NG] finish_current_match status={resp.status_code} body={resp.get_data(as_text=True)[:300]}")
        all_ok = False
    else:
        print("  finish_current_match: OK")

    meta_current_after = meta_table.get_item(Key={"match_id": "meta#current"}, ConsistentRead=True).get("Item", {}) or {}
    if meta_current_after.get("status") != "idle":
        print(f"[NG] finish後もidleに戻っていません: {meta_current_after}")
        all_ok = False

    return all_ok


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--players", type=int, default=12, help="テスト参加者数(4の倍数推奨)")
    parser.add_argument("--courts", type=int, default=3, help="最大コート数")
    parser.add_argument("--yes", action="store_true", help="本番DBに対して実行することを確認済み")
    parser.add_argument("--cleanup-only", action="store_true", help="残留テストデータの削除だけ行う")
    args = parser.parse_args()

    if not args.yes:
        print("本番と同じDynamoDBに対して実行します。内容を確認の上 --yes を付けて実行してください。")
        sys.exit(1)

    dynamodb = get_dynamodb()

    if args.cleanup_only:
        cleanup_test_data(dynamodb)
        print("\n[完了] クリーンアップのみ実施しました。")
        return

    if not check_safe_to_run(dynamodb):
        sys.exit(1)

    # 前回の残骸があれば先に掃除
    cleanup_test_data(dynamodb, verbose=False)

    meta_table = dynamodb.Table("bad-game-matches")
    snapshot_current = meta_table.get_item(Key={"match_id": "meta#current"}, ConsistentRead=True).get("Item")
    snapshot_pairing = meta_table.get_item(Key={"match_id": "meta#pairing"}, ConsistentRead=True).get("Item")

    from app import app as flask_app
    flask_app.config["WTF_CSRF_ENABLED"] = False
    flask_app.config["TESTING"] = True
    # login_manager.session_protection="strong" は、手動でセッションに
    # _user_id を注入するだけだと指紋(_id)が無いために強制ログアウトさせる。
    # このテスト実行中(このプロセスのみ)だけ無効化する。
    flask_app.login_manager.session_protection = None

    import logging as _logging
    _logging.getLogger("app").setLevel(_logging.WARNING)

    overall_ok = True
    users = []
    try:
        print(f"\n[setup] テストユーザー作成中... ({args.players}人 + 管理者1人)")
        users = create_test_users(dynamodb, args.players)
        admin_user = next(u for u in users if u["is_admin"])
        player_users = [u for u in users if not u["is_admin"]]

        admin_client = make_client_for_user(flask_app, admin_user["user_id"])

        print("[setup] チェックイン (/game/entry) 実行中...")
        for u in player_users:
            c = make_client_for_user(flask_app, u["user_id"])
            resp = c.post("/game/entry", follow_redirects=False)
            if resp.status_code not in (200, 302):
                print(f"  [NG] entry user={u['user_id']} status={resp.status_code}")
                overall_ok = False
        # 管理者自身もプレイヤーとしてチェックインさせる（人数合わせ）
        resp = admin_client.post("/game/entry", follow_redirects=False)

        rounds = [
            ("1回目 (cycle_index想定=random)", "/game/create_pairings"),
            ("2回目 (cycle_index想定=full_random)", "/game/create_pairings"),
            ("3回目 (cycle_index想定=ai)", "/game/create_pairings"),
            ("4回目 (skilled_ai)", "/game/create_pairings_skilled"),
        ]

        for label, route in rounds:
            ok = run_round(admin_client, label, args.courts, route=route)
            overall_ok = overall_ok and ok
            if not ok:
                print(f"[警告] {label} で異常を検出しました。続行します。")

            # 次ラウンドのため、pending に戻った選手を再度 court にとどめておく必要はない
            # (finish_current_match で自動的に pending 復帰済み)

        print("\n" + "=" * 60)
        if overall_ok:
            print("[結果] 異常は検出されませんでした。")
        else:
            print("[結果] 異常を検出しました。上のログを確認してください。")
        print("=" * 60)

    except Exception:
        overall_ok = False
        print("\n[例外発生]")
        traceback.print_exc()

    finally:
        print("\n[teardown] テストデータを削除し、meta状態を復元します...")
        try:
            cleanup_test_data(dynamodb)
        except Exception:
            print("[teardown] cleanup_test_data でエラー:")
            traceback.print_exc()

        try:
            if snapshot_current is not None:
                meta_table.put_item(Item=snapshot_current)
            if snapshot_pairing is not None:
                meta_table.put_item(Item=snapshot_pairing)
            print("[teardown] meta#current / meta#pairing を実行前の状態に復元しました。")
        except Exception:
            print("[teardown] meta復元でエラー:")
            traceback.print_exc()

    sys.exit(0 if overall_ok else 2)


if __name__ == "__main__":
    main()

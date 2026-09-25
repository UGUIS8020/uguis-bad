#!/usr/bin/env python
"""
テストコート（game2）の自動スモークテスト

既存の smoke_test_matching.py と同じやり方で、app.test_client() を通して
実際の /game2 ルートを叩く。game2は専用テーブル（bad-game2-match_entries,
bad-game2-results）を使うので、既存の本番コート（bad-game-match_entries等）
には基本的に影響しない。

ただし「緊急: 全員を通常コートに移動」機能だけは本物の
bad-game-match_entries に書き込むので、そのテストは本物の参加者がいない
時にだけ行い、テスト後は追加したデータを削除する。

使い方:
  python smoke_test_game2.py --yes
  python smoke_test_game2.py --yes --players 8 --courts 2
  python smoke_test_game2.py --cleanup-only --yes
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

TEST_PREFIX = "【TEST_GAME2】"
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
    """本番コートに本物の参加者がいないか確認する(緊急移動テストが書き込むため)"""
    entry_table = dynamodb.Table("bad-game-match_entries")
    items = scan_all(entry_table, FilterExpression=Attr("entry_status").is_in(["playing", "pending", "resting"]))
    non_test = [i for i in items if not str(i.get("display_name", "")).startswith(("【TEST_SMOKE】", TEST_PREFIX))]
    if non_test:
        names = sorted({str(i.get("display_name", "?")) for i in non_test})
        print(f"[中断] 本番コートに実際の参加者がいます: {names}。安全のため実行しません。")
        return False
    return True


def cleanup_test_data(dynamodb, verbose=True):
    def log(msg):
        if verbose:
            print(msg)

    users_table = dynamodb.Table("bad-users")
    g2_entry_table = dynamodb.Table("bad-game2-match_entries")
    g2_results_table = dynamodb.Table("bad-game2-results")
    main_entry_table = dynamodb.Table("bad-game-match_entries")
    history_table = dynamodb.Table("bad-users-history")

    test_users = scan_all(users_table, FilterExpression=Attr("display_name").begins_with(TEST_PREFIX))
    test_uids = [u.get("user#user_id") for u in test_users if u.get("user#user_id")]

    g2_entries = scan_all(g2_entry_table, FilterExpression=Attr("display_name").begins_with(TEST_PREFIX))
    for e in g2_entries:
        g2_entry_table.delete_item(Key={"entry_id": e["entry_id"]})
    log(f"[cleanup] game2 match_entries: {len(g2_entries)}件削除")

    # 緊急移動テストで本番テーブルに移されたテストエントリーも削除
    main_entries = scan_all(main_entry_table, FilterExpression=Attr("display_name").begins_with(TEST_PREFIX))
    for e in main_entries:
        main_entry_table.delete_item(Key={"entry_id": e["entry_id"]})
    log(f"[cleanup] 本番 match_entries (緊急移動分): {len(main_entries)}件削除")

    all_results = scan_all(g2_results_table)
    del_count = 0
    for r in all_results:
        a_names = [p.get("display_name", "") for p in (r.get("team_a") or [])]
        b_names = [p.get("display_name", "") for p in (r.get("team_b") or [])]
        if any(str(n).startswith(TEST_PREFIX) for n in a_names + b_names):
            g2_results_table.delete_item(Key={"result_id": r["result_id"]})
            del_count += 1
    log(f"[cleanup] game2 results: {del_count}件削除")

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
        "user_name": "smoke_g2_admin",
        "email": f"smoke_g2_admin_{admin_uid[:8]}@test.invalid",
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
            "user_name": f"smoke_g2_player_{i+1}",
            "email": f"smoke_g2_player_{i+1}_{uid[:8]}@test.invalid",
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


def get_courts_state(dynamodb):
    """現在playing中の全エントリーを、コート番号ごとにグルーピングして返す"""
    entry_table = dynamodb.Table("bad-game2-match_entries")
    playing = scan_all(entry_table, FilterExpression=Attr("entry_status").eq("playing"), ConsistentRead=True)
    by_court = {}
    for e in playing:
        c = int(e["court_number"])
        by_court.setdefault(c, {"match_id": e.get("match_id"), "entries": []})
        by_court[c]["entries"].append(e)
    return by_court


def seed_first_round(admin_client, max_courts):
    print(f"\n--- 最初の組み合わせ作成 (courts={max_courts}) ---")
    resp = admin_client.post("/game2/create_pairings", data={"max_courts": str(max_courts)}, follow_redirects=False)
    if resp.status_code not in (302, 200):
        print(f"[NG] create_pairings status={resp.status_code} body={resp.get_data(as_text=True)[:300]}")
        return False

    dynamodb = get_dynamodb()
    courts = get_courts_state(dynamodb)
    print(f"  作成されたコート数: {len(courts)}")
    for c, info in sorted(courts.items()):
        names = [(e.get("display_name"), e.get("team")) for e in info["entries"]]
        print(f"  court{c}: match_id={info['match_id']} {names}")
    return len(courts) > 0


def submit_and_check_refill(admin_client, court_num, old_match_id):
    """1コート分のスコアを送信し、そのコートだけ自動で次の試合に切り替わるか確認する"""
    dynamodb = get_dynamodb()
    t1, t2 = (21, random.randint(10, 19))
    resp = admin_client.post(
        f"/game2/submit_score/{old_match_id}/court/{court_num}",
        data={"team1_score": str(t1), "team2_score": str(t2)},
        follow_redirects=False,
    )
    if resp.status_code not in (200, 302):
        print(f"  [NG] court{court_num} submit_score status={resp.status_code} body={resp.get_data(as_text=True)[:200]}")
        return False
    print(f"  court{court_num}: {t1}-{t2} 送信 [OK]")

    courts_after = get_courts_state(dynamodb)
    info = courts_after.get(court_num)
    if not info:
        print(f"  [NG] court{court_num} が補充後に消えています（空いたまま候補不足の可能性）")
        return False
    if info["match_id"] == old_match_id:
        print(f"  [NG] court{court_num} のmatch_idが変わっていません（自動補充されていない）")
        return False

    names = [(e.get("display_name"), e.get("team")) for e in info["entries"]]
    print(f"  court{court_num}: 自動補充後 match_id={info['match_id']} {names} [OK]")
    return True


def run_continuous_rounds(admin_client, rounds):
    """全コートで順番にスコアを送信し、毎回そのコートだけ自動補充されるか確認する"""
    all_ok = True
    dynamodb = get_dynamodb()
    for i in range(rounds):
        courts = get_courts_state(dynamodb)
        if not courts:
            print(f"\n[情報] {i+1}周目: 進行中のコートが無いため終了（人数不足の可能性）")
            break
        print(f"\n--- 連続マッチング {i+1}周目 (コート{len(courts)}面) ---")
        for court_num, info in sorted(courts.items()):
            ok = submit_and_check_refill(admin_client, court_num, info["match_id"])
            all_ok = all_ok and ok
    return all_ok


def test_emergency_transfer(flask_app, admin_client, dynamodb):
    print("\n--- 緊急脱出ボタンのテスト ---")
    resp = admin_client.post("/game2/emergency_transfer", follow_redirects=False)
    if resp.status_code not in (200, 302):
        print(f"[NG] emergency_transfer status={resp.status_code} body={resp.get_data(as_text=True)[:300]}")
        return False

    g2_entry_table = dynamodb.Table("bad-game2-match_entries")
    remaining = scan_all(g2_entry_table, FilterExpression=Attr("display_name").begins_with(TEST_PREFIX))
    if remaining:
        print(f"[NG] 緊急移動後もgame2側にエントリーが残っています: {len(remaining)}件")
        return False

    main_entry_table = dynamodb.Table("bad-game-match_entries")
    moved = scan_all(main_entry_table, FilterExpression=Attr("display_name").begins_with(TEST_PREFIX) & Attr("entry_status").eq("pending"))
    print(f"  本番コート側に移動されたテストユーザー: {len(moved)}人")
    if not moved:
        print("[NG] 本番コート側に移動されたエントリーが見つかりません")
        return False

    print("  emergency_transfer: OK")
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--players", type=int, default=8)
    parser.add_argument("--courts", type=int, default=2)
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--cleanup-only", action="store_true")
    args = parser.parse_args()

    if not args.yes:
        print("本番と同じAWSに対して実行します。内容を確認の上 --yes を付けて実行してください。")
        sys.exit(1)

    dynamodb = get_dynamodb()

    if args.cleanup_only:
        cleanup_test_data(dynamodb)
        print("\n[完了] クリーンアップのみ実施しました。")
        return

    if not check_safe_to_run(dynamodb):
        sys.exit(1)

    cleanup_test_data(dynamodb, verbose=False)

    from app import app as flask_app
    flask_app.config["WTF_CSRF_ENABLED"] = False
    flask_app.config["TESTING"] = True
    flask_app.login_manager.session_protection = None

    import logging as _logging
    _logging.getLogger("app").setLevel(_logging.WARNING)

    overall_ok = True
    try:
        print(f"\n[setup] テストユーザー作成中... ({args.players}人 + 管理者1人)")
        users = create_test_users(dynamodb, args.players)
        admin_user = next(u for u in users if u["is_admin"])
        player_users = [u for u in users if not u["is_admin"]]

        admin_client = make_client_for_user(flask_app, admin_user["user_id"])

        print("[setup] チェックイン (/game2/entry) 実行中...")
        for u in player_users:
            c = make_client_for_user(flask_app, u["user_id"])
            resp = c.post("/game2/entry", follow_redirects=False)
            if resp.status_code not in (200, 302):
                print(f"  [NG] entry user={u['user_id']} status={resp.status_code}")
                overall_ok = False
        admin_client.post("/game2/entry", follow_redirects=False)

        ok = seed_first_round(admin_client, args.courts)
        overall_ok = overall_ok and ok

        ok = run_continuous_rounds(admin_client, rounds=3)
        overall_ok = overall_ok and ok
        if not ok:
            print("[警告] 連続マッチングで異常を検出しました。続行します。")

        ok = test_emergency_transfer(flask_app, admin_client, dynamodb)
        overall_ok = overall_ok and ok

        print("\n" + "=" * 60)
        print("[結果] 異常は検出されませんでした。" if overall_ok else "[結果] 異常を検出しました。上のログを確認してください。")
        print("=" * 60)

    except Exception:
        overall_ok = False
        print("\n[例外発生]")
        traceback.print_exc()

    finally:
        print("\n[teardown] テストデータを削除します...")
        try:
            cleanup_test_data(dynamodb)
        except Exception:
            print("[teardown] cleanup_test_data でエラー:")
            traceback.print_exc()

    sys.exit(0 if overall_ok else 2)


if __name__ == "__main__":
    main()

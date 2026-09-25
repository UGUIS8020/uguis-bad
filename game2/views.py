"""
テストコート（game2）: 既存のマッチングシステム（game/views.py）とは
完全に独立した並行実装。

- 参加者データは専用テーブル（bad-game2-match_entries, bad-game2-results）に
  保存し、既存の bad-game-match_entries / bad-game-results には一切触れない。
- 進行状態は bad-game-matches テーブルに meta#continuous_current /
  meta#continuous_pairing / meta#continuous_rest_queue という別キーで保存する
  （既存の meta#current / meta#pairing / meta#rest_queue とはキーが違うので
  衝突しない）。
- スキル値（skill_score/skill_sigma）だけは bad-users を共有する（同じ人の
  実力なので、どちらのコートで試合しても更新されるのが自然なため）。
- 何か問題が起きた場合、管理者は「緊急: 全員を通常コートへ移動」ボタンで
  テストコートの参加者全員を既存システムの待機列（pending）に移せる。

Phase 1: 既存システムと同じ「全コート一括進行」の動きをこのテーブル上で
再現する（コート単位の連続マッチングはPhase 2で追加予定）。
"""
from flask import Blueprint, render_template, redirect, url_for, flash, request, current_app, jsonify
from flask_login import login_required, current_user
from datetime import datetime
from decimal import Decimal
import uuid
import random
import logging

from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from game.game_utils import (
    Player,
    generate_ai_best_pairings,
    generate_balanced_pairs_and_matches,
    generate_full_random_pairings,
    update_trueskill_for_players_and_return_updates,
)
from game.views import (
    persist_skill_to_bad_users,
    _pick_waiters_by_rest_queue,
)
from utils.timezone import JST

logger = logging.getLogger(__name__)

bp_game2 = Blueprint('game2', __name__)

REST_QUEUE_KEY = "continuous_rest_queue"
META_CURRENT_PK = "meta#continuous_current"
META_PAIRING_PK = "meta#continuous_pairing"


def _entry_table():
    return current_app.dynamodb.Table("bad-game2-match_entries")


def _results_table():
    return current_app.dynamodb.Table("bad-game2-results")


def _meta_table():
    return current_app.dynamodb.Table("bad-game-matches")


def generate_match_id2():
    """試合ID生成（既存システムのIDと絶対に衝突しないよう g2_ を付ける）"""
    now = datetime.now()
    match_id = "g2_" + now.strftime("%Y%m%d_%H%M%S")
    current_app.logger.info(f"[game2] 生成された試合ID: {match_id}")
    return match_id


def has_ongoing_matches2():
    """テストコート側で進行中の試合があるか（既存システムとは独立に判定）"""
    try:
        entry_table = _entry_table()
        resp = entry_table.scan(FilterExpression=Attr("entry_status").eq("playing"))
        return len(resp.get("Items", [])) > 0
    except Exception as e:
        current_app.logger.error(f"[game2] 進行中試合チェックエラー: {str(e)}")
        return False


def sync_match_entries_with_updated_skills2(entry_mapping, updated_skills):
    """更新されたスキルスコアで bad-game2-match_entries を同期する"""
    entry_table = _entry_table()
    sync_count = 0
    for user_id, data in updated_skills.items():
        entry_id = data.get("entry_id") or entry_mapping.get(user_id)
        if not entry_id:
            current_app.logger.warning(f"[game2] エントリーID未発見: user_id={user_id}")
            continue
        try:
            entry_table.update_item(
                Key={"entry_id": entry_id},
                UpdateExpression="SET skill_score = :mu, skill_sigma = :sigma",
                ExpressionAttributeValues={
                    ":mu": Decimal(str(data["skill_score"])),
                    ":sigma": Decimal(str(data["skill_sigma"])),
                },
            )
            sync_count += 1
        except Exception as e:
            current_app.logger.error(f"[game2] 更新失敗 entry_id={entry_id}: {str(e)}")
    current_app.logger.info(f"[game2] 同期完了: {sync_count}/{len(updated_skills)} 件")
    return sync_count


@bp_game2.route("/court")
@login_required
def court():
    entry_table = _entry_table()
    meta_table = _meta_table()

    meta_current = meta_table.get_item(Key={"match_id": META_CURRENT_PK}, ConsistentRead=True).get("Item", {}) or {}
    status = meta_current.get("status", "idle")
    match_id = meta_current.get("current_match_id")

    all_entries = entry_table.scan().get("Items", [])
    my_entry = next((e for e in all_entries if e.get("user_id") == current_user.get_id()), None)

    courts = {}
    if status == "playing" and match_id:
        for e in all_entries:
            if e.get("match_id") == match_id and e.get("entry_status") == "playing":
                c = int(e.get("court_number", 0))
                courts.setdefault(c, {"A": [], "B": []})
                team = e.get("team", "A")
                courts[c][team].append(e)

    pending = [e for e in all_entries if e.get("entry_status") == "pending"]
    resting = [e for e in all_entries if e.get("entry_status") == "resting"]

    return render_template(
        "game2/court.html",
        status=status,
        match_id=match_id,
        courts=courts,
        pending=pending,
        resting=resting,
        my_entry=my_entry,
        is_admin=getattr(current_user, "administrator", False),
    )


@bp_game2.route("/entry", methods=["POST"])
@login_required
def entry():
    user_id = current_user.get_id()
    now = datetime.now(JST).isoformat()
    entry_table = _entry_table()
    user_table = current_app.dynamodb.Table("bad-users")

    existing = entry_table.scan(
        FilterExpression=Attr("user_id").eq(user_id) & Attr("entry_status").is_in(["pending", "resting", "playing"])
    ).get("Items", [])
    if existing:
        return redirect(url_for("game2.court"))

    cleanup = entry_table.scan(FilterExpression=Attr("user_id").eq(user_id)).get("Items", [])
    for item in cleanup:
        entry_table.delete_item(Key={"entry_id": item["entry_id"]})

    user_resp = user_table.get_item(Key={"user#user_id": user_id})
    user_data = user_resp.get("Item")
    skill_score = user_data.get("skill_score", 50) if user_data else 50
    skill_sigma = user_data.get("skill_sigma", 8.333) if user_data else 8.333
    display_name = user_data.get("display_name", "未設定") if user_data else "未設定"
    gender = user_data.get("gender", "M") if user_data else "M"

    entry_table.put_item(Item={
        "entry_id": str(uuid.uuid4()),
        "user_id": user_id,
        "match_id": "pending",
        "entry_status": "pending",
        "display_name": display_name,
        "gender": gender,
        "skill_score": Decimal(str(skill_score)),
        "skill_sigma": Decimal(str(skill_sigma)),
        "joined_at": now,
        "created_at": now,
        "rest_count": 0,
        "match_count": 0,
    })
    current_app.logger.info(f"[game2][ENTRY] 参加登録: {user_id} 名前: {display_name}")
    return redirect(url_for("game2.court"))


@bp_game2.route("/create_pairings", methods=["POST"])
@login_required
def create_pairings():
    if not current_user.administrator:
        flash("管理者のみ実行できます。", "danger")
        return redirect(url_for("game2.court"))

    if has_ongoing_matches2():
        flash("テストコートで進行中の試合があるため、新しいペアリングを実行できません。", "warning")
        return redirect(url_for("game2.court"))

    max_courts = min(max(int(request.form.get("max_courts", 2)), 1), 6)
    entry_table = _entry_table()
    meta_table = _meta_table()

    pairing_meta = meta_table.get_item(Key={"match_id": META_PAIRING_PK}, ConsistentRead=True).get("Item", {}) or {}
    cycle_index = int(pairing_meta.get("cycle_index", 0))
    if cycle_index == 1:
        mode = "full_random"
        next_cycle_index = 2
    elif cycle_index == 2:
        mode = "ai"
        next_cycle_index = 0
    else:
        mode = "random"
        next_cycle_index = cycle_index + 1

    response = entry_table.scan(FilterExpression=Attr("entry_status").eq("pending"))
    entries_by_user = {}
    for e in response.get("Items", []):
        uid, joined_at = e["user_id"], e.get("joined_at", "")
        if uid not in entries_by_user or joined_at > entries_by_user[uid].get("joined_at", ""):
            entries_by_user[uid] = e
    entries = list(entries_by_user.values())

    if len(entries) < 4:
        flash("テストコート: 4人以上のエントリーが必要です。", "warning")
        return redirect(url_for("game2.court"))

    sorted_entries = sorted(entries, key=lambda e: (e.get("match_count", 0), random.random()))
    cap_by_courts = min(max_courts * 4, len(sorted_entries))
    required_players = cap_by_courts - (cap_by_courts % 4)
    waiting_count = len(sorted_entries) - required_players

    if waiting_count > 0:
        active_entries, waiting_entries, _meta = _pick_waiters_by_rest_queue(
            entries=sorted_entries, waiting_count=waiting_count, queue_key=REST_QUEUE_KEY,
        )
    else:
        active_entries, waiting_entries = sorted_entries, []

    players = []
    for e in active_entries:
        skill_score = float(e.get("skill_score", 50.0))
        skill_sigma = float(e.get("skill_sigma", 8.333))
        conservative_val = skill_score - 3 * skill_sigma
        p = Player(e["display_name"], conservative_val, e.get("gender", "M"))
        p.user_id = e.get("user_id")
        p.entry_id = e.get("entry_id")
        p.conservative = conservative_val
        p.skill_score = skill_score
        p.skill_sigma = skill_sigma
        players.append(p)

    match_id = generate_match_id2()
    effective_courts = min(max_courts, len(players) // 4)

    if mode == "ai":
        matches, additional_waiting_players = generate_ai_best_pairings(players, effective_courts, iterations=1000)
    elif mode == "full_random":
        _pairs, matches, additional_waiting_players = generate_full_random_pairings(players, effective_courts)
    else:
        _pairs, matches, additional_waiting_players = generate_balanced_pairs_and_matches(players, effective_courts)

    if not matches:
        flash("テストコート: 試合を作成できませんでした。", "warning")
        return redirect(url_for("game2.court"))

    current_app.logger.info(
        "[game2][pairing] match_id=%s mode=%s courts=%d", match_id, mode, len(matches)
    )

    now_jst = datetime.now(JST).isoformat()
    import boto3
    dynamodb_client = boto3.client("dynamodb", region_name="ap-northeast-1")

    tx_items = [{
        "Update": {
            "TableName": "bad-game-matches",
            "Key": {"match_id": {"S": META_CURRENT_PK}},
            "UpdateExpression": "SET #st = :playing, #cm = :mid, #cc = :cc, #ua = :now, #pm = :mode",
            "ConditionExpression": "attribute_not_exists(#st) OR #st <> :playing",
            "ExpressionAttributeNames": {
                "#st": "status", "#cm": "current_match_id", "#cc": "court_count",
                "#ua": "updated_at", "#pm": "pairing_mode",
            },
            "ExpressionAttributeValues": {
                ":playing": {"S": "playing"}, ":mid": {"S": str(match_id)},
                ":cc": {"N": str(len(matches))}, ":now": {"S": now_jst}, ":mode": {"S": mode},
            },
        }
    }]

    for court_num, ((a1, a2), (b1, b2)) in enumerate(matches, 1):
        for pl, team in [(a1, "A"), (a2, "A"), (b1, "B"), (b2, "B")]:
            entry_id = str(getattr(pl, "entry_id", "") or "")
            tx_items.append({
                "Update": {
                    "TableName": "bad-game2-match_entries",
                    "Key": {"entry_id": {"S": entry_id}},
                    "UpdateExpression": "SET entry_status=:playing, match_id=:mid, court_number=:c, team=:t, updated_at=:now",
                    "ConditionExpression": "entry_status = :pending",
                    "ExpressionAttributeValues": {
                        ":playing": {"S": "playing"}, ":pending": {"S": "pending"},
                        ":mid": {"S": str(match_id)}, ":c": {"N": str(court_num)},
                        ":t": {"S": team}, ":now": {"S": now_jst},
                    },
                }
            })

    try:
        dynamodb_client.transact_write_items(TransactItems=tx_items)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "TransactionCanceledException":
            flash("テストコート: 進行中の試合があるためペアリングできませんでした。", "warning")
            return redirect(url_for("game2.court"))
        raise

    meta_table.update_item(
        Key={"match_id": META_PAIRING_PK},
        UpdateExpression="SET cycle_index=:ci, last_mode=:m, last_match_id=:mid, updated_at=:now",
        ExpressionAttributeValues={
            ":ci": next_cycle_index, ":m": mode, ":mid": str(match_id), ":now": now_jst,
        },
    )

    flash(f"テストコート: {len(matches)}コート分の組み合わせを作成しました（モード: {mode}）", "success")
    return redirect(url_for("game2.court"))


@bp_game2.route("/submit_score/<match_id>/court/<int:court_number>", methods=["POST"])
@login_required
def submit_score(match_id, court_number):
    try:
        team1_raw = request.form.get("team1_score")
        team2_raw = request.form.get("team2_score")
        if team1_raw is None or team2_raw is None:
            return "スコアが送信されていません", 400
        team1_score = int(team1_raw)
        team2_score = int(team2_raw)
        if team1_score == team2_score:
            return "スコアが同点です。勝者を決めてください。", 400

        winner = "A" if team1_score > team2_score else "B"
        court_number_int = int(court_number)

        entry_table = _entry_table()
        entries = entry_table.scan(
            FilterExpression=Attr("match_id").eq(str(match_id)) & Attr("court_number").eq(court_number_int)
        ).get("Items", [])
        if not entries:
            return "コートのエントリーが見つかりません", 404

        if not current_user.administrator:
            court_user_ids = [str(e.get("user_id", "")) for e in entries]
            if current_user.user_id not in court_user_ids:
                return "このコートへのスコア送信権限がありません", 403

        team_a, team_b = [], []
        for e in entries:
            player = {
                "user_id": str(e.get("user_id", "")),
                "display_name": str(e.get("display_name", "不明")),
                "entry_id": str(e.get("entry_id", "")),
                "skill_score": Decimal(str(e.get("skill_score", 25.0))),
                "skill_sigma": Decimal(str(e.get("skill_sigma", 8.333))),
            }
            (team_a if e.get("team") == "A" else team_b).append(player)

        if not team_a or not team_b:
            return "コートのチームデータが不完全です", 404

        results_table = _results_table()
        result_id = f"{match_id}#{court_number_int}"
        try:
            results_table.put_item(
                Item={
                    "result_id": result_id, "match_id": str(match_id), "court_number": court_number_int,
                    "team1_score": team1_score, "team2_score": team2_score, "winner": winner,
                    "team_a": team_a, "team_b": team_b, "created_at": datetime.now(JST).isoformat(),
                },
                ConditionExpression="attribute_not_exists(result_id)",
            )
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return "", 200
            raise

        current_app.logger.info(
            "[game2] Score: Match=%s, Court=%d, %d-%d (Win:%s)",
            match_id, court_number_int, team1_score, team2_score, winner,
        )
        return "", 200
    except Exception as e:
        current_app.logger.error("[game2][submit_score ERROR] %s", str(e), exc_info=True)
        return "スコアの送信中にエラーが発生しました", 500


@bp_game2.route("/finish_current_match", methods=["POST"])
@login_required
def finish_current_match():
    if not current_user.administrator:
        flash("管理者のみ実行できます。", "danger")
        return redirect(url_for("game2.court"))

    meta_table = _meta_table()
    meta_item = meta_table.get_item(Key={"match_id": META_CURRENT_PK}, ConsistentRead=True).get("Item") or {}
    status = meta_item.get("status")
    match_id = meta_item.get("current_match_id")
    court_count = int(meta_item.get("court_count", 0) or 0)

    if status != "playing" or not match_id:
        flash("テストコート: アクティブな試合が見つかりません", "warning")
        return redirect(url_for("game2.court"))

    results_table = _results_table()
    match_results = results_table.scan(FilterExpression=Attr("match_id").eq(match_id)).get("Items", [])
    if len(match_results) < court_count:
        flash(f"テストコート: 未送信のコートがあります（{len(match_results)}/{court_count}）", "warning")
        return redirect(url_for("game2.court"))

    entry_table = _entry_table()
    playing_players = entry_table.scan(
        FilterExpression=Attr("match_id").eq(match_id) & Attr("entry_status").eq("playing")
    ).get("Items", [])
    player_mapping = {p["user_id"]: p["entry_id"] for p in playing_players if "user_id" in p and "entry_id" in p}

    updated_skills = {}
    for result in match_results:
        try:
            from game.game_utils import parse_players
            team_a = parse_players(result.get("team_a", []))
            team_b = parse_players(result.get("team_b", []))
            for pl in team_a + team_b:
                uid = pl.get("user_id")
                if uid in player_mapping:
                    pl["entry_id"] = player_mapping[uid]
            result_item = {
                "team_a": team_a, "team_b": team_b, "winner": result.get("winner", "A"),
                "match_id": match_id, "court_number": result.get("court_number"),
                "team1_score": result.get("team1_score"), "team2_score": result.get("team2_score"),
            }
            updated_skills.update(update_trueskill_for_players_and_return_updates(result_item))
        except Exception as e:
            current_app.logger.error("[game2] スキル更新エラー (court=%s): %s", result.get("court_number"), e)

    sync_match_entries_with_updated_skills2(player_mapping, updated_skills)
    persist_skill_to_bad_users(updated_skills)

    import boto3
    dynamodb_client = boto3.client("dynamodb", region_name="ap-northeast-1")
    now_jst = datetime.now(JST).isoformat()

    tx_items = [{
        "Update": {
            "TableName": "bad-game-matches",
            "Key": {"match_id": {"S": META_CURRENT_PK}},
            "UpdateExpression": "SET #st = :idle, #ua = :now REMOVE #cm, #cc",
            "ConditionExpression": "#st = :playing AND #cm = :mid",
            "ExpressionAttributeNames": {"#st": "status", "#ua": "updated_at", "#cm": "current_match_id", "#cc": "court_count"},
            "ExpressionAttributeValues": {":idle": {"S": "idle"}, ":playing": {"S": "playing"}, ":mid": {"S": str(match_id)}, ":now": {"S": now_jst}},
        }
    }]
    for p in playing_players:
        entry_id = p.get("entry_id")
        if not entry_id:
            continue
        tx_items.append({
            "Update": {
                "TableName": "bad-game2-match_entries",
                "Key": {"entry_id": {"S": str(entry_id)}},
                "UpdateExpression": "SET entry_status=:pending, updated_at=:now REMOVE court_number, team, match_id",
                "ConditionExpression": "entry_status = :playing AND match_id = :mid",
                "ExpressionAttributeValues": {
                    ":pending": {"S": "pending"}, ":playing": {"S": "playing"},
                    ":mid": {"S": str(match_id)}, ":now": {"S": now_jst},
                },
            }
        })

    try:
        dynamodb_client.transact_write_items(TransactItems=tx_items)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "TransactionCanceledException":
            return jsonify({"success": False, "error": "finish transaction canceled"}), 409
        raise

    flash("テストコート: 試合を終了しました", "success")
    return redirect(url_for("game2.court"))


@bp_game2.route("/emergency_transfer", methods=["POST"])
@login_required
def emergency_transfer():
    """
    緊急脱出ボタン: テストコートの参加者全員を、既存システム（本番のコート）の
    待機列(pending)に移す。テストコート側の状態は初期化する。
    既存システムのコードには一切触れず、bad-game-match_entries に
    「今から並び直す人」として新規レコードを作るだけなので安全。
    """
    if not current_user.administrator:
        flash("管理者のみ実行できます。", "danger")
        return redirect(url_for("game2.court"))

    entry_table = _entry_table()
    main_entry_table = current_app.dynamodb.Table("bad-game-match_entries")
    now = datetime.now(JST).isoformat()

    game2_entries = entry_table.scan().get("Items", [])

    # 既に本番側にアクティブなエントリーがある人は二重登録を避けるため対象外にする
    main_active = main_entry_table.scan(
        FilterExpression=Attr("entry_status").is_in(["pending", "playing", "resting"])
    ).get("Items", [])
    main_active_uids = {e.get("user_id") for e in main_active}

    moved, skipped = 0, 0
    for e in game2_entries:
        uid = e.get("user_id")
        if uid in main_active_uids:
            current_app.logger.warning("[game2][emergency] 本番側に既存エントリーあり、移動をスキップ: %s", uid)
            skipped += 1
        else:
            main_entry_table.put_item(Item={
                "entry_id": str(uuid.uuid4()),
                "user_id": uid,
                "match_id": "pending",
                "entry_status": "pending",
                "display_name": e.get("display_name", "未設定"),
                "gender": e.get("gender", "M"),
                "skill_score": e.get("skill_score", Decimal("50")),
                "skill_sigma": e.get("skill_sigma", Decimal("8.333")),
                "joined_at": now,
                "created_at": now,
                "rest_count": 0,
                "match_count": 0,
            })
            moved += 1
        entry_table.delete_item(Key={"entry_id": e["entry_id"]})

    meta_table = _meta_table()
    meta_table.update_item(
        Key={"match_id": META_CURRENT_PK},
        UpdateExpression="SET #st = :idle, #ua = :now REMOVE current_match_id, court_count",
        ExpressionAttributeNames={"#st": "status", "#ua": "updated_at"},
        ExpressionAttributeValues={":idle": "idle", ":now": now},
    )

    current_app.logger.warning(
        "[game2][emergency] 緊急移動実行: moved=%d skipped=%d by=%s",
        moved, skipped, current_user.get_id(),
    )
    flash(f"テストコートの参加者{moved}人を通常コートに移動しました。（本番側に既存登録があり{skipped}人はスキップ）", "warning")
    return redirect(url_for("game.court"))

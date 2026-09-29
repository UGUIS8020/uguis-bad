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

Phase 2: 最初の組み合わせだけ管理者が「組み合わせ作成」ボタンで作る。
その後は、あるコートのスコアが送信されるたびに、そのコートの4人だけを
TrueSkill更新してpending化し、休憩ローテーションで「次に出るべき」上位候補の
中から実力バランスが良い4人を選んで、そのコートだけ自動的に次の試合を作る。
他のコートは無関係に進行中のまま影響を受けない。
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
    _rest_queue_pk,
)
from game.views import (
    persist_skill_to_bad_users,
    _pick_waiters_by_rest_queue,
    _save_rest_queue_optimistic,
)
from utils.timezone import JST
from itertools import groupby

logger = logging.getLogger(__name__)

bp_game2 = Blueprint('game2', __name__)

REST_QUEUE_KEY = "continuous_rest_queue"
META_CURRENT_PK = "meta#continuous_current"
META_PAIRING_PK = "meta#continuous_pairing"

# コート表示に使う、ペアリングモードの日本語ラベル
PAIRING_MODE_LABELS = {
    "random": "バランス考慮",
    "full_random": "ランダム",  # 人選は完全ランダムだがチーム分けは実力差を調整するため、
                              # 「完全」ランダムという表示は誤解を招く
    "ai": "AIペアリング",
    "ai_pairing": "AIペアリング",
    "ai_pairing_balanced": "調整AIペアリング",  # 継続的バランス調整(参加回数の多い人を除外)
                                             # が適用された回のAIペアリング
    "ai_pairing_full_balanced": "強調整AIペアリング",  # スキルモード一斉入れ替え前の助走
                                                    # (pre_skill_remaining)期間、除外に加えて
                                                    # 最も少ない人を強制参加させる強めの調整
    "balance_only": "スキルモード",
    "fairness_first": "休憩優先",
    "safety_valve": "AIペアリング",  # 内部的には救済モード(待ちすぎの人を強制救済)だが、
                                    # 実体は_best_balanced_fourを使うAIペアリングと同じロジックのため表示を統合
}


def _mode_label(mode):
    return PAIRING_MODE_LABELS.get(mode, "自動進行中")


def _entry_table():
    return current_app.dynamodb.Table("bad-game2-match_entries")


def _results_table():
    return current_app.dynamodb.Table("bad-game2-results")


def _meta_table():
    return current_app.dynamodb.Table("bad-game-matches")


def generate_match_id2():
    """
    試合ID生成（既存システムのIDと絶対に衝突しないよう g2_ を付ける）。
    連続マッチングでは短時間に複数コートが立て続けに補充されるため、
    秒単位の時刻だけだと同一match_idが発行されうる（result_idの重複判定が
    誤動作し、そのコートが止まる原因になる）。末尾に短いランダム値を足して
    タイミングに関わらず一意にする。
    """
    now = datetime.now()
    match_id = "g2_" + now.strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
    current_app.logger.info(f"[game2] 生成された試合ID: {match_id}")
    return match_id


def has_ongoing_matches2():
    """テストコート側で進行中の試合があるか（既存システムとは独立に判定）"""
    try:
        entry_table = _entry_table()
        resp = entry_table.scan(FilterExpression=Attr("entry_status").eq("playing"), ConsistentRead=True)
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
    # ページ読み込みのたびに、猶予(COURT_REFILL_DELAY_SECONDS秒)を過ぎた「空きコート」があれば補充する
    try:
        _process_awaiting_refills()
    except Exception as e:
        current_app.logger.error("[game2] _process_awaiting_refills エラー: %s", e, exc_info=True)
    try:
        _process_held_pairs()
    except Exception as e:
        current_app.logger.error("[game2] _process_held_pairs エラー: %s", e, exc_info=True)
    try:
        _process_skill_burst()
    except Exception as e:
        current_app.logger.error("[game2] _process_skill_burst エラー: %s", e, exc_info=True)
    try:
        _process_new_court_opportunity()
    except Exception as e:
        current_app.logger.error("[game2] _process_new_court_opportunity エラー: %s", e, exc_info=True)
    try:
        _reconcile_orphaned_courts()
    except Exception as e:
        current_app.logger.error("[game2] _reconcile_orphaned_courts エラー: %s", e, exc_info=True)

    entry_table = _entry_table()
    meta_table = _meta_table()

    meta_current = meta_table.get_item(Key={"match_id": META_CURRENT_PK}, ConsistentRead=True).get("Item", {}) or {}
    status = meta_current.get("status", "idle")

    all_entries = entry_table.scan(ConsistentRead=True).get("Items", [])
    my_entry = next((e for e in all_entries if e.get("user_id") == current_user.get_id()), None)

    # ★Phase2: 全コート共通の1つのmatch_idではなく、コートごとに独立した
    #   match_idを持つので、entry_status=="playing"を持つ人をコート番号で
    #   グルーピングし、そのコートの現在のmatch_idも一緒に持たせる。
    courts = {}
    for e in all_entries:
        if e.get("entry_status") == "playing" and e.get("court_number") is not None:
            c = int(e.get("court_number"))
            courts.setdefault(c, {
                "A": [], "B": [], "match_id": e.get("match_id"),
                "mode_label": _mode_label(e.get("pairing_mode")),
            })
            team = e.get("team", "A")
            courts[c][team].append(e)

    pending = [e for e in all_entries if e.get("entry_status") == "pending"]
    resting = [e for e in all_entries if e.get("entry_status") == "resting"]
    # ★「参加者」= コートに入っている全員(試合中+待機中+休憩中)。
    #   非管理者にはcourtsを自分のコートだけに絞る(下記)ため、その前の
    #   all_entries時点で数えておく必要がある。
    total_participants = len(all_entries)

    is_admin = getattr(current_user, "administrator", False)

    # ★管理者以外は自分が今出ているコートだけを見せる（他コートの様子は非表示）
    if not is_admin:
        if my_entry and my_entry.get("entry_status") == "playing" and my_entry.get("court_number") is not None:
            my_court_num = int(my_entry.get("court_number"))
            courts = {my_court_num: courts[my_court_num]} if my_court_num in courts else {}
        else:
            courts = {}

    # ★コート番号順(1,2,3...)で常に同じ並びになるようにする
    courts = dict(sorted(courts.items()))

    awaiting_refill = meta_current.get("awaiting_refill") or {}
    awaiting_display = {}
    if awaiting_refill:
        now = datetime.now(JST)
        for court_str, freed_at_iso in awaiting_refill.items():
            try:
                freed_at = datetime.fromisoformat(freed_at_iso)
                remaining = max(0, int(COURT_REFILL_DELAY_SECONDS - (now - freed_at).total_seconds()))
            except Exception:
                remaining = 0
            awaiting_display[int(court_str)] = remaining

    held_for_pairing = meta_current.get("held_for_pairing") or {}
    held_display = sorted(int(c) for c in held_for_pairing.keys())

    awaiting_skill_burst = meta_current.get("awaiting_skill_burst") or {}
    skill_burst_display = sorted(int(c) for c in awaiting_skill_burst.keys())

    return render_template(
        "game2/court.html",
        status=status,
        courts=courts,
        pending=pending,
        resting=resting,
        total_participants=total_participants,
        my_entry=my_entry,
        is_admin=is_admin,
        awaiting_refill=awaiting_display,
        matching_paused=bool(meta_current.get("matching_paused")),
        held_for_pairing=held_display,
        awaiting_skill_burst=skill_burst_display,
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


@bp_game2.route("/leave_court", methods=["POST"])
@login_required
def leave_court():
    """テストコートから退出する（エントリー削除）。試合中は退出不可"""
    try:
        user_id = current_user.get_id()
        entry_table = _entry_table()

        items = entry_table.scan(
            FilterExpression=Attr("user_id").eq(user_id), ConsistentRead=True
        ).get("Items", [])

        if not items:
            flash("エントリーが見つかりませんでした", "warning")
            return redirect(url_for("game2.court"))

        for e in items:
            if e.get("entry_status") == "playing":
                flash("試合中のため退出できません", "warning")
                return redirect(url_for("game2.court"))

        for e in items:
            entry_table.delete_item(Key={"entry_id": e["entry_id"]})
            current_app.logger.info("[game2][leave_court] deleted entry_id=%s", e["entry_id"])

        flash("テストコートから退出しました", "info")
        return redirect(url_for("index"))

    except Exception as e:
        current_app.logger.exception(f"[game2][leave_court] 退出エラー: {e}")
        flash("退出に失敗しました", "danger")
        return redirect(url_for("game2.court"))


@bp_game2.route("/rest", methods=["POST"])
@login_required
def rest():
    """
    休憩ボタン。今の状態によって挙動が変わる:
    - 待機中(pending): 即座に休憩に切り替える
    - 試合中(playing): 今の試合はそのまま続け、rest_requestedを立てておく。
      試合が終わってpending/restingに振り分けられる際(_try_refill_court)に
      自動的に休憩扱いになる（＝次回から休憩が適用される）
    """
    try:
        user_id = current_user.get_id()
        entry_table = _entry_table()

        items = entry_table.scan(
            FilterExpression=Attr("user_id").eq(user_id), ConsistentRead=True
        ).get("Items", [])

        if not items:
            flash("エントリーが見つかりませんでした", "warning")
            return redirect(url_for("game2.court"))

        entry = items[0]

        if entry.get("entry_status") == "playing":
            entry_table.update_item(
                Key={"entry_id": entry["entry_id"]},
                UpdateExpression="SET rest_requested = :true, updated_at = :now",
                ExpressionAttributeValues={":true": True, ":now": datetime.now(JST).isoformat()},
            )
            current_app.logger.info("[game2][rest] user=%s entry_id=%s 休憩を予約(次回から適用)", user_id, entry["entry_id"])
            flash("休憩を予約しました。この試合が終わったら休憩になります。", "info")
            return redirect(url_for("game2.court"))

        entry_table.update_item(
            Key={"entry_id": entry["entry_id"]},
            UpdateExpression=(
                "SET entry_status = :resting, rest_started_at = :now, "
                "rest_count = if_not_exists(rest_count, :zero) + :one"
            ),
            ExpressionAttributeValues={
                ":resting": "resting", ":now": datetime.now(JST).isoformat(),
                ":zero": 0, ":one": 1,
            },
        )
        current_app.logger.info("[game2][rest] user=%s entry_id=%s 休憩に切替", user_id, entry["entry_id"])
    except Exception as e:
        current_app.logger.error(f"[game2][rest] 休憩エラー: {e}")
        flash("休憩への切替に失敗しました", "danger")

    return redirect(url_for("game2.court"))


@bp_game2.route("/rest_request_cancel", methods=["POST"])
@login_required
def rest_request_cancel():
    """試合中に予約した休憩をキャンセルする"""
    try:
        user_id = current_user.get_id()
        entry_table = _entry_table()

        items = entry_table.scan(
            FilterExpression=Attr("user_id").eq(user_id) & Attr("entry_status").eq("playing"),
            ConsistentRead=True,
        ).get("Items", [])

        if not items:
            flash("試合中のエントリーが見つかりませんでした", "warning")
            return redirect(url_for("game2.court"))

        entry = items[0]
        entry_table.update_item(
            Key={"entry_id": entry["entry_id"]},
            UpdateExpression="REMOVE rest_requested",
        )
        current_app.logger.info("[game2][rest_request_cancel] user=%s entry_id=%s 休憩予約をキャンセル", user_id, entry["entry_id"])
        flash("休憩の予約をキャンセルしました。", "info")
    except Exception as e:
        current_app.logger.error(f"[game2][rest_request_cancel] エラー: {e}")
        flash("キャンセルに失敗しました", "danger")

    return redirect(url_for("game2.court"))


@bp_game2.route("/admin_rest_entry/<entry_id>", methods=["POST"])
@login_required
def admin_rest_entry(entry_id):
    """管理者が待機中の名前タップで、その人を直接休憩に切り替える"""
    if not current_user.administrator:
        flash("管理者のみ実行できます。", "danger")
        return redirect(url_for("game2.court"))

    entry_table = _entry_table()
    item = entry_table.get_item(Key={"entry_id": entry_id}, ConsistentRead=True).get("Item")
    if not item:
        flash("エントリーが見つかりませんでした", "warning")
        return redirect(url_for("game2.court"))

    if item.get("entry_status") != "pending":
        flash("待機中の参加者のみ休憩に切り替えられます", "warning")
        return redirect(url_for("game2.court"))

    entry_table.update_item(
        Key={"entry_id": entry_id},
        UpdateExpression=(
            "SET entry_status = :resting, rest_started_at = :now, "
            "rest_count = if_not_exists(rest_count, :zero) + :one"
        ),
        ExpressionAttributeValues={
            ":resting": "resting", ":now": datetime.now(JST).isoformat(),
            ":zero": 0, ":one": 1,
        },
    )
    current_app.logger.info(
        "[game2][admin_rest_entry] admin=%s entry_id=%s(%s) を休憩に切替",
        current_user.get_id(), entry_id, item.get("display_name"),
    )
    flash(f"{item.get('display_name', '参加者')}を休憩に切り替えました", "info")
    return redirect(url_for("game2.court"))


@bp_game2.route("/admin_resume_entry/<entry_id>", methods=["POST"])
@login_required
def admin_resume_entry(entry_id):
    """管理者が休憩中の名前タップで、その人を直接待機に戻す"""
    if not current_user.administrator:
        flash("管理者のみ実行できます。", "danger")
        return redirect(url_for("game2.court"))

    entry_table = _entry_table()
    item = entry_table.get_item(Key={"entry_id": entry_id}, ConsistentRead=True).get("Item")
    if not item:
        flash("エントリーが見つかりませんでした", "warning")
        return redirect(url_for("game2.court"))

    if item.get("entry_status") != "resting":
        flash("休憩中の参加者のみ待機に戻せます", "warning")
        return redirect(url_for("game2.court"))

    entry_table.update_item(
        Key={"entry_id": entry_id},
        UpdateExpression="SET entry_status = :pending, match_id = :pending, resumed_at = :now",
        ExpressionAttributeValues={
            ":pending": "pending", ":now": datetime.now(JST).isoformat(),
        },
    )
    current_app.logger.info(
        "[game2][admin_resume_entry] admin=%s entry_id=%s(%s) を待機に戻す",
        current_user.get_id(), entry_id, item.get("display_name"),
    )
    flash(f"{item.get('display_name', '参加者')}を待機に戻しました", "info")
    return redirect(url_for("game2.court"))


@bp_game2.route("/resume", methods=["POST"])
@login_required
def resume():
    """休憩から復帰（待機に戻す）"""
    try:
        user_id = current_user.get_id()
        entry_table = _entry_table()

        items = entry_table.scan(
            FilterExpression=Attr("user_id").eq(user_id), ConsistentRead=True
        ).get("Items", [])

        if not items:
            flash("エントリーが見つかりませんでした", "warning")
            return redirect(url_for("game2.court"))

        entry = items[0]
        entry_table.update_item(
            Key={"entry_id": entry["entry_id"]},
            UpdateExpression="SET entry_status = :pending, match_id = :pending, resumed_at = :now",
            ExpressionAttributeValues={
                ":pending": "pending", ":now": datetime.now(JST).isoformat(),
            },
        )
        current_app.logger.info("[game2][resume] user=%s entry_id=%s 復帰", user_id, entry["entry_id"])
    except Exception as e:
        current_app.logger.error(f"[game2][resume] 復帰エラー: {e}")
        flash("復帰に失敗しました", "danger")

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

    response = entry_table.scan(FilterExpression=Attr("entry_status").eq("pending"), ConsistentRead=True)
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

    # ★継続補充モードでは、練習終了を押さない限りstatusは"playing"のまま
    #   変わらない（コートが全部空でも同じ）。以前はここに「statusが
    #   playingでないこと」というConditionExpressionがあり、マッチング停止
    #   →全コート終了→組み合わせ作成で再開、という流れを塞いでしまっていた。
    #   二重作成の防止は上のhas_ongoing_matches2()と各エントリーの
    #   ConditionExpression(entry_status = :pending)で十分なので、ここでは
    #   条件を付けず、マッチング停止フラグ・古いawaiting_refillも一緒に
    #   クリアして再開できるようにする。
    tx_items = [{
        "Update": {
            "TableName": "bad-game-matches",
            "Key": {"match_id": {"S": META_CURRENT_PK}},
            "UpdateExpression": (
                "SET #st = :playing, #cm = :mid, #cc = :cc, #ua = :now, #pm = :mode, #mc = :maxc "
                "REMOVE matching_paused, awaiting_refill, held_for_pairing, awaiting_skill_burst"
            ),
            "ExpressionAttributeNames": {
                "#st": "status", "#cm": "current_match_id", "#cc": "court_count",
                "#ua": "updated_at", "#pm": "pairing_mode", "#mc": "max_courts",
            },
            "ExpressionAttributeValues": {
                ":playing": {"S": "playing"}, ":mid": {"S": str(match_id)},
                ":cc": {"N": str(len(matches))}, ":now": {"S": now_jst}, ":mode": {"S": mode},
                ":maxc": {"N": str(max_courts)},
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
                    "UpdateExpression": "SET entry_status=:playing, match_id=:mid, court_number=:c, team=:t, updated_at=:now, pairing_mode=:pm",
                    "ConditionExpression": "entry_status = :pending",
                    "ExpressionAttributeValues": {
                        ":playing": {"S": "playing"}, ":pending": {"S": "pending"},
                        ":mid": {"S": str(match_id)}, ":c": {"N": str(court_num)},
                        ":t": {"S": team}, ":now": {"S": now_jst}, ":pm": {"S": mode},
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
        UpdateExpression=(
            "SET cycle_index=:ci, last_mode=:m, last_match_id=:mid, updated_at=:now, "
            "last_skill_burst_at=:now"
        ),
        ExpressionAttributeValues={
            ":ci": next_cycle_index, ":m": mode, ":mid": str(match_id), ":now": now_jst,
        },
    )

    flash(f"テストコート: {len(matches)}コート分の組み合わせを作成しました（モード: {mode}）", "success")
    return redirect(url_for("game2.court"))


def _all_pending_unordered(entry_table):
    """
    待機中の人を、休憩ローテーション上の順序を一切考慮せず全員返す。
    AIペアリングモード(force_top_n=1の強制枠を除く残り3枠)用。
    """
    return entry_table.scan(FilterExpression=Attr("entry_status").eq("pending"), ConsistentRead=True).get("Items", [])


def _pop_next_from_play_queue(entry_table, meta_table, count=1, *, queue_key=REST_QUEUE_KEY, max_retries=5):
    """
    休みの調整を担う、唯一の仕組み。旧システム(game/views.py の
    _pick_waiters_by_rest_queue)と同じ考え方の永続キューを使うが、
    「休む人」ではなく「次に出るべき人」を先頭から取り出す点が逆になっている。

    - 通常時: DynamoDBに保存された永続キューの先頭からcount人を取り出すだけ
      （真のFIFO）。
    - キューが足りない場合: 待機中全員をmatch_count(通算試合数)の少ない順に
      並べ直す。ただし同じmatch_count同士は必ずシャッフルしてから並べる
      （これをしないと同じ顔ぶれが固まって毎回一緒に選ばれ続けてしまう）。
      直前に選ばれた人たちは、連続で選ばれないよう最後尾に回す。

    WAIT_RESCUE_THRESHOLDのような「何回逃したら救済する」という事後的な
    しきい値判定は使わず、常にこのキューの先頭を優先的に含めることで
    公平性を保証する（旧システムが「休む人を先に確定する」のと同じ発想）。
    """
    import random as _random

    pk = _rest_queue_pk(queue_key)

    for attempt in range(1, max_retries + 1):
        pending = entry_table.scan(FilterExpression=Attr("entry_status").eq("pending"), ConsistentRead=True).get("Items", [])
        by_id = {e["user_id"]: e for e in pending if e.get("user_id")}
        current_user_set = set(by_id.keys())

        resp = meta_table.get_item(Key={"match_id": pk}, ConsistentRead=True)
        qi = resp.get("Item") or {}
        queue = [uid for uid in qi.get("queue", []) if uid in current_user_set]
        generation = int(qi.get("generation", 1) or 1)
        version = int(qi.get("version", 0) or 0)

        if len(queue) < count:
            generation += 1
            last_picked_uids = set(qi.get("last_waiters", []))
            sorted_entries = sorted(pending, key=lambda e: (e.get("match_count", 0), e.get("joined_at", "")))
            all_uids = [e["user_id"] for e in sorted_entries if e.get("user_id")]
            others = [uid for uid in all_uids if uid not in last_picked_uids]
            prev_picked = [uid for uid in all_uids if uid in last_picked_uids]

            shuffled = []
            for _, g in groupby(others, key=lambda uid: by_id.get(uid, {}).get("match_count", 0)):
                g = list(g)
                _random.shuffle(g)
                shuffled.extend(g)

            new_queue_ordered = shuffled + prev_picked
            picked_uids = new_queue_ordered[:count]
            queue_next = new_queue_ordered[count:]
        else:
            picked_uids = queue[:count]
            queue_next = queue[count:]

        if len(picked_uids) < count:
            return []

        ok = _save_rest_queue_optimistic(
            meta_table, queue_key=queue_key, queue=queue_next,
            generation=generation, prev_version=version,
            last_waiters=picked_uids,
        )
        if not ok:
            continue

        return [by_id[uid] for uid in picked_uids if uid in by_id]

    current_app.logger.error("[game2][continuous] 永続キューの保存に失敗しました(競合過多)")
    return []


RECENT_HISTORY_RESULTS = 50  # 直近何件の試合結果を「最近」とみなすか
PARTNER_REPEAT_WEIGHT = 1
OPPONENT_REPEAT_WEIGHT = 2  # 対戦相手の重複の方が体感の偏りが大きいため重めに
BALANCE_TIEBREAK_MARGIN_RATIO = 0.05  # 最良バランスの5%以内を「僅差」とみなす
BALANCE_TIEBREAK_MARGIN_FLOOR = 1.0


def _get_recent_pair_history2(results_table, max_results=RECENT_HISTORY_RESULTS):
    """
    直近の試合結果から「誰と誰がパートナーだったか」「誰と誰が対戦したか」を
    集計する。件数がまだ少ないテーブルなのでシンプルに全件スキャン→
    created_atで新しい順にmax_results件だけ使う。
    """
    from collections import Counter

    all_results = results_table.scan().get("Items", [])
    all_results.sort(key=lambda r: r.get("created_at", ""), reverse=True)
    recent = all_results[:max_results]

    partner_counter = Counter()
    opponent_counter = Counter()
    for r in recent:
        a_uids = [p.get("user_id") for p in (r.get("team_a") or []) if p.get("user_id")]
        b_uids = [p.get("user_id") for p in (r.get("team_b") or []) if p.get("user_id")]
        if len(a_uids) == 2:
            partner_counter[frozenset(a_uids)] += 1
        if len(b_uids) == 2:
            partner_counter[frozenset(b_uids)] += 1
        for x in a_uids:
            for y in b_uids:
                opponent_counter[frozenset([x, y])] += 1
    return partner_counter, opponent_counter


def _repeat_penalty2(team_a, team_b, partner_counter, opponent_counter):
    a_uids = [e.get("user_id") for e in team_a]
    b_uids = [e.get("user_id") for e in team_b]
    penalty = 0
    if a_uids[0] and a_uids[1]:
        penalty += partner_counter.get(frozenset(a_uids), 0) * PARTNER_REPEAT_WEIGHT
    if b_uids[0] and b_uids[1]:
        penalty += partner_counter.get(frozenset(b_uids), 0) * PARTNER_REPEAT_WEIGHT
    for x in a_uids:
        for y in b_uids:
            if x and y:
                penalty += opponent_counter.get(frozenset([x, y]), 0) * OPPONENT_REPEAT_WEIGHT
    return penalty


def _best_balanced_four(candidates, partner_counter=None, opponent_counter=None, force_top_n=1):
    """
    候補(4人以上、休憩ローテーション上「次に出るべき順」にソート済み)の中から
    4人+2v2分けを選ぶ。

    先頭からforce_top_n人(デフォルト1人＝最も長く待っている・試合数が
    少ない人)は必ず含める。これをせずに純粋にバランス最良の4人だけを
    毎回総当たりで選んでいたところ、特定の人がスキル値の組み合わせの
    都合で何度選んでも外れ続け、ずっと待機のままになる不具合があった
    （休憩の公平性が実質機能しない）。残りは、その人たちと組んだときに
    実力バランスが最良になるよう選ぶ。force_top_nを増やすほど、待機の
    公平性が強く保証される分、バランス最適化の余地は狭まる。

    さらに、実力バランスがほぼ同点の候補が複数あるときは、直近の試合で
    同じ相手とパートナー/対戦済みの頻度が低い方を優先する（実力バランス
    自体を崩してまで多様性を優先することはない）。
    """
    from itertools import combinations

    def conservative(e):
        return float(e.get("skill_score", 50.0)) - 3 * float(e.get("skill_sigma", 8.333))

    must_include = tuple(candidates[:force_top_n])
    rest_pool = candidates[force_top_n:]
    remaining_needed = 4 - force_top_n
    pairing_patterns = [((0, 1), (2, 3)), ((0, 2), (1, 3)), ((0, 3), (1, 2))]

    all_options = []  # [(diff, team_a, team_b), ...]
    for combo_rest in combinations(rest_pool, remaining_needed):
        combo = must_include + combo_rest
        scores = [conservative(e) for e in combo]
        for (i1, i2), (i3, i4) in pairing_patterns:
            diff = abs((scores[i1] + scores[i2]) - (scores[i3] + scores[i4]))
            all_options.append((diff, [combo[i1], combo[i2]], [combo[i3], combo[i4]]))

    min_diff = min(o[0] for o in all_options)

    use_tiebreak = bool(partner_counter) or bool(opponent_counter)
    if use_tiebreak:
        margin = max(BALANCE_TIEBREAK_MARGIN_FLOOR, min_diff * BALANCE_TIEBREAK_MARGIN_RATIO)
        near_best = [o for o in all_options if o[0] <= min_diff + margin]
        chosen = min(near_best, key=lambda o: _repeat_penalty2(o[1], o[2], partner_counter, opponent_counter))
    else:
        chosen = min(all_options, key=lambda o: o[0])

    diff, team_a, team_b = chosen
    return team_a, team_b, diff


def _partner_penalty(team_a, team_b, partner_counter):
    """パートナー重複だけを見るペナルティ（対戦相手の重複は見ない）"""
    a_uids = [e.get("user_id") for e in team_a]
    b_uids = [e.get("user_id") for e in team_b]
    penalty = 0
    if a_uids[0] and a_uids[1]:
        penalty += partner_counter.get(frozenset(a_uids), 0)
    if b_uids[0] and b_uids[1]:
        penalty += partner_counter.get(frozenset(b_uids), 0)
    return penalty


def _fairness_first_four(candidates, partner_counter=None):
    """
    休憩ローテーション上の待機順、上位4人をそのまま採用する（スキルバランスは
    「誰を選ぶか」には一切使わない）。選ばれた4人をどう2チームに分けるかは、
    まず3通りのパターンの中で最も実力差が小さいものを選ぶ。実力差がほぼ
    同点の複数パターンがある場合は、直近のパートナー履歴が少ない方を優先する
    （対戦相手の重複は見ない。実力差自体を崩してまで回避することはない）。
    """
    def conservative(e):
        return float(e.get("skill_score", 50.0)) - 3 * float(e.get("skill_sigma", 8.333))

    four = candidates[:4]
    scores = [conservative(e) for e in four]
    pairing_patterns = [((0, 1), (2, 3)), ((0, 2), (1, 3)), ((0, 3), (1, 2))]

    options = []
    for (i1, i2), (i3, i4) in pairing_patterns:
        diff = abs((scores[i1] + scores[i2]) - (scores[i3] + scores[i4]))
        options.append((diff, [four[i1], four[i2]], [four[i3], four[i4]]))

    min_diff = min(o[0] for o in options)

    if partner_counter:
        margin = max(BALANCE_TIEBREAK_MARGIN_FLOOR, min_diff * BALANCE_TIEBREAK_MARGIN_RATIO)
        near_best = [o for o in options if o[0] <= min_diff + margin]
        diff, team_a, team_b = min(near_best, key=lambda o: _partner_penalty(o[1], o[2], partner_counter))
    else:
        diff, team_a, team_b = min(options, key=lambda o: o[0])

    return team_a, team_b, diff


def _full_random_four(candidates, force_top_n=1, partner_counter=None):
    """
    完全ランダムモード。ただし待機順(candidatesの先頭)最上位force_top_n人は
    必ず含める。旧システム(game/views.py)の「誰が休むかは先にキューだけで
    決め、スキルバランスはその後」という考え方に合わせ、「誰が出るか」を
    モードの抽選に完全に委ねきってしまわないようにするための最低保証。
    残りの枠は実力・待機順に関係なく完全ランダムに選ぶ。
    チーム分けは_fairness_first_fourと同様に実力差が最小になる組み合わせを
    選ぶ（「調整はする」）。partner_counterを渡せば、実力差がほぼ同点の
    場合にパートナー重複が少ない方を優先する。
    """
    import random as _random

    must_include = list(candidates[:force_top_n])
    rest_pool = list(candidates[force_top_n:])
    remaining_needed = 4 - len(must_include)

    if len(rest_pool) >= remaining_needed:
        chosen_rest = _random.sample(rest_pool, remaining_needed)
    else:
        chosen_rest = rest_pool

    four = must_include + chosen_rest
    if len(four) < 4:
        four = candidates[:4]
    return _fairness_first_four(four, partner_counter=partner_counter)


def _skill_sorted_pending(entry_table):
    """pending中の人をスキルスコア(conservative)降順で返す（休憩ローテーションは無視）"""
    def conservative(e):
        return float(e.get("skill_score", 50.0)) - 3 * float(e.get("skill_sigma", 8.333))

    pending = entry_table.scan(FilterExpression=Attr("entry_status").eq("pending"), ConsistentRead=True).get("Items", [])
    return sorted(pending, key=conservative, reverse=True)


SKILL_PRIORITY_SPREAD_THRESHOLD = 20  # 選ばれた4人の実力差(最大-最小)がこれを超えたら「外れ値あり」とみなす


def _skill_priority_four(candidates, partner_counter=None):
    """
    実力優先モード: 休憩ローテーションは無視し、待機中のスキルスコアが
    最も高い人たちを中心に選ぶ。

    まず、待機中で最もスキルが高い人を基準に、そこから
    SKILL_PRIORITY_SPREAD_THRESHOLD以内の人だけに候補を絞ってから上位4人を
    選ぶ。これにより「極端にスキルが低い人」が候補に混ざらないようにする
    （例えば1人だけ飛び抜けて強い人がいる場合、その人がいつも同じ「2番目に
    強い人」とばかりパートナーになり続けてしまう問題を避けるため。実力が
    近い候補が4人に満たない場合は、従来通り単純に上位4人を使う）。

    チーム分けはハイブリッド方式:
    - 選ばれた4人の実力差(最大-最小)がSKILL_PRIORITY_SPREAD_THRESHOLD以下
      （皆ほぼ同レベル）の場合は、定番の「1位+4位 vs 2位+3位」（チーム間
      合計差が最小になる組み方）でペアリングする。実力差がほぼ同点の
      複数パターンがある場合は、直近のパートナー履歴が少ない方を優先する。
    - それを超える場合（候補を絞ってもなお実力差が大きい、など）は、
      単純にチーム間合計差だけを最小化すると上級者+初級者 vs 中級者+中級者
      のような組み合わせが選ばれてしまうため、まず「チーム内の実力差が
      大きい方」を最小化し、それが同じ場合に限りチーム間の合計差、
      さらに同じ場合はパートナー履歴で決める。
    """
    def conservative(e):
        return float(e.get("skill_score", 50.0)) - 3 * float(e.get("skill_sigma", 8.333))

    sorted_all = sorted(candidates, key=conservative, reverse=True)
    top_skill = conservative(sorted_all[0])
    close_pool = [e for e in sorted_all if top_skill - conservative(e) <= SKILL_PRIORITY_SPREAD_THRESHOLD]
    four = close_pool[:4] if len(close_pool) >= 4 else sorted_all[:4]
    scores = [conservative(e) for e in four]
    spread = max(scores) - min(scores)
    pairing_patterns = [((0, 1), (2, 3)), ((0, 2), (1, 3)), ((0, 3), (1, 2))]

    if spread <= SKILL_PRIORITY_SPREAD_THRESHOLD:
        options = []
        for (i1, i2), (i3, i4) in pairing_patterns:
            diff = abs((scores[i1] + scores[i2]) - (scores[i3] + scores[i4]))
            options.append((diff, [four[i1], four[i2]], [four[i3], four[i4]]))
        min_diff = min(o[0] for o in options)
        if partner_counter:
            margin = max(BALANCE_TIEBREAK_MARGIN_FLOOR, min_diff * BALANCE_TIEBREAK_MARGIN_RATIO)
            near_best = [o for o in options if o[0] <= min_diff + margin]
            diff, team_a, team_b = min(near_best, key=lambda o: _partner_penalty(o[1], o[2], partner_counter))
        else:
            diff, team_a, team_b = min(options, key=lambda o: o[0])
        return team_a, team_b, diff

    options2 = []
    for (i1, i2), (i3, i4) in pairing_patterns:
        within_team_a = abs(scores[i1] - scores[i2])
        within_team_b = abs(scores[i3] - scores[i4])
        between_diff = abs((scores[i1] + scores[i2]) - (scores[i3] + scores[i4]))
        options2.append(((max(within_team_a, within_team_b), between_diff), [four[i1], four[i2]], [four[i3], four[i4]]))

    min_key = min(o[0] for o in options2)
    if partner_counter:
        near_best2 = [o for o in options2 if o[0] == min_key]
        key, team_a, team_b = min(near_best2, key=lambda o: _partner_penalty(o[1], o[2], partner_counter))
    else:
        key, team_a, team_b = min(options2, key=lambda o: o[0])

    return team_a, team_b, key[1]


ENABLE_RESCUE_MODE = False  # 救済モードを使うかどうか(いったんオフ。2026-09-29時点)
WAIT_RESCUE_THRESHOLD = 5  # 何回の補充機会を待たされたら救済モードで強制的に含めるか
QUEUE_FORCE_COUNT = 1  # 完全ランダム/AIペアリングで、永続キューの先頭から必ず含める人数

# モード選定ルール(ステップ数の固定サイクルではなく、時間ベース):
#   1. 練習開始直後、INITIAL_FULL_RANDOM_COUNT回はランダム
#   2. 続くINITIAL_AI_PURE_COUNT回は調整なしの純粋なAIペアリング
#   3. それ以降は、参加回数の多い人を除外する「調整AIペアリング」のみ
#      (以前は3回ごとにオン/オフを切り替えていたが、常時オンに変更)
#   4. 前回のスキルモード一斉入れ替えからPRE_SKILL_BALANCE_LEAD_MINUTES分
#      経過したら、続くPRE_SKILL_BALANCE_REFILLS回だけ「強調整AIペアリング」
#      (除外+最も少ない人を強制参加)の助走を行い、参加回数を一度揃えてから
#      スキルモード一斉入れ替えに入る(実力バランスだけで組む一斉入れ替えの
#      直前に、参加回数の偏りをリセットしておく狙い)。助走が終わると即座に
#      スキルモード一斉入れ替えの収集を開始する(_try_refill_court/
#      _process_skill_burst/_execute_skill_burstを参照)。結果として、
#      スキルモード一斉入れ替え自体はPRE_SKILL_BALANCE_LEAD_MINUTES分より
#      やや後(助走の数試合分だけ遅れて)に発動する。一斉入れ替えで作られた
#      試合が個別に終わったあとは、通常の1コートずつの補充に戻る。
# 休みの調整は二重構成:
#   1. ランダム/AIペアリングは、_pop_next_from_play_queue()による永続
#      キューの先頭QUEUE_FORCE_COUNT人を毎回必ず含める(旧システムと同じ
#      「休む人を先に決める」発想。通常時の穏やかな公平性)
#   2. さらにモードを問わず、WAIT_RESCUE_THRESHOLD回以上補充を逃し続けている
#      人がいれば救済モードが割り込み、最大4人まで強制的に含める(極端な
#      長時間待ちを防ぐ保険。ENABLE_RESCUE_MODEでオン/オフ切替可)
#   3. さらに、調整AIペアリングの期間は毎回、参加回数が最も多い人を
#      今回の候補から除外する「継続的バランス調整」を行う(休憩にはしない。
#      次回はまた対象になりうる)
#   4. ただし、スキルモード一斉入れ替え前の助走期間(上記4)だけ、
#      「最も少ない人を強制参加」も併用する「強調整AIペアリング」を行う
INITIAL_FULL_RANDOM_COUNT = 6  # 練習開始直後、ランダムを連続させる回数
INITIAL_AI_PURE_COUNT = 6  # ランダムの後、調整なしの純粋なAIペアリングを連続させる回数
SKILL_BURST_INTERVAL_MINUTES = 60  # 参考値(実際の間隔は助走込みでこれよりやや長くなる)
PRE_SKILL_BALANCE_LEAD_MINUTES = 45  # 前回のスキル優先から何分後に強調整AIペアリングの助走を始めるか
PRE_SKILL_BALANCE_REFILLS = 3  # 助走(強調整AIペアリング)を何回の補充分行うか


def _next_refill_mode(meta_table):
    """
    補充のたびに meta#continuous_pairing の refill_count を加算し、次のモードを決める。
    スキルモード一斉入れ替えはここでは扱わない(_try_refill_court /
    _process_skill_burst が、通常の1コート補充より先に横取りする)。
    """
    resp = meta_table.update_item(
        Key={"match_id": META_PAIRING_PK},
        UpdateExpression="ADD refill_count :one",
        ExpressionAttributeValues={":one": 1},
        ReturnValues="UPDATED_NEW",
    )
    refill_count = int(resp["Attributes"]["refill_count"])

    if refill_count <= INITIAL_FULL_RANDOM_COUNT:
        return "full_random", refill_count

    return "ai_pairing", refill_count


def _skill_burst_should_collect(meta_current, pairing_meta):
    """
    このタイミングで空いたコートを、スキルモード一斉入れ替えの収集対象
    (awaiting_skill_burst)に加えるべきかどうかを判定する。

    - 既に収集が始まっている(awaiting_skill_burstに1つでもコートがある)
      場合は、経過時間に関わらず最後まで合流させる(全コートが揃うまで待つ)。
    - まだ始まっていなければ、強調整AIペアリングの助走(pre_skill_remaining)
      を使い切ったかどうかで、新規に収集を開始すべきか判定する
      (助走が始まっていなければFalse、使い切っていればTrue)。
    """
    if meta_current.get("awaiting_skill_burst"):
        return True
    pre_skill_remaining = pairing_meta.get("pre_skill_remaining")
    if pre_skill_remaining is None:
        return False
    return int(pre_skill_remaining) <= 0


COURT_REFILL_DELAY_SECONDS = 15  # スコア送信から次の組み合わせ開始までの猶予（休憩したい人が申告できる時間）
LOW_BUFFER_THRESHOLD = 2  # 待機バッファがこの人数以下なら、単独補充せずペア待ちにする
PAIR_HOLD_MAX_WAIT_SECONDS = 60  # ペア相手が来ない場合、単独補充に切り替えるまでの最大待ち時間


def _try_refill_court(old_match_id, court_number):
    """
    1コート分の結果が確定した直後に呼ぶ。
    その4人をTrueSkill更新してpending化する（次の組み合わせはすぐには
    作らず、コートを「空き」として記録するだけ）。実際の次の組み合わせ選定は
    _process_awaiting_refills() が、COURT_REFILL_DELAY_SECONDS 経過後に行う。
    これにより、試合を終えた人が休憩ボタンを押す時間的猶予ができる。
    """
    entry_table = _entry_table()
    results_table = _results_table()
    meta_table = _meta_table()

    finished_entries = entry_table.scan(
        FilterExpression=Attr("match_id").eq(str(old_match_id))
        & Attr("court_number").eq(court_number)
        & Attr("entry_status").eq("playing"),
        ConsistentRead=True,
    ).get("Items", [])

    if len(finished_entries) != 4:
        current_app.logger.warning(
            "[game2][continuous] court=%s の4人が揃っていません(%d人)。補充をスキップ",
            court_number, len(finished_entries),
        )
        return

    result = results_table.get_item(Key={"result_id": f"{old_match_id}#{court_number}"}).get("Item")
    if not result:
        return

    player_mapping = {e["user_id"]: e["entry_id"] for e in finished_entries if "user_id" in e}

    # ★救済モード用: 「今から待機に入る」時点でのrefill_countを記録しておき、
    #   後で(次にrefill_countがいくつ進んだか)=待たされた補充回数として使う
    pairing_meta_before = meta_table.get_item(
        Key={"match_id": META_PAIRING_PK}, ConsistentRead=True
    ).get("Item", {}) or {}
    refill_count_at_pending = int(pairing_meta_before.get("refill_count", 0))

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
            "match_id": old_match_id, "court_number": court_number,
            "team1_score": result.get("team1_score"), "team2_score": result.get("team2_score"),
        }
        updated_skills = update_trueskill_for_players_and_return_updates(result_item)
        sync_match_entries_with_updated_skills2(player_mapping, updated_skills)
        persist_skill_to_bad_users(updated_skills)
    except Exception as e:
        current_app.logger.error("[game2][continuous] スキル更新エラー (court=%s): %s", court_number, e)

    now_jst = datetime.now(JST).isoformat()
    for e in finished_entries:
        entry_id = e["entry_id"]
        if e.get("rest_requested"):
            entry_table.update_item(
                Key={"entry_id": entry_id},
                UpdateExpression=(
                    "SET entry_status=:resting, updated_at=:now, "
                    "rest_count = if_not_exists(rest_count, :zero) + :one "
                    "REMOVE court_number, team, match_id, rest_requested"
                ),
                ExpressionAttributeValues={":resting": "resting", ":now": now_jst, ":zero": 0, ":one": 1},
            )
        else:
            entry_table.update_item(
                Key={"entry_id": entry_id},
                UpdateExpression=(
                    "SET entry_status=:pending, updated_at=:now, "
                    "match_count = if_not_exists(match_count, :zero) + :one, "
                    "pending_since_refill_count = :rc "
                    "REMOVE court_number, team, match_id"
                ),
                ExpressionAttributeValues={
                    ":pending": "pending", ":now": now_jst, ":zero": 0, ":one": 1,
                    ":rc": refill_count_at_pending,
                },
            )

    meta_current_now = meta_table.get_item(Key={"match_id": META_CURRENT_PK}, ConsistentRead=True).get("Item", {}) or {}
    court_count = int(meta_current_now.get("court_count", 0) or 0)

    # ★スキルモード一斉入れ替えの前に、強調整AIペアリングの助走を挟む:
    #   前回のスキル優先からPRE_SKILL_BALANCE_LEAD_MINUTES分経過していて、
    #   まだ助走を始めていなければ、PRE_SKILL_BALANCE_REFILLS回分の助走を
    #   開始する(pre_skill_remainingをセット)。以降の通常補充
    #   (_select_and_start_court)はこのカウントを見て強調整AIペアリングを
    #   適用し、使い切ったら自動的に0になる。
    pairing_meta_now = meta_table.get_item(Key={"match_id": META_PAIRING_PK}, ConsistentRead=True).get("Item", {}) or {}
    if pairing_meta_now.get("pre_skill_remaining") is None:
        last_burst_at_iso = pairing_meta_now.get("last_skill_burst_at")
        if last_burst_at_iso:
            try:
                last_burst_at = datetime.fromisoformat(last_burst_at_iso)
                elapsed_minutes = (datetime.now(JST) - last_burst_at).total_seconds() / 60
            except Exception:
                elapsed_minutes = 0
            if elapsed_minutes >= PRE_SKILL_BALANCE_LEAD_MINUTES:
                try:
                    meta_table.update_item(
                        Key={"match_id": META_PAIRING_PK},
                        UpdateExpression="SET pre_skill_remaining = :n",
                        ConditionExpression="attribute_not_exists(pre_skill_remaining)",
                        ExpressionAttributeValues={":n": PRE_SKILL_BALANCE_REFILLS},
                    )
                    pairing_meta_now["pre_skill_remaining"] = PRE_SKILL_BALANCE_REFILLS
                    current_app.logger.info(
                        "[game2][continuous] 前回のスキル優先から%d分経過。強調整AIペアリングの助走(%d回)を開始",
                        int(elapsed_minutes), PRE_SKILL_BALANCE_REFILLS,
                    )
                except ClientError as e:
                    if e.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                        raise

    # ★スキルモード一斉入れ替え: 助走(強調整AIペアリング)を使い切った場合、
    #   このコートは単独では補充せず、全コートが空くまで待つ
    #   (awaiting_skill_burstに登録)。全コート揃ったら_process_skill_burst()
    #   が待機中全員をスキル順に並べて一括で組み直す。既に収集が始まっている
    #   場合は、経過時間に関わらず最後まで合流させる(バッファが少ない場合の
    #   ペア待ちより優先する)。
    if _skill_burst_should_collect(meta_current_now, pairing_meta_now):
        meta_table.update_item(
            Key={"match_id": META_CURRENT_PK},
            UpdateExpression="SET awaiting_skill_burst = if_not_exists(awaiting_skill_burst, :empty)",
            ExpressionAttributeValues={":empty": {}},
        )
        meta_table.update_item(
            Key={"match_id": META_CURRENT_PK},
            UpdateExpression="SET awaiting_skill_burst.#c = :now",
            ExpressionAttributeNames={"#c": str(court_number)},
            ExpressionAttributeValues={":now": now_jst},
        )
        current_app.logger.info(
            "[game2][continuous] court=%s スキルモード一斉入れ替えのため待機に登録", court_number,
        )
        return

    # ★参加人数が少ないと、待機バッファがほぼ無く、空いたコートを単独で
    #   即補充してもほぼ同じ顔ぶれがそのまま戻ってくるだけになってしまう
    #   （例: 8〜10人で2コート、12〜14人で3コートなど）。
    #   このコートを除いた「今すぐ試合に戻れる人数(バッファ)」が
    #   LOW_BUFFER_THRESHOLD人以下の場合は、単独では補充せず、もう1コート
    #   分空くのを待ってから2コート分まとめて組み直すことで、混ざり合う
    #   余地を作る。
    active_items = entry_table.scan(
        FilterExpression=Attr("entry_status").is_in(["pending", "playing"]), ConsistentRead=True
    ).get("Items", [])
    buffer = len(active_items) - court_count * 4

    if court_count >= 2 and buffer <= LOW_BUFFER_THRESHOLD:
        meta_table.update_item(
            Key={"match_id": META_CURRENT_PK},
            UpdateExpression="SET held_for_pairing = if_not_exists(held_for_pairing, :empty)",
            ExpressionAttributeValues={":empty": {}},
        )
        meta_table.update_item(
            Key={"match_id": META_CURRENT_PK},
            UpdateExpression="SET held_for_pairing.#c = :now",
            ExpressionAttributeNames={"#c": str(court_number)},
            ExpressionAttributeValues={":now": now_jst},
        )
        current_app.logger.info(
            "[game2][continuous] court=%s 待機バッファが少ない(buffer=%d)ためペア待ちに登録",
            court_number, buffer,
        )
        return

    # ★次の組み合わせはすぐには作らず、このコートを「空き」として記録するだけ。
    #   実際の補充は _process_awaiting_refills() が COURT_REFILL_DELAY_SECONDS
    #   経過後に行う（休憩したい人が申告する時間を確保するため）。
    #   awaiting_refill はマップ属性なので、無い場合にネストしたSETが失敗しない
    #   よう、先に空マップとして存在を保証してから値を入れる(2段階)。
    meta_table.update_item(
        Key={"match_id": META_CURRENT_PK},
        UpdateExpression="SET awaiting_refill = if_not_exists(awaiting_refill, :empty)",
        ExpressionAttributeValues={":empty": {}},
    )
    meta_table.update_item(
        Key={"match_id": META_CURRENT_PK},
        UpdateExpression="SET awaiting_refill.#c = :now",
        ExpressionAttributeNames={"#c": str(court_number)},
        ExpressionAttributeValues={":now": now_jst},
    )
    current_app.logger.info(
        "[game2][continuous] court=%s 空き待ち登録（%d秒後に自動補充）",
        court_number, COURT_REFILL_DELAY_SECONDS,
    )


def _select_and_start_court(court_number, clear_awaiting_refill=True):
    """
    空いているコートに、休憩ローテーション上位の候補の中から実力バランスが
    良い4人を選んで新しい試合を発行する。_process_awaiting_refills() から
    COURT_REFILL_DELAY_SECONDS 経過後に呼ばれる。

    clear_awaiting_refill=False の場合は、トランザクションにawaiting_refill
    解除を含めない（_process_held_pairs()からの呼び出し用。こちらは
    held_for_pairingを対象にした別の競合ガード(_clear_held_for_pairing)を
    既に済ませているため）。
    """
    entry_table = _entry_table()
    results_table = _results_table()
    meta_table = _meta_table()
    now_jst = datetime.now(JST).isoformat()

    meta_current = meta_table.get_item(Key={"match_id": META_CURRENT_PK}, ConsistentRead=True).get("Item", {}) or {}
    if meta_current.get("matching_paused"):
        current_app.logger.info(
            "[game2][continuous] court=%s マッチング停止中のため補充をスキップ", court_number
        )
        return

    mode, refill_count = _next_refill_mode(meta_table)
    all_pending = _all_pending_unordered(entry_table)
    continuous_balance_applied = False
    full_balance_applied = False

    # ★完全ランダムは練習開始直後のINITIAL_FULL_RANDOM_COUNT回だけ使う、
    #   あえて何の調整もしないシンプルなモード。救済モード・永続キューに
    #   よる強制・継続的バランス調整は一切適用せず、待機中から純粋に
    #   ランダムに4人選ぶ(チーム分けの実力差調整のみ行う)。
    if mode == "full_random":
        rescued = []
        if len(all_pending) < 4:
            current_app.logger.info(
                "[game2][continuous] court=%s 補充する人数が足りないため空けたままにします(候補%d人)",
                court_number, len(all_pending),
            )
            return
        partner_counter, _opponent_counter = _get_recent_pair_history2(results_table)
        team_a_entries, team_b_entries, diff = _full_random_four(
            all_pending, force_top_n=0, partner_counter=partner_counter
        )
    else:
        # ★救済モード: WAIT_RESCUE_THRESHOLD回以上、補充のチャンスを逃し続けて
        #   いる人がいれば、強制的に含める（極端な長時間待ちを防ぐ保険。
        #   永続キューによる穏やかな公平性とは別枠で併存させる）。
        #   ENABLE_RESCUE_MODE=Falseの間はいったん無効化(2026-09-29時点)。
        rescued = sorted(
            [
                p for p in all_pending
                if refill_count - int(p.get("pending_since_refill_count", refill_count)) >= WAIT_RESCUE_THRESHOLD
            ],
            key=lambda p: int(p.get("pending_since_refill_count", refill_count)),
        )[:4] if ENABLE_RESCUE_MODE else []

        if rescued:
            rescued_uids = {p["entry_id"] for p in rescued}
            others = [p for p in all_pending if p["entry_id"] not in rescued_uids]
            candidates = rescued + others
            if len(candidates) < 4:
                current_app.logger.info(
                    "[game2][continuous] court=%s 補充する人数が足りないため空けたままにします(候補%d人)",
                    court_number, len(candidates),
                )
                return
            partner_counter, opponent_counter = _get_recent_pair_history2(results_table)
            team_a_entries, team_b_entries, diff = _best_balanced_four(
                candidates, partner_counter, opponent_counter, force_top_n=len(rescued)
            )
            current_app.logger.info(
                "[game2][continuous] court=%s 救済モード発動: %d人を強制的に含める(%s)",
                court_number, len(rescued), [p.get("display_name") for p in rescued],
            )
        else:
            # AIペアリング: 永続キューの先頭QUEUE_FORCE_COUNT人を必ず含める
            # (旧システムと同じ「休む人を先に決める」発想。通常時の公平性)
            forced = _pop_next_from_play_queue(entry_table, meta_table, count=QUEUE_FORCE_COUNT)
            if not forced:
                current_app.logger.info(
                    "[game2][continuous] court=%s 補充する人数が足りないため空けたままにします",
                    court_number,
                )
                return
            forced_uids = {p["user_id"] for p in forced}

            # ★継続的バランス調整: 練習開始直後のランダムINITIAL_FULL_RANDOM_COUNT回・
            #   純粋なAIペアリングINITIAL_AI_PURE_COUNT回が終わった後は、毎回
            #   参加回数(match_count)が最も多い人を今回の候補から除外する
            #   (休憩にはしない。次回はまた対象になりうる)。
            #   以前は「最も少ない人を強制参加」も併用していたが、遅れて参加
            #   した人がmatch_count=0のため毎回最優先で拾われてしまう懸念があり、
            #   シミュレーションで検証の上、除外のみに変更した(永続キュー自体が
            #   参加回数の少ない人を優先する仕組みを既に持っているため、除外
            #   だけでも一定の公平性は保てる)。
            apply_continuous_balance = refill_count > (INITIAL_FULL_RANDOM_COUNT + INITIAL_AI_PURE_COUNT)
            continuous_balance_applied = apply_continuous_balance

            # ★強調整(スキルモード一斉入れ替え前の助走期間、pre_skill_remaining
            #   が残っている間だけ): 除外のみでは是正しきれない偏りを一気に
            #   縮めるため、この期間だけ「最も少ない人を強制参加」も併用する
            #   (除外のみを続けているとその場の巡り合わせで偏りが蓄積するため、
            #   スキル優先の直前に一度、強めに補正する)。
            pairing_meta_for_balance = meta_table.get_item(
                Key={"match_id": META_PAIRING_PK}, ConsistentRead=True
            ).get("Item", {}) or {}
            pre_skill_remaining = pairing_meta_for_balance.get("pre_skill_remaining")
            apply_full_balance = pre_skill_remaining is not None and int(pre_skill_remaining) > 0
            full_balance_applied = False

            excluded_uid = None
            if apply_continuous_balance:
                if apply_full_balance:
                    remaining_low = [e for e in all_pending if e.get("user_id") not in forced_uids]
                    if remaining_low:
                        lowest = min(remaining_low, key=lambda e: int(e.get("match_count", 0) or 0))
                        forced = forced + [lowest]
                        forced_uids.add(lowest["user_id"])
                        full_balance_applied = True
                    meta_table.update_item(
                        Key={"match_id": META_PAIRING_PK},
                        UpdateExpression="ADD pre_skill_remaining :neg1",
                        ExpressionAttributeValues={":neg1": -1},
                    )

                remaining2 = [e for e in all_pending if e.get("user_id") not in forced_uids]
                excluded_display_name = None
                if remaining2:
                    highest = max(remaining2, key=lambda e: int(e.get("match_count", 0) or 0))
                    excluded_uid = highest["user_id"]
                    excluded_display_name = highest.get("display_name")
                current_app.logger.info(
                    "[game2][continuous] court=%s %s(refill_count=%d): 優先=%s 除外=%s",
                    court_number,
                    "強調整AIペアリング" if full_balance_applied else "継続的バランス調整",
                    refill_count,
                    lowest.get("display_name") if apply_full_balance and remaining_low else None,
                    excluded_display_name,
                )

            rest_pool = [
                e for e in all_pending
                if e.get("user_id") not in forced_uids and e.get("user_id") != excluded_uid
            ]
            candidates = forced + rest_pool
            if len(candidates) < 4:
                # ★除外すると4人未満になってしまう場合は、除外をやめて通常通りにする
                rest_pool = [e for e in all_pending if e.get("user_id") not in forced_uids]
                candidates = forced + rest_pool

            if len(candidates) < 4:
                current_app.logger.info(
                    "[game2][continuous] court=%s 補充する人数が足りないため空けたままにします(候補%d人)",
                    court_number, len(candidates),
                )
                return

            partner_counter, opponent_counter = _get_recent_pair_history2(results_table)
            team_a_entries, team_b_entries, diff = _best_balanced_four(
                candidates, partner_counter, opponent_counter, force_top_n=len(forced)
            )

    if rescued:
        used_mode = "safety_valve"
    elif full_balance_applied:
        used_mode = "ai_pairing_full_balanced"
    elif continuous_balance_applied:
        used_mode = "ai_pairing_balanced"
    else:
        used_mode = mode
    new_match_id = generate_match_id2()

    import boto3
    dynamodb_client = boto3.client("dynamodb", region_name="ap-northeast-1")
    tx_items = []
    if clear_awaiting_refill:
        tx_items.append({
            "Update": {
                "TableName": "bad-game-matches",
                "Key": {"match_id": {"S": META_CURRENT_PK}},
                # ★同じコートに対して_select_and_start_courtが同時に2回呼ばれた場合
                #   (例: ページ読み込みとsubmit_score直後の呼び出しが重なった場合)、
                #   ConditionExpressionが無いとREMOVEは「既に無い属性の削除」を
                #   エラーにせず黙って成功させてしまい、両方の呼び出しが別々の
                #   4人を選んで同じコート番号に試合を作ってしまう(表示が
                #   8人になるバグの原因になった)。attribute_existsを条件にする
                #   ことで、後から来た方のトランザクションを確実に失敗させる。
                "UpdateExpression": "REMOVE awaiting_refill.#c",
                "ConditionExpression": "attribute_exists(awaiting_refill.#c)",
                "ExpressionAttributeNames": {"#c": str(court_number)},
            }
        })
    for pl, team in [(team_a_entries[0], "A"), (team_a_entries[1], "A"),
                      (team_b_entries[0], "B"), (team_b_entries[1], "B")]:
        tx_items.append({
            "Update": {
                "TableName": "bad-game2-match_entries",
                "Key": {"entry_id": {"S": pl["entry_id"]}},
                "UpdateExpression": "SET entry_status=:playing, match_id=:mid, court_number=:c, team=:t, updated_at=:now, pairing_mode=:pm",
                "ConditionExpression": "entry_status = :pending",
                "ExpressionAttributeValues": {
                    ":playing": {"S": "playing"}, ":pending": {"S": "pending"},
                    ":mid": {"S": str(new_match_id)}, ":c": {"N": str(court_number)},
                    ":t": {"S": team}, ":now": {"S": now_jst}, ":pm": {"S": used_mode},
                },
            }
        })

    try:
        dynamodb_client.transact_write_items(TransactItems=tx_items)
        current_app.logger.info(
            "[game2][continuous] court=%s 自動補充(mode=%s, refill_count=%d): new_match_id=%s balance_diff=%.2f members=%s",
            court_number, mode, refill_count, new_match_id, diff,
            [e.get("display_name") for e in team_a_entries + team_b_entries],
        )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "TransactionCanceledException":
            current_app.logger.warning(
                "[game2][continuous] court=%s 補充tx競合のためスキップ（次のチェックで再試行される）", court_number
            )
        else:
            raise


def _process_awaiting_refills():
    """
    「空き」として記録されているコートのうち、COURT_REFILL_DELAY_SECONDS
    以上経過したものを実際に補充する。court()ページの読み込みや
    submit_score()の直後など、頻繁に呼ばれる場所から都度チェックする
    （バックグラウンドジョブは使わず、ポーリング的に処理する）。
    """
    meta_table = _meta_table()
    meta_current = meta_table.get_item(Key={"match_id": META_CURRENT_PK}, ConsistentRead=True).get("Item", {}) or {}
    awaiting = meta_current.get("awaiting_refill") or {}
    if not awaiting:
        return

    now = datetime.now(JST)
    for court_str, freed_at_iso in list(awaiting.items()):
        try:
            freed_at = datetime.fromisoformat(freed_at_iso)
        except Exception:
            continue
        elapsed = (now - freed_at).total_seconds()
        if elapsed >= COURT_REFILL_DELAY_SECONDS:
            try:
                _select_and_start_court(int(court_str))
            except Exception as e:
                current_app.logger.error(
                    "[game2][continuous] court=%s 補充処理でエラー: %s", court_str, e, exc_info=True
                )


NEW_COURT_MIN_PENDING = 4  # 待機中がこの人数以上溜まったら新しいコートを開く


def _process_new_court_opportunity():
    """
    練習中に参加者が増えた場合、組み合わせ作成時に指定したコート数上限
    (max_courts)まで、新しいコートを自動的に開く。

    継続補充方式では、court_countは組み合わせ作成時点の人数から決まる
    固定値のまま変わらない設計だったため、後から参加者がゆっくり集まって
    くる実際の運用(シミュレーションのように最初から全員揃っているのとは
    違う)では、待機人数が1コート分(4人)以上に達しても新しいコートが
    永遠に作られない不具合があった。ここで、待機人数が十分になり次第
    court_countをmax_courtsの範囲内で1つずつ増やし、通常のawaiting_refill
    経路(_process_awaiting_refills)に乗せて補充させる。
    """
    meta_table = _meta_table()
    entry_table = _entry_table()
    meta_current = meta_table.get_item(Key={"match_id": META_CURRENT_PK}, ConsistentRead=True).get("Item", {}) or {}
    if meta_current.get("status") != "playing":
        return
    if meta_current.get("matching_paused"):
        return

    court_count = int(meta_current.get("court_count", 0) or 0)
    max_courts = int(meta_current.get("max_courts", court_count) or court_count)
    if court_count >= max_courts:
        return

    # ★開設済みだがまだ実際に人が割り当てられていないコート(awaiting_refill /
    #   held_for_pairing)がある場合、その分の4人ずつは既に「予約済み」として
    #   待機人数から差し引く。差し引かずに数えると、まだ補充されていない
    #   コートの分の待機者を「新しいコートを開くのに十分な人数」として
    #   二重にカウントしてしまい、実際には補充できない人数のまま次のコートを
    #   開いてしまう(コートが永遠に空いたままになる不具合の原因だった)。
    awaiting_refill = meta_current.get("awaiting_refill") or {}
    held_for_pairing = meta_current.get("held_for_pairing") or {}
    reserved = 4 * (len(awaiting_refill) + len(held_for_pairing))

    pending = _all_pending_unordered(entry_table)
    if len(pending) - reserved < NEW_COURT_MIN_PENDING:
        return

    new_court_number = court_count + 1
    try:
        meta_table.update_item(
            Key={"match_id": META_CURRENT_PK},
            UpdateExpression="SET court_count = :new",
            ConditionExpression="court_count = :old",
            ExpressionAttributeValues={":new": new_court_number, ":old": court_count},
        )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return  # 他のリクエストが既に増やした(競合)
        raise

    now_jst = datetime.now(JST).isoformat()
    meta_table.update_item(
        Key={"match_id": META_CURRENT_PK},
        UpdateExpression="SET awaiting_refill = if_not_exists(awaiting_refill, :empty)",
        ExpressionAttributeValues={":empty": {}},
    )
    meta_table.update_item(
        Key={"match_id": META_CURRENT_PK},
        UpdateExpression="SET awaiting_refill.#c = :now",
        ExpressionAttributeNames={"#c": str(new_court_number)},
        ExpressionAttributeValues={":now": now_jst},
    )
    current_app.logger.info(
        "[game2][continuous] 待機%d人に達したため新しいコート%dを開きます"
        "(上限%d、%d秒後に補充)",
        len(pending), new_court_number, max_courts, COURT_REFILL_DELAY_SECONDS,
    )


def _reconcile_orphaned_courts():
    """
    court_countが実際のコート状況とズレて、誰も割り当てられておらず、
    どの待ち行列(awaiting_refill/held_for_pairing/awaiting_skill_burst)にも
    登録されていない「迷子のコート」がないか毎回チェックする。

    本来、court_countを増やす処理(_process_new_court_opportunity)は
    同時にそのコート番号をawaiting_refillへ必ず登録するので迷子は
    起きないはずだが、_select_and_start_courtの補充トランザクションが
    競合(TransactionCanceledException)した場合など、まれに
    awaiting_refillから外れたのに実際の補充は成功していない、という
    状態になりうる。一度そうなると、そのコートを補充するきっかけが
    どこにも無くなり永遠に空いたまま放置されてしまう
    (2026-09-29の実運用で実際に発生した不具合)。ここで毎回軽くチェックし、
    見つかり次第awaiting_refillに登録し直して通常の補充経路に乗せる。
    """
    meta_table = _meta_table()
    entry_table = _entry_table()
    meta_current = meta_table.get_item(Key={"match_id": META_CURRENT_PK}, ConsistentRead=True).get("Item", {}) or {}
    if meta_current.get("status") != "playing":
        return

    court_count = int(meta_current.get("court_count", 0) or 0)
    if court_count <= 0:
        return

    tracked = set()
    for key in ("awaiting_refill", "held_for_pairing", "awaiting_skill_burst"):
        tracked |= set((meta_current.get(key) or {}).keys())

    playing_courts = set()
    for e in entry_table.scan(
        FilterExpression=Attr("entry_status").eq("playing"), ConsistentRead=True
    ).get("Items", []):
        if e.get("court_number") is not None:
            playing_courts.add(int(e["court_number"]))

    now_jst = datetime.now(JST).isoformat()
    for court_num in range(1, court_count + 1):
        if court_num in playing_courts or str(court_num) in tracked:
            continue
        meta_table.update_item(
            Key={"match_id": META_CURRENT_PK},
            UpdateExpression="SET awaiting_refill = if_not_exists(awaiting_refill, :empty)",
            ExpressionAttributeValues={":empty": {}},
        )
        meta_table.update_item(
            Key={"match_id": META_CURRENT_PK},
            UpdateExpression="SET awaiting_refill.#c = :now",
            ExpressionAttributeNames={"#c": str(court_num)},
            ExpressionAttributeValues={":now": now_jst},
        )
        current_app.logger.warning(
            "[game2][continuous] court=%s 迷子状態(誰も割り当てられず待ち行列にも未登録)を検知、補充待ちに再登録しました",
            court_num,
        )


def _clear_held_for_pairing(meta_table, court_number):
    """
    held_for_pairingから指定コートを取り除く。取り除けたらTrue、既に他の処理で
    取り除かれていた(競合)場合はFalseを返す。呼び出し側はFalseの場合、
    そのコートへの後続処理(_select_and_start_court)を行ってはいけない
    （既に別の処理がこのコートを担当している）。
    """
    try:
        meta_table.update_item(
            Key={"match_id": META_CURRENT_PK},
            UpdateExpression="REMOVE held_for_pairing.#c",
            ConditionExpression="attribute_exists(held_for_pairing.#c)",
            ExpressionAttributeNames={"#c": str(court_number)},
        )
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise


def _process_held_pairs():
    """
    待機バッファが少ないため単独補充を保留されている(held_for_pairing)コートを
    処理する。2コート分揃い、かつ最初に保留されたコートからCOURT_REFILL_DELAY_
    SECONDS以上経過していれば、その2コートをまとめて処理する。
    実際には_select_and_start_courtを2回順番に呼ぶだけでよい。1回目の呼び出しで
    そのコートの分だけ待機プールから抜けるので、2回目の呼び出し時には両コート
    分の人がまとめて候補になっており、自然に混ざり合った組み合わせになる。

    片方のコートだけがPAIR_HOLD_MAX_WAIT_SECONDS以上待たされている場合
    (ペア相手がなかなか現れない場合)は、待たせすぎないよう単独ででも補充する。
    """
    meta_table = _meta_table()
    meta_current = meta_table.get_item(Key={"match_id": META_CURRENT_PK}, ConsistentRead=True).get("Item", {}) or {}
    held = meta_current.get("held_for_pairing") or {}
    if not held:
        return

    now = datetime.now(JST)
    held_list = []
    for court_str, freed_at_iso in held.items():
        try:
            freed_at = datetime.fromisoformat(freed_at_iso)
        except Exception:
            continue
        held_list.append((int(court_str), freed_at))
    held_list.sort(key=lambda x: x[1])  # 古い順

    if len(held_list) >= 2:
        oldest_court, oldest_freed_at = held_list[0]
        if (now - oldest_freed_at).total_seconds() >= COURT_REFILL_DELAY_SECONDS:
            pair = held_list[:2]
            current_app.logger.info(
                "[game2][continuous] ペア補充実行: コート%s とコート%s をまとめて組み直します",
                pair[0][0], pair[1][0],
            )
            for court_num, _freed_at in pair:
                if not _clear_held_for_pairing(meta_table, court_num):
                    current_app.logger.info(
                        "[game2][continuous] court=%s 既に他の処理が担当済みのためスキップ", court_num,
                    )
                    continue
                try:
                    _select_and_start_court(court_num, clear_awaiting_refill=False)
                except Exception as e:
                    current_app.logger.error(
                        "[game2][continuous] court=%s ペア補充処理でエラー: %s", court_num, e, exc_info=True
                    )
            return

    # 安全弁: ペア相手が来ないまま長時間待たされているコートは単独ででも補充する
    for court_num, freed_at in held_list:
        if (now - freed_at).total_seconds() >= PAIR_HOLD_MAX_WAIT_SECONDS:
            if not _clear_held_for_pairing(meta_table, court_num):
                continue
            current_app.logger.info(
                "[game2][continuous] court=%s ペア相手が来ないため単独補充します", court_num,
            )
            try:
                _select_and_start_court(court_num, clear_awaiting_refill=False)
            except Exception as e:
                current_app.logger.error(
                    "[game2][continuous] court=%s 単独補充処理でエラー: %s", court_num, e, exc_info=True
                )


SKILL_BURST_MAX_WAIT_SECONDS = 300  # スキルモード一斉入れ替えで、揃わないコートを何秒まで待つか(安全弁)


def _mark_skill_burst_consumed(meta_table, now_jst_iso):
    """
    このスキルモード一斉入れ替えを消費済みにする(次はPRE_SKILL_BALANCE_LEAD_
    MINUTES分後に強調整AIペアリングの助走から再スタート)。pre_skill_remaining
    も削除し、次のサイクルで再度助走を開始できるようにする。
    """
    meta_table.update_item(
        Key={"match_id": META_PAIRING_PK},
        UpdateExpression="SET last_skill_burst_at = :now REMOVE pre_skill_remaining",
        ExpressionAttributeValues={":now": now_jst_iso},
    )


def _return_courts_to_awaiting_refill(meta_table, court_numbers, now_jst_iso):
    """スキルモード一斉入れ替えの対象から外れたコートを、通常の空き待ちに戻す。"""
    for c in court_numbers:
        meta_table.update_item(
            Key={"match_id": META_CURRENT_PK},
            UpdateExpression="SET awaiting_refill = if_not_exists(awaiting_refill, :empty)",
            ExpressionAttributeValues={":empty": {}},
        )
        meta_table.update_item(
            Key={"match_id": META_CURRENT_PK},
            UpdateExpression="SET awaiting_refill.#c = :now",
            ExpressionAttributeNames={"#c": str(c)},
            ExpressionAttributeValues={":now": now_jst_iso},
        )


def _execute_skill_burst(court_numbers):
    """
    スキルモード一斉入れ替え: 指定された全コート分をまとめて、待機中全員を
    スキル順に並べ、上から4人ずつの「層」ごとに一括で組み直す(一番実力が
    高い層をコート番号が最も小さいコートに、以下降順に割り当てる)。

    court_numbersはawaiting_skill_burstに集まった、今まさに空いた全コート。
    まず、この呼び出しが対象コートの「担当」であることを、awaiting_skill_burst
    からの一括削除(全コート分をまとめて1つの条件付き更新で)によって確定させる。
    一部でも既に他の処理に取られていれば(競合)、この回は何もせず見送る
    (次のポーリングで状態を見直して再試行される)。

    実行後は、このバーストを消費済み扱いにする(_mark_skill_burst_consumed)。
    以降、この一斉入れ替えで作られた試合が個別に終わったコートは、通常の
    1コートずつの補充(_try_refill_court→awaiting_refill)に自然に戻る
    (_next_refill_modeはこのバースト機構と無関係にfull_random/ai_pairingしか
    返さないため)。
    """
    entry_table = _entry_table()
    results_table = _results_table()
    meta_table = _meta_table()
    now_jst = datetime.now(JST).isoformat()

    court_numbers = sorted(court_numbers)
    remove_expr = "REMOVE " + ", ".join(f"awaiting_skill_burst.#c{i}" for i in range(len(court_numbers)))
    condition_expr = " AND ".join(f"attribute_exists(awaiting_skill_burst.#c{i})" for i in range(len(court_numbers)))
    names = {f"#c{i}": str(c) for i, c in enumerate(court_numbers)}
    try:
        meta_table.update_item(
            Key={"match_id": META_CURRENT_PK},
            UpdateExpression=remove_expr,
            ConditionExpression=condition_expr,
            ExpressionAttributeNames=names,
        )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            current_app.logger.info(
                "[game2][continuous] スキルモード一斉入れ替え: 既に他の処理が担当済みのため見送り"
            )
            return
        raise

    candidates = _skill_sorted_pending(entry_table)
    usable_groups = min(len(court_numbers), len(candidates) // 4)

    if usable_groups == 0:
        current_app.logger.info(
            "[game2][continuous] スキルモード一斉入れ替え: 補充する人数が足りないため見送ります(候補%d人)",
            len(candidates),
        )
        _mark_skill_burst_consumed(meta_table, now_jst)
        _return_courts_to_awaiting_refill(meta_table, court_numbers, now_jst)
        return

    partner_counter, _opponent_counter = _get_recent_pair_history2(results_table)

    import boto3
    dynamodb_client = boto3.client("dynamodb", region_name="ap-northeast-1")
    tx_items = []
    assigned = []
    for i in range(usable_groups):
        court_number = court_numbers[i]
        four = candidates[i * 4:(i + 1) * 4]
        team_a_entries, team_b_entries, diff = _skill_priority_four(four, partner_counter=partner_counter)
        new_match_id = generate_match_id2()
        for pl, team in [(team_a_entries[0], "A"), (team_a_entries[1], "A"),
                          (team_b_entries[0], "B"), (team_b_entries[1], "B")]:
            tx_items.append({
                "Update": {
                    "TableName": "bad-game2-match_entries",
                    "Key": {"entry_id": {"S": pl["entry_id"]}},
                    "UpdateExpression": "SET entry_status=:playing, match_id=:mid, court_number=:c, team=:t, updated_at=:now, pairing_mode=:pm",
                    "ConditionExpression": "entry_status = :pending",
                    "ExpressionAttributeValues": {
                        ":playing": {"S": "playing"}, ":pending": {"S": "pending"},
                        ":mid": {"S": str(new_match_id)}, ":c": {"N": str(court_number)},
                        ":t": {"S": team}, ":now": {"S": now_jst}, ":pm": {"S": "balance_only"},
                    },
                }
            })
        assigned.append((court_number, new_match_id, diff, [e.get("display_name") for e in team_a_entries + team_b_entries]))

    try:
        dynamodb_client.transact_write_items(TransactItems=tx_items)
        for court_number, new_match_id, diff, names_list in assigned:
            current_app.logger.info(
                "[game2][continuous] court=%s スキルモード一斉入れ替え補充: new_match_id=%s balance_diff=%.2f members=%s",
                court_number, new_match_id, diff, names_list,
            )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "TransactionCanceledException":
            current_app.logger.warning(
                "[game2][continuous] スキルモード一斉入れ替えtx競合のため見送り(通常補充に戻します)"
            )
            _return_courts_to_awaiting_refill(meta_table, court_numbers, now_jst)
            return
        raise

    # ★人数不足で一部のコートしか埋められなかった場合、残りは通常の空き待ちに戻す
    leftover_courts = court_numbers[usable_groups:]
    if leftover_courts:
        _return_courts_to_awaiting_refill(meta_table, leftover_courts, now_jst)

    _mark_skill_burst_consumed(meta_table, now_jst)


def _process_skill_burst():
    """
    スキルモード一斉入れ替えの収集(awaiting_skill_burst)を処理する。現在の
    コート数(court_count)分すべてのコートが集まったら、待機中全員をスキル
    順に並べて一括で組み直す。長時間コートが揃わない場合の安全弁として、
    最初に空いたコートからSKILL_BURST_MAX_WAIT_SECONDS経過していれば、
    揃っていないコート数のままでも実行する。
    """
    meta_table = _meta_table()
    meta_current = meta_table.get_item(Key={"match_id": META_CURRENT_PK}, ConsistentRead=True).get("Item", {}) or {}
    awaiting = meta_current.get("awaiting_skill_burst") or {}
    if not awaiting:
        return

    court_count = int(meta_current.get("court_count", 0) or 0)
    now = datetime.now(JST)
    collected = []
    for court_str, freed_at_iso in awaiting.items():
        try:
            freed_at = datetime.fromisoformat(freed_at_iso)
        except Exception:
            continue
        collected.append((int(court_str), freed_at))
    if not collected:
        return
    collected.sort(key=lambda x: x[1])  # 古い順

    oldest_waited = (now - collected[0][1]).total_seconds()
    newest_waited = (now - collected[-1][1]).total_seconds()
    ready = court_count > 0 and len(collected) >= court_count
    timed_out = oldest_waited >= SKILL_BURST_MAX_WAIT_SECONDS

    if not (ready or timed_out):
        return
    if newest_waited < COURT_REFILL_DELAY_SECONDS:
        # 最後に空いたコートの、休憩したい人が申告する猶予をまだ確保中
        return

    court_numbers = [c for c, _ in collected]
    current_app.logger.info(
        "[game2][continuous] スキルモード一斉入れ替え実行: courts=%s (ready=%s timed_out=%s)",
        court_numbers, ready, timed_out,
    )
    try:
        _execute_skill_burst(court_numbers)
    except Exception as e:
        current_app.logger.error(
            "[game2][continuous] スキルモード一斉入れ替えでエラー: %s", e, exc_info=True
        )


@bp_game2.route("/submit_score/<match_id>/court/<int:court_number>", methods=["POST"])
@login_required
def submit_score(match_id, court_number):
    try:
        team1_raw = request.form.get("team1_score")
        team2_raw = request.form.get("team2_score")
        if team1_raw is None or team2_raw is None:
            flash("スコアが送信されていません", "danger")
            return redirect(url_for("game2.court"))
        team1_score = int(team1_raw)
        team2_score = int(team2_raw)
        if team1_score == team2_score:
            flash("スコアが同点です。勝者を決めてください。", "danger")
            return redirect(url_for("game2.court"))

        winner = "A" if team1_score > team2_score else "B"
        court_number_int = int(court_number)

        entry_table = _entry_table()
        entries = entry_table.scan(
            FilterExpression=Attr("match_id").eq(str(match_id)) & Attr("court_number").eq(court_number_int),
            ConsistentRead=True,
        ).get("Items", [])
        if not entries:
            flash("コートのエントリーが見つかりません（すでに次の試合に切り替わった可能性があります）", "warning")
            return redirect(url_for("game2.court"))

        if not current_user.administrator:
            court_user_ids = [str(e.get("user_id", "")) for e in entries]
            if current_user.user_id not in court_user_ids:
                flash("このコートへのスコア送信権限がありません", "danger")
                return redirect(url_for("game2.court"))

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
            flash("コートのチームデータが不完全です", "danger")
            return redirect(url_for("game2.court"))

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
                return redirect(url_for("game2.court"))
            raise

        current_app.logger.info(
            "[game2] Score: Match=%s, Court=%d, %d-%d (Win:%s)",
            match_id, court_number_int, team1_score, team2_score, winner,
        )

        # ★Phase2: このコートの結果が確定したら、そのコートを「空き」として記録する
        #   （実際の次の組み合わせはCOURT_REFILL_DELAY_SECONDS秒後）
        try:
            _try_refill_court(match_id, court_number_int)
        except Exception as e:
            current_app.logger.error(
                "[game2][continuous] court=%s 自動補充でエラー: %s", court_number_int, e, exc_info=True
            )

        # ついでに、他のコートで猶予時間を過ぎているものがあれば処理しておく
        try:
            _process_awaiting_refills()
        except Exception as e:
            current_app.logger.error("[game2] _process_awaiting_refills エラー: %s", e, exc_info=True)
        try:
            _process_held_pairs()
        except Exception as e:
            current_app.logger.error("[game2] _process_held_pairs エラー: %s", e, exc_info=True)
        try:
            _process_skill_burst()
        except Exception as e:
            current_app.logger.error("[game2] _process_skill_burst エラー: %s", e, exc_info=True)
        try:
            _process_new_court_opportunity()
        except Exception as e:
            current_app.logger.error("[game2] _process_new_court_opportunity エラー: %s", e, exc_info=True)
        try:
            _reconcile_orphaned_courts()
        except Exception as e:
            current_app.logger.error("[game2] _reconcile_orphaned_courts エラー: %s", e, exc_info=True)

        flash(f"コート{court_number_int}のスコアを送信しました（{team1_score}-{team2_score}）", "success")
        return redirect(url_for("game2.court"))
    except Exception as e:
        current_app.logger.error("[game2][submit_score ERROR] %s", str(e), exc_info=True)
        flash("スコアの送信中にエラーが発生しました", "danger")
        return redirect(url_for("game2.court"))


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


@bp_game2.route("/stop_matching", methods=["POST"])
@login_required
def stop_matching():
    """
    マッチング停止ボタン: 新規の自動補充を止める。進行中の試合はそのまま
    続行できるが、スコアが送信されたコートはそれ以降、空いたままになる
    （_select_and_start_courtがmatching_pausedを見て補充をスキップする）。
    全コートの試合が終わった後、「組み合わせを作成」を押せば
    matching_pausedを自動クリアして再開できる。そのまま終わりにしたい
    場合は「練習終了」を押す。
    """
    if not current_user.administrator:
        flash("管理者のみ実行できます。", "danger")
        return redirect(url_for("game2.court"))

    meta_table = _meta_table()
    meta_table.update_item(
        Key={"match_id": META_CURRENT_PK},
        UpdateExpression="SET matching_paused = :true",
        ExpressionAttributeValues={":true": True},
    )
    current_app.logger.info("[game2][stop_matching] by=%s", current_user.get_id())
    flash("マッチングを停止しました。進行中の試合のスコアを送信すると、それ以降そのコートは再マッチングされません。全コートが終わったら「組み合わせを作成」で再開できます。", "info")
    return redirect(url_for("game2.court"))


@bp_game2.route("/resume_matching", methods=["POST"])
@login_required
def resume_matching():
    """stop_matchingで止めた自動補充を、新しい組み合わせを作らずそのまま再開する"""
    if not current_user.administrator:
        flash("管理者のみ実行できます。", "danger")
        return redirect(url_for("game2.court"))

    meta_table = _meta_table()
    meta_table.update_item(
        Key={"match_id": META_CURRENT_PK},
        UpdateExpression="REMOVE matching_paused",
    )
    current_app.logger.info("[game2][resume_matching] by=%s", current_user.get_id())
    flash("マッチングを再開しました。", "info")
    return redirect(url_for("game2.court"))


@bp_game2.route("/reset_participants", methods=["POST"])
@login_required
def reset_participants():
    """
    練習終了ボタン: テストコートの全エントリーを削除し、休憩ローテーション
    (continuous_rest_queue) とペアリングサイクル(cycle_index/refill_count)を
    リセットする。既存システム（bad-game-*）には一切触れない。
    """
    if not current_user.administrator:
        flash("管理者のみ実行できます。", "danger")
        return redirect(url_for("game2.court"))

    if has_ongoing_matches2():
        flash(
            "まだ試合中のコートがあるため、練習を終了できません。"
            "先に全コートのスコアを送信するか、「緊急: 全員を通常コートへ移動」で"
            "試合を終了してから、もう一度お試しください。",
            "danger",
        )
        return redirect(url_for("game2.court"))

    entry_table = _entry_table()
    meta_table = _meta_table()

    try:
        items = entry_table.scan().get("Items", [])
        deleted_count = 0
        for item in items:
            entry_table.delete_item(Key={"entry_id": item["entry_id"]})
            deleted_count += 1

        meta_table.update_item(
            Key={"match_id": META_CURRENT_PK},
            UpdateExpression="SET #st = :idle REMOVE current_match_id, court_count, max_courts, awaiting_refill, matching_paused, held_for_pairing, awaiting_skill_burst",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={":idle": "idle"},
        )

        meta_table.delete_item(Key={"match_id": _rest_queue_pk(REST_QUEUE_KEY)})

        meta_table.update_item(
            Key={"match_id": META_PAIRING_PK},
            UpdateExpression=(
                "SET cycle_index = :zero, refill_count = :zero "
                "REMOVE last_mode, last_match_id, last_skill_burst_at, pre_skill_remaining"
            ),
            ExpressionAttributeValues={":zero": 0},
        )

        current_app.logger.info(
            "[game2][reset_participants] 全エントリー削除: %d件 by %s",
            deleted_count, current_user.get_id(),
        )
        flash(f"テストコート: 練習を終了しました（{deleted_count}件のエントリーとローテーションをリセット）", "success")
    except Exception as e:
        current_app.logger.error("[game2][reset_participants] エラー: %s", e, exc_info=True)
        flash("練習終了処理中にエラーが発生しました", "danger")

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

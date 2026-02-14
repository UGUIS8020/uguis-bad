from flask import Blueprint, render_template, redirect, url_for, flash, request, current_app
from flask_login import login_required, current_user
import boto3
import uuid
from datetime import datetime, date, time, timedelta, timezone
import random
from boto3.dynamodb.conditions import Key, Attr, And
from flask import jsonify
from flask import session
from .game_utils import update_trueskill_for_players_and_return_updates, parse_players, Player, generate_balanced_pairs_and_matches
from .game_utils import start_match_meta, get_current_match_id, sync_match_entries_with_updated_skills
from utils.timezone import JST
import re
from decimal import Decimal
import time
import logging
from botocore.exceptions import ClientError
from zoneinfo import ZoneInfo
import json

JST = ZoneInfo("Asia/Tokyo")

bp_game = Blueprint('game', __name__)


# DynamoDBリソース取得
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
match_table = dynamodb.Table('bad-game-match_entries')
game_meta_table = dynamodb.Table('bad-game-matches')
user_table = dynamodb.Table("bad-users")

    
def _scan_all(table, **kwargs):
    """
    DynamoDB scan を全ページ取得して返す。
    """
    items = []
    resp = table.scan(**kwargs)
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"], **kwargs)
        items.extend(resp.get("Items", []))
    return items


@bp_game.route("/court")
@login_required
def court():
    logger = current_app.logger

    try:
        logger.info("=== コート入場開始 ===")

        # セッション初期値
        session.setdefault("score_format", "21")

        match_table = current_app.dynamodb.Table("bad-game-match_entries")

        # 全ステータスのプレイヤーを取得（scanは全ページ取得）
        items = _scan_all(
            match_table,
            FilterExpression=Attr("entry_status").is_in(["pending", "resting", "playing"]),
            ConsistentRead=True
        )

        # --- デフォルト値補完（ログは出さない） ---
        for it in items:
            it["rest_count"]  = it.get("rest_count")  or 0
            it["match_count"] = it.get("match_count") or 0
            it["join_count"]  = it.get("join_count")  or 0

        # --- ステータス別に分類 ---
        pending_players  = [it for it in items if it.get("entry_status") == "pending"]
        resting_players  = [it for it in items if it.get("entry_status") == "resting"]
        playing_players  = [it for it in items if it.get("entry_status") == "playing"]

        # INFOは “件数だけ”
        logger.info(
            "players total=%d pending=%d resting=%d playing=%d",
            len(items), len(pending_players), len(resting_players), len(playing_players)
        )

        # --- ユーザー状態の判定 ---
        user_id = current_user.get_id()

        # any() を3回回すより、user_entries を先に拾って使い回す
        user_entries = [it for it in items if it.get("user_id") == user_id]

        is_registered = any(it.get("entry_status") == "pending" for it in user_entries)
        is_resting    = any(it.get("entry_status") == "resting" for it in user_entries)
        is_playing    = any(it.get("entry_status") == "playing" for it in user_entries)

        # スキル / 試合回数（見つからなければデフォルト）
        if user_entries:
            skill_score = user_entries[0].get("skill_score", 50)
            match_count = user_entries[0].get("match_count", 0) or 0
        else:
            skill_score = 50
            match_count = 0

        # --- 進行中試合関連 ---
        # ここで複数回 scan する可能性があるので、ログは INFO最小・詳細はDEBUGのみ
        has_ongoing = has_ongoing_matches()
        completed, total = get_match_progress()
        current_courts = get_current_match_status()

        logger.debug("has_ongoing_matches=%s match_progress=%s/%s", has_ongoing, completed, total)

        # 試合情報の取得（match_id が無い時は INFO 1本だけ）
        match_id = get_latest_match_id()
        if not match_id:
            logger.info("進行中の試合はありません")
            match_courts = {}
        else:
            logger.info("ongoing match_id=%s", match_id)
            match_courts = get_organized_match_data(match_id)
            logger.debug("match_courts keys=%d", len(match_courts))

        return render_template(
            "game/court.html",
            pending_players=pending_players,
            resting_players=resting_players,
            playing_players=playing_players,
            is_registered=is_registered,
            is_resting=is_resting,
            is_playing=is_playing,
            current_user_skill_score=skill_score,
            current_user_match_count=match_count,
            match_courts=match_courts,
            match_id=match_id,
            has_ongoing_matches=has_ongoing,
            completed_matches=completed,
            total_matches=total,
            current_courts=current_courts,
        )

    except Exception:
        # 例外ログは1本でスタックトレースまで出る
        logger.exception("コート入場エラー")
        return "コート画面でエラーが発生しました", 500

    
def _now_utc_iso():
    return datetime.now(timezone.utc).isoformat()

def _since_iso(hours=12):
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat(timespec="milliseconds")

def _scan_all(table, **kwargs):
    """DynamoDB Scanのページネーション吸収（必要最小限で）"""
    items = []
    resp = table.scan(**kwargs)
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
    return items

def _has_entries_for_match(match_table, match_id):
    resp = match_table.scan(
        ProjectionExpression="entry_status",
        FilterExpression=Attr("match_id").eq(match_id) & Attr("entry_status").eq("playing"),
        Limit=1,
        ConsistentRead=True,
    )
    items = resp.get("Items", [])
    current_app.logger.info(f" _has_entries_for_match({match_id}): {len(items)}件のplayingエントリ")
    return bool(items)

# def get_latest_match_id(hours_window=12):
#     """'playing' が残っている最新の match_id を返す（なければ None）"""
#     current_app.logger.info(" get_latest_match_id 開始")

#     match_table  = current_app.dynamodb.Table("bad-game-match_entries")
#     result_table = current_app.dynamodb.Table("bad-game-results")

#     since = _since_iso(hours_window)

#     # 1) まず 'playing' のエントリーから探す（最新優先）
#     current_app.logger.info("ステップ1: 進行中(playing)の試合を探す")
#     playing_items = _scan_all(
#         match_table,
#         ProjectionExpression="match_id, entry_status, created_at",
#         FilterExpression=Attr("entry_status").eq("playing") & Attr("created_at").gt(since),
#         ConsistentRead=True  
#     )
#     current_app.logger.info(f"進行中のプレイヤー数: {len(playing_items)}")

#     if playing_items:
#         playing_items.sort(key=lambda x: x.get("created_at", ""), reverse=True)
#         seen = set()
#         for it in playing_items:
#             mid = it.get("match_id")
#             if not mid or mid in seen:
#                 continue
#             seen.add(mid)
#             # その match_id に playing が1件以上あることを確認
#             if _has_entries_for_match(match_table, mid):
#                 current_app.logger.info(f"進行中の試合ID: {mid}")
#                 return mid

#     # 2) （保険）結果テーブル側を新しい順に当たり、playing が残っているものだけ採用
#     current_app.logger.info("ステップ2: 結果テーブルから最新の試合を取得（playing確認つき）")
#     result_items = _scan_all(
#         result_table,
#         ProjectionExpression="match_id, created_at",
#         FilterExpression=Attr("created_at").gt(since)
#     )
#     current_app.logger.info(f" 結果テーブルのアイテム数(最近{hours_window}h): {len(result_items)}")

#     if result_items:
#         result_items.sort(key=lambda x: x.get("created_at", ""), reverse=True)
#         current_app.logger.info(f"結果テーブルのmatch_id例: {[r.get('match_id') for r in result_items[:10]]}")
#         seen = set()
#         for r in result_items:
#             mid = r.get("match_id")
#             if not mid or mid in seen:
#                 continue
#             seen.add(mid)
#             if _has_entries_for_match(match_table, mid):  # ← playing が残っている試合のみOK
#                 current_app.logger.info(f"結果テーブルからの最新試合ID(playing有): {mid}")
#                 return mid

#     current_app.logger.info("進行中の試合はありません")
#     return None

def get_latest_match_id(hours_window=12):
    """
    進行中の match_id を返す（なければ None）

    優先順位:
      1) メタアイテム (bad-game-matches / match_id="meta#current") の current_match_id
      2) 旧方式: match_entries を scan(playing) して時刻が新しいもの
    """
    logger = current_app.logger

    # ----------------------------
    # 1) メタ方式（最優先）
    # ----------------------------
    try:
        meta_table = current_app.dynamodb.Table("bad-game-matches")
        meta_pk = "meta#current"

        meta_resp = meta_table.get_item(
            Key={"match_id": meta_pk},
            ConsistentRead=True
        )
        meta = meta_resp.get("Item")

        if meta:
            status = meta.get("status")
            current_match_id = meta.get("current_match_id")

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "[get_latest_match_id] meta found status=%s current_match_id=%s",
                    status, current_match_id
                )

            if status == "playing":
                if current_match_id:
                    logger.info("[get_latest_match_id] ongoing match_id=%s (meta)", current_match_id)
                    return current_match_id
                else:
                    # playing なのに current_match_id が空は不整合なので警告してフォールバック
                    logger.warning("[get_latest_match_id] meta status=playing but current_match_id is empty -> fallback scan")
            # status が playing でない → フォールバックへ
        else:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("[get_latest_match_id] meta not found -> fallback scan")

    except Exception as e:
        # メタ取得に失敗しても旧方式で拾う
        logger.warning("[get_latest_match_id] meta read failed -> fallback scan: %s", e)

    # ----------------------------
    # 2) 旧方式（フォールバック）
    # ----------------------------
    match_table = current_app.dynamodb.Table("bad-game-match_entries")
    since = _since_iso(hours_window)

    try:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "[get_latest_match_id] scan playing entries since=%s hours_window=%s",
                since, hours_window
            )

        # created_at が無いデータが混ざっても落ちにくいように候補を多めに取る
        playing_items = _scan_all(
            match_table,
            ProjectionExpression="match_id, entry_status, created_at, updated_at, joined_at",
            FilterExpression=Attr("entry_status").eq("playing"),
            ConsistentRead=True
        )

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("[get_latest_match_id] playing_items=%d", len(playing_items))

        if not playing_items:
            logger.debug("[get_latest_match_id] no ongoing match")
            return None

        # match_id が空のものは除外
        playing_items = [it for it in playing_items if it.get("match_id")]
        if not playing_items:
            logger.warning("[get_latest_match_id] playing entries exist but all have empty match_id")
            return None

        # created_at が無い場合は updated_at -> joined_at の順で使う
        def sort_key(it):
            for k in ("created_at", "updated_at", "joined_at"):
                v = it.get(k)
                if isinstance(v, str) and v:
                    return v
            return ""

        latest = max(playing_items, key=sort_key)
        match_id = latest.get("match_id")

        # さらに一応
        if not match_id:
            logger.warning("[get_latest_match_id] found playing entry but match_id is empty")
            return None

        logger.info("[get_latest_match_id] ongoing match_id=%s (scan)", match_id)
        return match_id

    except Exception as e:
        logger.exception("[get_latest_match_id] error: %s", e)
        return None

def get_match_players_by_court(match_id):
    """指定された試合IDに対するコート別のプレイヤー構成を取得"""
    match_table = current_app.dynamodb.Table("bad-game-match_entries")
    current_app.logger.info(f" 試合情報取得開始: match_id={match_id}")

    players = _scan_all(
        match_table,
        ProjectionExpression=(
            "user_id, display_name, skill_score, gender, organization, badminton_experience, "
            "match_id, entry_status, court_number, team, team_name, team_side"
        ),
        FilterExpression=Attr("match_id").eq(match_id) & Attr("entry_status").eq("playing")
    )
    current_app.logger.info(f"試合プレイヤー取得: {len(players)}人")

    for i, p in enumerate(players):
        current_app.logger.info(f"プレイヤー{i+1}の全フィールド: {p}")
        current_app.logger.info(f"利用可能なキー: {list(p.keys())}")

    courts = {}

    def norm_team(val: str) -> str | None:
        if not val:
            return None
        s = str(val).strip().lower()
        if s in ("a", "team_a", "left"):
            return "A"
        if s in ("b", "team_b", "right"):
            return "B"
        return None

    for p in players:
        court_raw = p.get("court_number")
        if not court_raw:
            current_app.logger.warning(f"⚠️ court_numberが見つかりません: {p}")
            continue
        try:
            court_num = int(str(court_raw).strip())
        except (ValueError, TypeError):
            current_app.logger.warning(f"無効なコート番号: {court_raw}")
            continue

        team_raw = p.get("team") or p.get("team_name") or p.get("team_side")
        team_norm = norm_team(team_raw)

        player_info = {
            "user_id": p.get("user_id"),
            "display_name": p.get("display_name", "匿名"),
            "skill_score": int(p.get("skill_score", 0) or 0),
            "gender": p.get("gender", "unknown"),
            "organization": p.get("organization", ""),
            "badminton_experience": p.get("badminton_experience", "")
        }

        if court_num not in courts:
            courts[court_num] = {"court_number": court_num, "team_a": [], "team_b": []}

        # 1) team_norm が A/B のときはそれに従う
        if team_norm == "A":
            courts[court_num]["team_a"].append(player_info)
        elif team_norm == "B":
            courts[court_num]["team_b"].append(player_info)
        else:
            # 2) 情報が無いときは人数バランスで
            if len(courts[court_num]["team_a"]) <= len(courts[court_num]["team_b"]):
                courts[court_num]["team_a"].append(player_info)
            else:
                courts[court_num]["team_b"].append(player_info)

    current_app.logger.info(f"構築されたコート情報: {len(courts)}面")
    for court_num, court_info in sorted(courts.items()):
        current_app.logger.info(
            f"コート{court_num}: チームA={len(court_info['team_a'])}人, チームB={len(court_info['team_b'])}人"
        )
    return courts     


@bp_game.route("/api/court_status")
@login_required
def court_status_api():
    """コート状況のAPIエンドポイント"""
    try:
        pending_players = get_pending_players()
        resting_players = get_resting_players()
        
        return jsonify({
            'pending_count': len(pending_players),
            'resting_count': len(resting_players),
            'entry_status': 'success'
        })
    except Exception as e:
        current_app.logger.error(f"コート状況API エラー: {str(e)}")
        return jsonify({'error': str(e), 'status': 'error'}), 500
    

# def get_pending_players()
# def get_resting_players
# get_user_status
# をまとめるコード

#get_players_status
#主にコートの参加者（参加中 or 休憩中）のリスト表示やフィルタに使う。
#user_id を指定した場合は、ログイン中ユーザーの status を確認する目的にも使える

def get_players_status(status, user_id=None, debug_dump_all=False, debug_sample=3):
    """
    status のエントリーを取得して返す。
    - 通常ログは件数のみ（INFO）
    - 詳細は DEBUG（サンプルだけ）
    - 全件scanのダンプは debug_dump_all=True の時だけ（強いデバッグ用）
    """
    logger = current_app.logger

    try:
        match_table = current_app.dynamodb.Table("bad-game-match_entries")

        # --- 強いデバッグ：全件scanして中身を見る（普段はOFF） ---
        if debug_dump_all and logger.isEnabledFor(logging.DEBUG):
            all_items = []
            resp = match_table.scan()
            all_items.extend(resp.get("Items", []))

            while "LastEvaluatedKey" in resp:
                resp = match_table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
                all_items.extend(resp.get("Items", []))

            logger.debug("[get_players_status] dump_all total=%d", len(all_items))

            # 出しすぎ防止：最大20件、かつ debug_sample 件は最低保証
            cap = min(len(all_items), max(1, min(20, int(debug_sample) if debug_sample else 3)))
            for it in all_items[:cap]:
                logger.debug(
                    "[all] name=%s status=%s user_id=%s entry_id=%s",
                    it.get("display_name"),
                    it.get("entry_status"),
                    it.get("user_id"),
                    it.get("entry_id"),
                )

        # --- 本処理：必要なものだけ取得 ---
        filter_expr = Attr("entry_status").eq(status)
        if user_id:
            filter_expr = filter_expr & Attr("user_id").eq(user_id)

        items = []
        resp = match_table.scan(FilterExpression=filter_expr)
        items.extend(resp.get("Items", []))

        while "LastEvaluatedKey" in resp:
            resp = match_table.scan(
                FilterExpression=filter_expr,
                ExclusiveStartKey=resp["LastEvaluatedKey"],
            )
            items.extend(resp.get("Items", []))

        # --- INFO：件数だけ ---
        logger.info(
            "[get_players_status] status=%s user_id=%s count=%d",
            status,
            user_id or "-",
            len(items),
        )

        # --- DEBUG：先頭サンプルだけ ---
        if logger.isEnabledFor(logging.DEBUG):
            sample_n = max(0, int(debug_sample) if debug_sample else 0)
            for it in items[:sample_n]:
                logger.debug(
                    "[%s] sample name=%s user_id=%s entry_id=%s rest=%s match=%s join=%s",
                    status,
                    it.get("display_name"),
                    it.get("user_id"),
                    it.get("entry_id"),
                    it.get("rest_count"),
                    it.get("match_count"),
                    it.get("join_count"),
                )

        # --- デフォルト値補完（ログは出さない） ---
        for it in items:
            it["rest_count"] = it.get("rest_count") if it.get("rest_count") is not None else 0
            it["match_count"] = it.get("match_count") if it.get("match_count") is not None else 0
            it["join_count"] = it.get("join_count") if it.get("join_count") is not None else 0

        return items

    except Exception as e:
        logger.exception(
            "[get_players_status] error status=%s user_id=%s: %s",
            status,
            user_id or "-",
            e,
        )
        return []
    
    #get_current_user_status
    #現在のログインユーザーの状態だけ取得(1人（ログインユーザー)
    #表示やボタン制御などテンプレートで多用
def get_current_user_status():
    """現在のユーザーの登録状態、休憩状態、スキルスコアを取得"""
    user_id = current_user.get_id()

    # 登録中 or 休憩中の判定    
    is_registered = bool(get_players_status('pending', user_id))
    is_resting = bool(get_players_status('resting', user_id))
    

    # スキルスコア取得（優先順：active > resting > user_table）
    skill_score = None
    for status in ['pending', 'resting']:
        result = get_players_status(status, user_id)
        if result:
            skill_score = result[0].get('skill_score')
            break

    if skill_score is None:
        user_response = user_table.get_item(Key={"user#user_id": user_id})
        skill_score = user_response.get("Item", {}).get("skill_score", 50)

    return {
        'is_registered': is_registered,
        'is_resting': is_resting,
        'skill_score': skill_score
    }


def get_pending_players():
    """参加待ちプレイヤーを取得（match_idは見ない／entry_statusのみ）"""
    try:
        today = date.today().isoformat()
        match_table   = current_app.dynamodb.Table("bad-game-match_entries")
        history_table = current_app.dynamodb.Table("bad-users-history")
        user_table    = current_app.dynamodb.Table("bad-users")

        # ✅entry_statusのみでフィルタ。メタ行は除外。強整合読みを推奨
        resp = match_table.scan(
            FilterExpression=Attr('entry_status').eq('pending') & ~Attr('entry_id').contains('meta'),
            ConsistentRead=True,
        )
        items = resp.get('Items', [])

        players = []
        for item in items:
            user_id = item.get('user_id')
            if not user_id:
                continue

            # ユーザー詳細
            uresp = user_table.get_item(Key={"user#user_id": user_id})
            user_data = uresp.get("Item", {})

            # 参加回数（履歴）
            try:
                hresp = history_table.scan(FilterExpression=Attr('user_id').eq(user_id))
                history_items = hresp.get('Items', [])
                join_count = sum(1 for h in history_items if h.get('date') and h['date'] < today)
            except Exception as e:
                current_app.logger.warning(f"[履歴取得エラー] user_id={user_id}: {e}")
                join_count = 0

            # 👇 skill_score と skill_sigma を取得
            skill_score = float(item.get('skill_score', user_data.get('skill_score', 50.0)))
            skill_sigma = float(item.get('skill_sigma', user_data.get('skill_sigma', 8.333)))
            
            # 👇 保守的スキルを計算
            conservative_skill = skill_score - 3 * skill_sigma

            players.append({
                'entry_id': item.get('entry_id'),
                'user_id': user_id,
                'display_name': item.get('display_name', user_data.get('display_name', '不明')),
                'skill_score': skill_score,              # 👈 μ
                'skill_sigma': skill_sigma,              # 👈 σ（追加）
                'conservative_skill': conservative_skill, # 👈 μ - 3σ（追加）
                'badminton_experience': user_data.get('badminton_experience', '未設定'),
                'joined_at': item.get('joined_at'),
                'rest_count': item.get('rest_count', 0),
                'match_count': item.get('match_count', 0),
                'join_count': join_count,
            })

        # 参加時刻でソート
        players.sort(key=lambda x: x.get('joined_at') or "")

        current_app.logger.info(f"[PENDING PLAYERS] 表示件数: {len(players)}")
        for p in players:
            current_app.logger.info(
                f"  - {p['display_name']}（μ={p['skill_score']:.2f}, σ={p['skill_sigma']:.4f}, "
                f"保守的={p['conservative_skill']:.2f}）参加時刻: {p.get('joined_at')}"
            )

        return players

    except Exception as e:
        current_app.logger.error(f"参加待ちプレイヤー取得エラー: {e}")
        return []
    

def get_resting_players():
    """休憩中プレイヤーを取得（match_idは見ない／entry_statusのみ）"""
    try:
        today = date.today().isoformat()

        match_table   = current_app.dynamodb.Table("bad-game-match_entries")
        history_table = current_app.dynamodb.Table("bad-users-history")
        user_table    = current_app.dynamodb.Table("bad-users")

        # entry_statusのみでフィルタ。メタ行除外。強整合読み。
        resp = match_table.scan(
            FilterExpression=Attr('entry_status').eq('resting') & ~Attr('entry_id').contains('meta'),
            ConsistentRead=True,
        )
        items = resp.get('Items', [])

        players = []
        for item in items:
            user_id = item.get('user_id')
            if not user_id:
                continue

            # ユーザー詳細
            uresp = user_table.get_item(Key={"user#user_id": user_id})
            user_data = uresp.get("Item", {}) or {}

            # 参加回数（履歴）
            try:
                hresp = history_table.scan(FilterExpression=Attr('user_id').eq(user_id))
                history_items = hresp.get('Items', []) or []
                join_count = sum(1 for h in history_items if h.get('date') and h['date'] < today)
            except Exception as e:
                current_app.logger.warning(f"[履歴取得エラー] user_id={user_id}: {e}")
                join_count = 0

            players.append({
                'entry_id': item.get('entry_id'),
                'user_id': user_id,
                'display_name': item.get('display_name', user_data.get('display_name', '不明')),
                'skill_score': item.get('skill_score', user_data.get('skill_score', 50)),
                'badminton_experience': user_data.get('badminton_experience', '未設定'),
                'joined_at': item.get('joined_at'),
                'rest_count': item.get('rest_count', 0),
                'match_count': item.get('match_count', 0),
                'join_count': join_count,
                'is_current_user': (user_id == current_user.get_id()),
            })

        # 並び順：休憩回数が多い→参加時刻（任意）
        players.sort(key=lambda x: (-(x.get('rest_count') or 0), x.get('joined_at') or ""))

        current_app.logger.info(f"[RESTING PLAYERS] 表示件数: {len(players)}")
        for p in players:
            current_app.logger.info(f"  - {p['display_name']}（{p['skill_score']}点）休憩回数: {p.get('rest_count',0)}")

        return players

    except Exception as e:
        current_app.logger.error(f"休憩中プレイヤー取得エラー: {e}")
        return []
    
def get_user_status(user_id):
    """ユーザーの現在の状態を取得"""
    try:
        # pending状態の確認
        pending_response = match_table.scan(
            FilterExpression=Attr('user_id').eq(user_id) & Attr('match_id').eq('pending')
        )
        is_registered = bool(pending_response.get('Items'))
        
        # resting状態の確認
        resting_response = match_table.scan(
            FilterExpression=Attr('user_id').eq(user_id) & Attr('match_id').eq('resting')
        )
        is_resting = bool(resting_response.get('Items'))
        
        # 戦闘力を取得
        skill_score = None
        
        # pending_itemsまたはresting_itemsから戦闘力を取得
        all_items = pending_response.get('Items', []) + resting_response.get('Items', [])
        if all_items:
            skill_score = all_items[0].get('skill_score')
        
        # 見つからない場合はuser_tableから取得
        if skill_score is None:
            user_response = user_table.get_item(Key={"user#user_id": user_id})
            user_data = user_response.get("Item", {})
            skill_score = user_data.get("skill_score", 50)
        
        return {
            'is_registered': is_registered,
            'is_resting': is_resting,
            'skill_score': skill_score  # ←追加
        }
        
    except Exception as e:
        current_app.logger.error(f"ユーザー状態取得エラー: {str(e)}")
        return {
            'is_registered': False,
            'is_resting': False,
            'skill_score': 50  # ←追加
        }
    
# @bp_game.route("/entry", methods=["POST"])
# @login_required
# def entry():
#     """明示的な参加登録（重複チェック＋新規登録）"""
#     user_id = current_user.get_id()
#     now = datetime.now().isoformat()
#     current_app.logger.info(f"[ENTRY] 参加登録開始: {user_id}")

#     # すでにpending登録されていないかチェック
#     response = match_table.scan(
#         FilterExpression=Attr("user_id").eq(user_id) & Attr("match_id").eq("pending")
#     )
#     existing = response.get("Items", [])

#     if existing:
#         current_app.logger.info("[ENTRY] すでに参加登録済みのためスキップ")
#         flash("すでに参加登録されています", "info")
#         return redirect(url_for("game.court"))

#     # 他の状態（restingなど）があれば削除
#     cleanup_response = match_table.scan(
#         FilterExpression=Attr("user_id").eq(user_id) & Attr("match_id").is_in(["resting", "active"])
#     )
#     for item in cleanup_response.get("Items", []):
#         match_table.delete_item(Key={"entry_id": item["entry_id"]})
#         current_app.logger.info(f"[ENTRY] 古いエントリ削除: {item['entry_id']}")

#     # ユーザー情報から戦闘力を取得
#     user_data = user_table.get_item(Key={"user#user_id": user_id}).get("Item", {})
#     skill_score = user_data.get("skill_score", 50)
#     display_name = user_data.get("display_name", "未設定")

#     # 新規登録
#     entry_item = {
#             "entry_id": str(uuid.uuid4()),
#             "user_id": user_id,
#             "match_id": "pending",          # NoneまたはDBの制約に合わせて""などを使用
#             "entry_status": "pending",  # 状態を示すフィールドはこちらを使用
#             # "status": "pending",        # statusフィールドも設定
#             "display_name": display_name,
#             "skill_score": skill_score,
#             "joined_at": now,
#             "created_at": now,
#             "rest_count": 0,      # 休憩回数を初期化
#             "match_count": 0,     # 試合回数を初期化
#         }
#     match_table.put_item(Item=entry_item)
#     current_app.logger.info(f"[ENTRY] 新規参加登録完了: {entry_item['entry_id']}")    

#     return redirect(url_for("game.court"))


@bp_game.route("/entry", methods=["POST"])
@login_required
def entry():
    """明示的な参加登録（重複チェック＋新規登録）"""
    user_id = current_user.get_id()
    now = datetime.now(JST).isoformat()  # ← JST追加
    current_app.logger.info(f"[ENTRY] 参加登録開始: {user_id}")

    # ← テーブル取得を追加
    match_table = current_app.dynamodb.Table("bad-game-match_entries")
    user_table = current_app.dynamodb.Table("bad-users")

    # すでにpending登録されていないかチェック
    response = match_table.scan(
        FilterExpression=Attr("user_id").eq(user_id) & Attr("entry_status").is_in(["pending", "resting", "playing"])  # ← 修正
    )
    existing = response.get("Items", [])

    if existing:
        current_app.logger.info("[ENTRY] すでに参加登録済みのためスキップ")
        # flash("すでに参加登録されています", "info")  ← フラッシュ削除
        return redirect(url_for("game.court"))

    # 他の状態のエントリがあれば削除（念のため）
    cleanup_response = match_table.scan(
        FilterExpression=Attr("user_id").eq(user_id)
    )
    for item in cleanup_response.get("Items", []):
        match_table.delete_item(Key={"entry_id": item["entry_id"]})
        current_app.logger.info(f"[ENTRY] 古いエントリ削除: {item['entry_id']}")

    # ユーザー情報から戦闘力を取得
    user_data = user_table.get_item(Key={"user#user_id": user_id}).get("Item", {})
    skill_score = user_data.get("skill_score", 50)
    display_name = user_data.get("display_name", "未設定")

    # 新規登録
    entry_item = {
        "entry_id": str(uuid.uuid4()),
        "user_id": user_id,
        "match_id": "pending",
        "entry_status": "pending",
        "display_name": display_name,
        "skill_score": Decimal(str(skill_score)),  # ← Decimalに変換
        "joined_at": now,
        "created_at": now,
        "rest_count": 0,
        "match_count": 0,
        "join_count": 1  # ← 追加
    }
    match_table.put_item(Item=entry_item)
    current_app.logger.info(f"[ENTRY] 新規参加登録完了: {entry_item['entry_id']}, スキルスコア: {skill_score}")

    return redirect(url_for("game.court"))


# さらに強力な重複クリーンアップ関数
def cleanup_duplicate_entries(user_id=None):
    """重複エントリのクリーンアップ（管理者用）"""
    try:
        if user_id:
            # 特定ユーザーの重複クリーンアップ
            users_to_check = [user_id]
        else:
            # 全ユーザーの重複チェック
            response = match_table.scan()
            all_entries = response.get('Items', [])
            users_to_check = list(set(entry['user_id'] for entry in all_entries))
        
        cleanup_count = 0
        for check_user_id in users_to_check:
            # pending重複チェック
            pending_response = match_table.scan(
                FilterExpression=Attr('user_id').eq(check_user_id) & Attr('match_id').eq('pending')
            )
            pending_entries = pending_response.get('Items', [])
            
            if len(pending_entries) > 1:
                # 最新以外を削除
                sorted_entries = sorted(pending_entries, key=lambda x: x.get('joined_at', ''), reverse=True)
                for old_entry in sorted_entries[1:]:
                    match_table.delete_item(Key={'entry_id': old_entry['entry_id']})
                    cleanup_count += 1
                    current_app.logger.info(f"重複クリーンアップ: {check_user_id} -> {old_entry['entry_id']}")
        
        current_app.logger.info(f"重複クリーンアップ完了: {cleanup_count}件削除")
        return cleanup_count
        
    except Exception as e:
        current_app.logger.error(f"重複クリーンアップエラー: {e}")
        return 0


# 管理者用エンドポイント
@bp_game.route("/admin/cleanup_duplicates", methods=['POST'])
@login_required
def admin_cleanup_duplicates():
    """管理者用：重複エントリクリーンアップ"""
    try:
        # 管理者権限チェック
        if not getattr(current_user, 'administrator', False):
            return jsonify({'success': False, 'message': '管理者権限が必要です'})
        
        cleanup_count = cleanup_duplicate_entries()
        
        return jsonify({
            'success': True,
            'message': f'重複エントリのクリーンアップが完了しました（{cleanup_count}件削除）'
        })
        
    except Exception as e:
        current_app.logger.error(f"管理者クリーンアップエラー: {e}")
        return jsonify({'success': False, 'message': 'クリーンアップに失敗しました'})
    
def increment_match_count(entry_id):
    table = current_app.dynamodb.Table("bad-game-match_entries")
    table.update_item(
        Key={"entry_id": entry_id},
        UpdateExpression="SET match_count = if_not_exists(match_count, :zero) + :inc",
        ExpressionAttributeValues={":inc": 1, ":zero": 0}
    )

def increment_rest_count(entry_id):
    table = current_app.dynamodb.Table("bad-game-match_entries")
    try:
        table.update_item(
            Key={"entry_id": entry_id},
            UpdateExpression="SET rest_count = if_not_exists(rest_count, :zero) + :inc",
            ExpressionAttributeValues={":inc": 1, ":zero": 0}
        )
        current_app.logger.info(f"🔁 [rest_count 加算] entry_id={entry_id}")
    except Exception as e:
        current_app.logger.error(f"rest_count 更新失敗: {e}")


def update_player_for_match(entry_id, match_id, court_number, team_side):
    """プレイヤーを試合用に更新（match_countもインクリメント）"""
    table = current_app.dynamodb.Table("bad-game-match_entries")
    try:
        # 更新前の確認
        current_app.logger.info(f"更新開始: entry_id={entry_id}, match_id={match_id}, court={court_number}, team={team_side}")
        
        # 更新前の状態を確認
        response = table.get_item(Key={"entry_id": entry_id})
        before_item = response.get("Item", {})
        current_app.logger.info(f"更新前: status={before_item.get('entry_status')}, match_id={before_item.get('match_id')}")
        
        table.update_item(
            Key={"entry_id": entry_id},
            UpdateExpression="SET match_id = :m, entry_status = :s, court_number = :c, team_side = :t, match_count = if_not_exists(match_count, :zero) + :inc",
            ExpressionAttributeValues={
                ":m": match_id,
                ":s": "playing",
                ":c": court_number,
                ":t": team_side,
                ":zero": 0,
                ":inc": 1
            }
        )
        
        # 更新後の確認
        response = table.get_item(Key={"entry_id": entry_id})
        after_item = response.get("Item", {})
        current_app.logger.info(f"更新後: status={after_item.get('entry_status')}, match_id={after_item.get('match_id')}, court={after_item.get('court_number')}, team={after_item.get('team_side')}")
        
        current_app.logger.info(f"プレイヤー更新: entry_id={entry_id}, コート{court_number}, チーム{team_side}")
        
    except Exception as e:
        current_app.logger.error(f"❌ プレイヤー更新エラー: {e}")
        import traceback
        current_app.logger.error(f"❌ スタックトレース: {traceback.format_exc()}")

def update_player_for_rest(entry_id):
    """プレイヤーを休憩用に更新（rest_countもインクリメント）"""
    table = current_app.dynamodb.Table("bad-game-match_entries")
    try:
        table.update_item(
            Key={"entry_id": entry_id},
            UpdateExpression="SET entry_status = :status, rest_count = if_not_exists(rest_count, :zero) + :inc",
            ExpressionAttributeValues={
                ":status": "resting",
                ":zero": 0,
                ":inc": 1
            }
        )
        current_app.logger.info(f"休憩プレイヤー更新: entry_id={entry_id}")
    except Exception as e:
        current_app.logger.error(f"休憩プレイヤー更新エラー: {e}")


# @bp_game.route('/create_pairings', methods=["POST"])
# @login_required
# def create_pairings():
#     # 進行中の試合チェック
#     if has_ongoing_matches():
#         flash('進行中の試合があるため、新しいペアリングを実行できません。全ての試合のスコア入力を完了してください。', 'warning')
#         return redirect(url_for('game.court'))
    
#     try:
#         max_courts = min(max(int(request.form.get("max_courts", 3)), 1), 6)        

#         # 1. pendingエントリー取得 & ユーザーごとに最新だけ残す
#         match_table = current_app.dynamodb.Table("bad-game-match_entries")
#         response = match_table.scan(FilterExpression=Attr("entry_status").eq("pending"))
#         entries_by_user = {}
#         for e in response.get("Items", []):
#             uid, joined_at = e["user_id"], e.get("joined_at", "")
#             if uid not in entries_by_user or joined_at > entries_by_user[uid].get("joined_at", ""):
#                 entries_by_user[uid] = e
#         entries = list(entries_by_user.values())

#         if len(entries) < 4:
#             flash("4人以上のエントリーが必要です。", "warning")
#             return redirect(url_for("game.court"))

#         # 再度チェック（二重送信防止）
#         if has_ongoing_matches():
#             flash('他のユーザーが同時にペアリングを実行したため、処理を中止しました。', 'warning')
#             return redirect(url_for('game.court'))

#         # スキルスコア最下位2名を選定
#         skill_sorted = sorted(
#             [(e["display_name"], e["entry_id"], Decimal(e.get("skill_score", 50))) for e in entries],
#             key=lambda x: x[2]
#         )
#         lowest_players = skill_sorted[:2]
#         current_app.logger.info(f"🧠 スキル最下位2名: {lowest_players}")

#         # 🎲 10%の確率で最下位2名を待機させる
#         forced_wait_ids = []
#         for name, entry_id, _ in lowest_players:
#             if random.random() < 0.10:
#                 forced_wait_ids.append(entry_id)
#         current_app.logger.info(f"⏸ 実際の待機者（{len(forced_wait_ids)}名）: {[(n, s) for n, i, s in skill_sorted if i in forced_wait_ids]}")

#         # 2. 休憩回数・試合回数に基づく優先順位付け
#         sorted_entries = sorted(entries, key=lambda e: (
#             -e.get("rest_count", 0),
#             e.get("match_count", 0),
#             random.random()
#         ))

#         # 3. 強制待機者を除外
#         sorted_entries = [e for e in sorted_entries if e["entry_id"] not in forced_wait_ids]

#         # 4. 必要なプレイヤー数を計算（4の倍数に調整）
#         required_players = min(max_courts * 4, len(sorted_entries) - (len(sorted_entries) % 4))
#         active_entries = sorted_entries[:required_players]
#         waiting_entries = sorted_entries[required_players:]
#         # 強制待機者を追加
#         waiting_entries.extend([e for e in entries if e["entry_id"] in forced_wait_ids])

#         # 5. シャッフル
#         random.shuffle(active_entries)

#         # 6. Player変換
#         name_to_id, players, waiting_players = {}, [], []
#         for e in active_entries:
#             name = e["display_name"]
#             p = Player(name, int(e.get("skill_score", 50)), e.get("gender", "M"))
#             p.match_count = e.get("match_count", 0)
#             p.rest_count = e.get("rest_count", 0)
#             name_to_id[name] = e["entry_id"]
#             players.append(p)
#         for e in waiting_entries:
#             name = e["display_name"]
#             p = Player(name, int(e.get("skill_score", 50)), e.get("gender", "M"))
#             p.match_count = e.get("match_count", 0)
#             p.rest_count = e.get("rest_count", 0)
#             name_to_id[name] = e["entry_id"]
#             waiting_players.append(p)

#         # 7. ペア生成 & マッチ生成
#         match_id = generate_match_id()
#         pairs, matches, additional_waiting_players = generate_balanced_pairs_and_matches(players, max_courts)
#         waiting_players.extend(additional_waiting_players)

#         # 8. 試合参加プレイヤー更新
#         for court_num, ((a1, a2), (b1, b2)) in enumerate(matches, 1):
#             for name, team in [(a1.name, "A"), (a2.name, "A"), (b1.name, "B"), (b2.name, "B")]:
#                 update_player_for_match(name_to_id[name], match_id, court_num, team)        
        
#         current_app.logger.info(f"ペアリング成功: {len(matches)}試合, {len(waiting_players)}人待機")

#         return redirect(url_for("game.court"))

#     except Exception as e:
#         current_app.logger.error(f"[ペア生成エラー] {str(e)}", exc_info=True)
#         flash("試合の作成中にエラーが発生しました。", "danger")
#         return redirect(url_for("game.court"))
    

def weighted_sample_no_replace(items, weights, k):
    chosen = []
    pool = list(zip(items, weights))
    while pool and len(chosen) < k:
        total = sum(w for _, w in pool)
        r = random.random() * total
        upto = 0.0
        for idx, (it, w) in enumerate(pool):
            upto += w
            if upto >= r:
                chosen.append(it)
                pool.pop(idx)  # 重複なし
                break
    return chosen


@bp_game.route('/create_pairings', methods=["POST"])
@login_required
def create_pairings():
    # ここでの has_ongoing_matches() は「画面メッセージ用」程度
    if has_ongoing_matches():
        flash('進行中の試合があるため、新しいペアリングを実行できません。全ての試合のスコア入力を完了してください。', 'warning')
        return redirect(url_for('game.court'))

    try:
        import boto3
        from botocore.exceptions import ClientError
        from boto3.dynamodb.conditions import Attr
        from datetime import datetime
        import random
        from decimal import Decimal

        max_courts = min(max(int(request.form.get("max_courts", 3)), 1), 6)

        # 1) pendingエントリー取得 & ユーザーごとに最新だけ残す
        entry_table = current_app.dynamodb.Table("bad-game-match_entries")
        response = entry_table.scan(FilterExpression=Attr("entry_status").eq("pending"))

        entries_by_user = {}
        for e in response.get("Items", []):
            uid, joined_at = e["user_id"], e.get("joined_at", "")
            if uid not in entries_by_user or joined_at > entries_by_user[uid].get("joined_at", ""):
                entries_by_user[uid] = e
        entries = list(entries_by_user.values())

        if len(entries) < 4:
            flash("4人以上のエントリーが必要です。", "warning")
            return redirect(url_for("game.court"))

        # 2) 優先順位（休憩多い→試合少ない→ランダム）
        sorted_entries = sorted(entries, key=lambda e: (
            -e.get("rest_count", 0),
            e.get("match_count", 0),
            random.random()
        ))

        # 3) required_players / waiting_count
        cap_by_courts = min(max_courts * 4, len(sorted_entries))
        required_players = cap_by_courts - (cap_by_courts % 4)
        waiting_count = len(sorted_entries) - required_players

        # 4) 待機枠バイアス（skill低い2名の待機確率を微増）
        if waiting_count > 0:
            skill_sorted = sorted(
                [(e["entry_id"], Decimal(e.get("skill_score", 50))) for e in sorted_entries],
                key=lambda x: x[1]
            )
            low2_ids = {eid for eid, _ in skill_sorted[:2]}

            LOW_BIAS = random.uniform(1.15, 1.3)
            weights = [(LOW_BIAS if e["entry_id"] in low2_ids else 1.0) for e in sorted_entries]

            chosen_waiting = weighted_sample_no_replace(sorted_entries, weights, waiting_count)
            waiting_ids = {e["entry_id"] for e in chosen_waiting}

            active_entries = [e for e in sorted_entries if e["entry_id"] not in waiting_ids]
            waiting_entries = [e for e in sorted_entries if e["entry_id"] in waiting_ids]

            current_app.logger.debug("[wait-bias] waiting_count=%s, low_bias=%s", waiting_count, LOW_BIAS)
        else:
            active_entries = sorted_entries
            waiting_entries = []

        random.shuffle(active_entries)

        # 5) Player変換
        name_to_id, players, waiting_players = {}, [], []

        for e in active_entries:
            name = e["display_name"]
            
            # skill_score と skill_sigma を取得
            skill_score = float(e.get("skill_score", 50.0))
            skill_sigma = float(e.get("skill_sigma", 8.333))
            
            # 保守的スキルを計算
            conservative = skill_score - 3 * skill_sigma
            
            # Player オブジェクト作成
            p = Player(name, conservative, e.get("gender", "M"))
            p.skill_score = skill_score
            p.skill_sigma = skill_sigma
            p.match_count = e.get("match_count", 0)
            p.rest_count = e.get("rest_count", 0)
            name_to_id[name] = e["entry_id"]
            players.append(p)

        # 👇 waiting_entries の処理
        for e in waiting_entries:
            name = e["display_name"]
            
            # skill_score と skill_sigma を取得
            skill_score = float(e.get("skill_score", 50.0))
            skill_sigma = float(e.get("skill_sigma", 8.333))
            
            # 保守的スキルを計算
            conservative = skill_score - 3 * skill_sigma
            
            # Player オブジェクト作成
            p = Player(name, conservative, e.get("gender", "M"))
            p.skill_score = skill_score
            p.skill_sigma = skill_sigma
            p.match_count = e.get("match_count", 0)
            p.rest_count = e.get("rest_count", 0)
            name_to_id[name] = e["entry_id"]
            waiting_players.append(p)

        # 6) ペア生成
        match_id = generate_match_id()
        pairs, matches, additional_waiting_players = generate_balanced_pairs_and_matches(players, max_courts)
        waiting_players.extend(additional_waiting_players)
        if not matches:
            flash("試合を作成できませんでした（人数不足など）。", "warning")
            return redirect(url_for("game.court"))

        # -------------------------------------------------
        # ✅ TransactWriteItems：metaロック + 試合参加者更新
        #   最大 25件制限：meta(1) + 4*len(matches)
        # -------------------------------------------------
        max_tx = 25
        need_tx = 1 + 4 * len(matches)
        if need_tx > max_tx:
            current_app.logger.error("[meta] tx items exceed limit: need=%s", need_tx)
            flash("試合数が多すぎて作成できませんでした。コート数を減らしてください。", "danger")
            return redirect(url_for("game.court"))

        now_jst = datetime.now(JST).isoformat()
        import boto3
        dynamodb_client = boto3.client('dynamodb', region_name='ap-northeast-1')

        tx_items = []
        meta_pk_str = "meta#current"

        # (1) meta#current を playing に（すでに playing なら弾く）
        tx_items.append({
            "Update": {
                "TableName": "bad-game-matches",
                "Key": {"match_id": {"S": "meta#current"}},
                "UpdateExpression": (
                    "SET #st = :playing, #cm = :mid, #cc = :cc, #ua = :now, #sa = :now"
                ),
                "ConditionExpression": "attribute_not_exists(#st) OR #st <> :playing",
                "ExpressionAttributeNames": {
                    "#st": "status",
                    "#cm": "current_match_id",
                    "#cc": "court_count",
                    "#ua": "updated_at",
                    "#sa": "started_at",
                },
                "ExpressionAttributeValues": {
                    ":playing": {"S": "playing"},
                    ":mid": {"S": str(match_id)},
                    ":cc": {"N": str(len(matches))},
                    ":now": {"S": now_jst},
                },
            }
        })
        # (2) pending の参加者を playing に（試合ID・コート・チームを付与）
        for court_num, ((a1, a2), (b1, b2)) in enumerate(matches, 1):
            for pl, team in [(a1, "A"), (a2, "A"), (b1, "B"), (b2, "B")]:
                entry_id = str(name_to_id[pl.name])

                tx_items.append({
                    "Update": {
                        "TableName": "bad-game-match_entries",
                        "Key": {"entry_id": {"S": entry_id}},
                        "UpdateExpression": (
                            "SET entry_status=:playing, match_id=:mid, court_number=:c, team=:t, updated_at=:now"
                        ),
                        "ConditionExpression": "entry_status = :pending",
                        "ExpressionAttributeValues": {
                            ":playing": {"S": "playing"},
                            ":pending": {"S": "pending"},
                            ":mid": {"S": str(match_id)},
                            ":c": {"N": str(court_num)},
                            ":t": {"S": team},
                            ":now": {"S": now_jst},
                        },
                    }
                })

        try:
            dynamodb_client.transact_write_items(TransactItems=tx_items)

            current_app.logger.info(
                "✅ [meta] lock+players committed: current_match_id=%s court_count=%s",
                match_id, len(matches)
            )

        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "TransactionCanceledException":
                current_app.logger.warning("[meta] lock tx canceled: %s", e)
                flash("進行中の試合があるためペアリングできませんでした。", "warning")
                return redirect(url_for("game.court"))
            raise

        current_app.logger.info("ペアリング成功: %s試合, %s人待機", len(matches), len(waiting_players))
        return redirect(url_for("game.court"))

    except Exception as e:
        current_app.logger.error("[ペア生成エラー] %s", str(e), exc_info=True)
        flash("試合の作成中にエラーが発生しました。", "danger")
        return redirect(url_for("game.court"))
    
def dump_client(tag, c):
    current_app.logger.warning("[%s] endpoint_url=%s", tag, c.meta.endpoint_url)
    current_app.logger.warning("[%s] region=%s", tag, c.meta.region_name)
    try:
        creds = c._request_signer._credentials
        current_app.logger.warning("[%s] access_key=%s", tag, getattr(creds, "access_key", None))
    except Exception as e:
        current_app.logger.warning("[%s] creds_dump_failed=%r", tag, e)


def update_players_to_playing(matches, match_id, match_table):
    """選ばれた人を 'playing' に更新する（このタイミングで match_id を新規に付与）"""
    current_app.logger.info(f" [START] update_players_to_playing - match_id: {match_id}")

    # 例: 2025-09-02T14:25:00+09:00
    now_iso = datetime.now(JST).isoformat()

    for match_idx, match in enumerate(matches):
        try:
            current_app.logger.info(f"処理中の match[{match_idx}]: {match}")

            if not isinstance(match, dict):
                current_app.logger.error(f"❌ match[{match_idx}] は dict ではありません: {type(match)}")
                continue

            courts_data = match.get("courts", match)
            if not isinstance(courts_data, dict):
                current_app.logger.error(f"❌ courts_data が dict ではありません: {type(courts_data)}")
                continue

            current_app.logger.info(f"使用する courts_data: {list(courts_data.keys())}")

            for court_key, court_data in courts_data.items():
                if not isinstance(court_data, dict):
                    current_app.logger.error(f"court_data[{court_key}] が dict ではありません: {type(court_data)}")
                    continue

                # court_number は数値に正規化
                try:
                    court_number = int(str(court_key).strip())
                except (ValueError, TypeError):
                    current_app.logger.error(f"❌ 無効な court_number: {court_key}")
                    continue

                for team_key in ["team_a", "team_b"]:
                    players = court_data.get(team_key, [])
                    if not isinstance(players, list):
                        current_app.logger.error(f"❌ players[{court_key}][{team_key}] が list ではありません: {type(players)}")
                        continue

                    # "team_a"/"team_b" -> "A"/"B" に正規化（保存は 'A' / 'B'）
                    team_letter = "A" if team_key == "team_a" else "B"

                    current_app.logger.info(f" court={court_number}, team={team_letter}, players={len(players)}人")

                    for player in players:
                        if not isinstance(player, dict) or "entry_id" not in player:
                            current_app.logger.error(f"無効なプレイヤーデータ: {player}")
                            continue

                        entry_id = player["entry_id"]
                        display_name = player.get("display_name", "Unknown")
                        user_id = player.get("user_id", "N/A")

                        current_app.logger.info(f"↪DynamoDB更新開始: {display_name} (entry_id={entry_id})")

                        # 🔒 冪等化: pending/resting の人だけ playing に昇格（playing 連打防止）
                        # ついでに 'court' フィールドは今後使わない前提で削除（古い互換を掃除）
                        result = match_table.update_item(
                            Key={"entry_id": entry_id},
                            UpdateExpression=(
                                "SET entry_status = :playing, "
                                "    match_id     = :mid, "
                                "    court_number = :court, "
                                "    team         = :team, "
                                "    team_side    = :team, "        # 互換のため重複持ち
                                "    updated_at   = :now, "
                                "    match_count  = if_not_exists(match_count, :zero) + :one "
                                "REMOVE court"                      # 旧 'court' を掃除（残すならこの行は外す）
                            ),
                            ConditionExpression=(
                                "attribute_exists(entry_id) AND "
                                "(attribute_not_exists(entry_status) OR entry_status IN (:pend, :rest))"
                            ),
                            ExpressionAttributeValues={
                                ":playing": "playing",
                                ":mid": {"S": str(match_id)},
                                ":court": court_number,
                                ":team": team_letter,
                                ":now": now_iso,
                                ":zero": 0,
                                ":one": 1,
                                ":pend": "pending",
                                ":rest": "resting",
                            },
                            ReturnValues="UPDATED_NEW",
                        )

                        updated_attrs = result.get("Attributes", {})
                        current_app.logger.info(
                            f"更新完了: {display_name} (user_id={user_id}, entry_id={entry_id}) "
                            f"→ court_number={court_number}, team={team_letter}, 更新後: {updated_attrs}"
                        )
        except Exception as e:
            current_app.logger.error(f"例外発生（match[{match_idx}]）: {e}")
            import traceback
            current_app.logger.error(traceback.format_exc())
            continue

    current_app.logger.info(f"[END] update_players_to_playing - match_id: {match_id}")


def simplify_player(player):
    """保存用に必要な情報だけ抽出（Decimalや不要情報を排除）"""
    return {
        "user_id": player.get("user_id"),
        "display_name": player.get("display_name")
    }


def perform_pairing(entries, match_id, max_courts=6):
    """
    プレイヤーのペアリングを行い、チームとコートを決定する
    
    Parameters:
    - entries: プレイヤーエントリーのリスト
    - match_id: 試合ID（YYYYMMDD_HHMMSS形式）
    - max_courts: 最大コート数
    
    Returns:
    - matches: コートとチームの情報
    - rest: 休憩するプレイヤーのリスト
    """
    matches = []
    rest = []
    court_number = 1
    
    match_table = current_app.dynamodb.Table("bad-game-match_entries")
    
    current_app.logger.info(f"ペアリング開始: 総エントリー数={len(entries)}, 最大コート数={max_courts}")
    current_app.logger.info(f"使用する試合ID: {match_id}")
    
    random.shuffle(entries)
    
    # 4人ずつのグループを作成
    for i in range(0, len(entries), 4):
        if court_number > max_courts:
            remaining_players = entries[i:]
            current_app.logger.info(f"コート数超過 - 残り{len(remaining_players)}人は休憩")
            rest.extend(remaining_players)
            break
        
        group = entries[i:i + 4]
        current_app.logger.info(f"グループ{court_number}: {len(group)}人")
        
        if len(group) == 4:
            teamA = group[:2]
            teamB = group[2:]
            
            current_app.logger.info(f"コート{court_number}で試合作成")
            
            # プレイヤーのエントリーステータスを更新
            for p in teamA:
                try:
                    match_table.update_item(
                        Key={'entry_id': p['entry_id']},
                        UpdateExpression="SET #status = :playing, entry_status = :playing, match_id = :mid, court_number = :court, team_side = :team",
                        ExpressionAttributeNames={"#status": "entry_status"},
                        ExpressionAttributeValues={
                            ":playing": "playing",
                            ":mid": {"S": str(match_id)},
                            ":court": court_number,
                            ":team": "A"
                        }
                    )
                except Exception as e:
                    current_app.logger.error(f"⚠️ プレイヤー更新エラー (チームA): {p.get('display_name')} - {str(e)}")
            
            for p in teamB:
                try:
                    match_table.update_item(
                        Key={'entry_id': p['entry_id']},
                        UpdateExpression="SET #status = :playing, entry_status = :playing, match_id = :mid, court_number = :court, team_side = :team",
                        ExpressionAttributeNames={"#status": "entry_status"},
                        ExpressionAttributeValues={
                            ":playing": "playing",
                            ":mid": {"S": str(match_id)},
                            ":court": court_number,
                            ":team": "B"
                        }
                    )
                except Exception as e:
                    current_app.logger.error(f"⚠️ プレイヤー更新エラー (チームB): {p.get('display_name')} - {str(e)}")
            
            # プレイヤー情報を簡素化して保存用辞書に変換
            simplified_teamA = [simplify_player(p) for p in teamA]
            simplified_teamB = [simplify_player(p) for p in teamB]
            
            match_data = {
                f"court_{court_number}": {
                    "court_number": court_number,
                    "team_a": simplified_teamA,
                    "team_b": simplified_teamB
                }
            }
            
            matches.append(match_data)
            court_number += 1
        else:
            current_app.logger.info(f"グループ{court_number}は{len(group)}人なので休憩")
            rest.extend(group)
    
    current_app.logger.info(f"ペアリング結果: {len(matches)}コート使用, {len(rest)}人休憩")
    
    for p in rest:
        try:
            match_table.update_item(
                Key={'entry_id': p['entry_id']},
                UpdateExpression="SET entry_status = :resting",
                ExpressionAttributeValues={
                    ":resting": "resting"
                }
            )
        except Exception as e:
            current_app.logger.error(f"⚠️ 休憩者更新エラー: {p.get('display_name')} - {str(e)}")

    
@bp_game.route("/finish_current_match", methods=["POST"])
@login_required
def finish_current_match():
    try:
        # =========================================================
        # 0) meta#current から進行中 match_id を取得
        # =========================================================
        meta_pk = "meta#current"
        meta_table = current_app.dynamodb.Table("bad-game-matches")

        meta_resp = meta_table.get_item(Key={"match_id": meta_pk}, ConsistentRead=True)
        meta_item = meta_resp.get("Item") or {}

        status = meta_item.get("status")
        match_id = meta_item.get("current_match_id")

        if status != "playing" or not match_id:
            current_app.logger.warning(
                "⚠️ アクティブな試合が見つかりません(meta). status=%s, current_match_id=%s",
                status, match_id
            )
            return "アクティブな試合が見つかりません", 400

        current_app.logger.info("🏁 試合終了処理開始(meta): match_id=%s", match_id)

        # (任意) ID形式チェック
        if not re.compile(r"^\d{8}_\d{6}$").match(match_id):
            current_app.logger.warning("⚠️ 非標準形式の試合ID: %s", match_id)

        now_jst = datetime.now(JST).isoformat()
        
        # 直接 boto3 client を作成
        import boto3
        dynamodb_client = boto3.client('dynamodb', region_name='ap-northeast-1')

        # =========================================================
        # 1) playing プレイヤー一覧（後で transaction に使う）
        # =========================================================
        match_table = current_app.dynamodb.Table("bad-game-match_entries")

        def scan_all_playing():
            items = []
            kwargs = {
                "FilterExpression": (
                    Attr("match_id").eq(match_id) &
                    Attr("entry_status").eq("playing") &
                    ~Attr("entry_id").contains("meta")
                ),
            }
            while True:
                resp = match_table.scan(**kwargs)
                items.extend(resp.get("Items", []))
                lek = resp.get("LastEvaluatedKey")
                if not lek:
                    break
                kwargs["ExclusiveStartKey"] = lek
            return items

        playing_players = scan_all_playing()

        if len(playing_players) > 24:
            current_app.logger.error("playing_players too many: %d", len(playing_players))
            return "playingプレイヤー数が多すぎます", 500

        player_mapping = {
            p["user_id"]: p["entry_id"]
            for p in playing_players
            if "user_id" in p and "entry_id" in p
        }

        # =========================================================
        # 2) TrueSkill 更新（これは transaction に入れない）
        # =========================================================
        results_table = current_app.dynamodb.Table("bad-game-results")
        
        def scan_all_results():
            items = []
            kwargs = {"FilterExpression": Attr("match_id").eq(match_id)}
            while True:
                resp = results_table.scan(**kwargs)
                items.extend(resp.get("Items", []))
                lek = resp.get("LastEvaluatedKey")
                if not lek:
                    break
                kwargs["ExclusiveStartKey"] = lek
            return items

        match_results = scan_all_results()
        current_app.logger.info("🎮 試合結果数: %d", len(match_results))

        updated_skills = {}
        skill_update_count = 0

        for result in match_results:
            try:
                team_a = parse_players(result["team_a"])
                team_b = parse_players(result["team_b"])
                winner = result.get("winner", "A")

                # entry_id 補完（同期用）
                for pl in team_a + team_b:
                    uid = pl.get("user_id")
                    if uid in player_mapping:
                        pl["entry_id"] = player_mapping[uid]

                result_item = {
                    "team_a": team_a,
                    "team_b": team_b,
                    "winner": winner,
                    "match_id": match_id
                }

                current_app.logger.info("🎯 コート%s: %sチーム勝利", result.get("court_number"), winner)

                updated_user_skills = update_trueskill_for_players_and_return_updates(result_item)
                updated_skills.update(updated_user_skills)

                skill_update_count += 1

            except Exception as e:
                current_app.logger.error("スキル更新エラー (court=%s): %s", result.get("court_number"), e)

        current_app.logger.info("✅ スキル更新完了: %d/%dコート", skill_update_count, len(match_results))

        # =========================================================
        # 3) エントリーテーブル同期（スキル値の反映）
        # =========================================================
        sync_count = sync_match_entries_with_updated_skills(player_mapping, updated_skills)
        current_app.logger.info("✅ エントリーテーブル同期完了: %d件", sync_count)

        # =========================================================
        # 4) ✅ meta解除 + playing→pending を 1トランザクションで確定
        #    meta(1) + 最大24人 = 25件（上限内）
        # =========================================================       

        tx_items = []

        # (a) meta を idle に戻す（status=playing かつ current_match_id 一致）
        tx_items.append({
            "Update": {
                "TableName": "bad-game-matches",
                "Key": {"match_id": {"S": "meta#current"}},
                "UpdateExpression": (
                    "SET #st = :idle, #ua = :now, #fa = :now, #lm = :mid "
                    "REMOVE #cm, #cc"
                ),
                "ConditionExpression": "#st = :playing AND #cm = :mid",
                "ExpressionAttributeNames": {
                    "#st": "status",
                    "#ua": "updated_at",
                    "#fa": "finished_at",
                    "#lm": "last_match_id",
                    "#cm": "current_match_id",
                    "#cc": "court_count",
                },
                "ExpressionAttributeValues": {
                    ":idle": {"S": "idle"},
                    ":playing": {"S": "playing"},
                    ":mid": {"S": str(match_id)},
                    ":now": {"S": now_jst},
                },
            }
        })

        # (b) 全 playing を pending に戻す
        for p in playing_players:
            entry_id = p.get("entry_id")
            if not entry_id:
                current_app.logger.warning("[finish] missing entry_id in playing player: %s", p)
                continue

            tx_items.append({
                "Update": {
                    "TableName": "bad-game-match_entries",
                    "Key": {"entry_id": {"S": str(entry_id)}},
                    "UpdateExpression": (
                        "SET entry_status=:pending, updated_at=:now "
                        "REMOVE court_number, team, team_side"
                    ),
                    "ConditionExpression": "entry_status = :playing AND match_id = :mid",
                    "ExpressionAttributeValues": {
                        ":pending": {"S": "pending"},
                        ":playing": {"S": "playing"},
                        ":mid": {"S": str(match_id)},
                        ":now": {"S": now_jst},
                    },
                }
            })

        # トランザクション実行
        try:
            dynamodb_client.transact_write_items(TransactItems=tx_items)
            current_app.logger.info("✅ [meta] unlocked + players pending committed: match_id=%s", match_id)

        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code == "TransactionCanceledException":
                current_app.logger.error("⚠️ Transaction canceled for match_id=%s: %s", 
                                       match_id, e.response.get("Error", {}).get("Message"))
                return jsonify({"success": False, "error": "finish transaction canceled"}), 409
            raise

        # =========================================================
        # Ajax / 通常レスポンス
        # =========================================================
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({
                "success": True,
                "message": "試合が正常に終了しました",
                "updated_players": len(playing_players),
                "skill_updates": skill_update_count,
                "synced_entries": sync_count
            })

        return redirect(url_for("game.court"))

    except Exception as e:
        current_app.logger.error("[試合終了処理エラー] %s", str(e), exc_info=True)

        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"success": False, "error": str(e)}), 500

        flash(f"エラーが発生しました: {str(e)}", "danger")
        return redirect(url_for("game.court"))
    

@bp_game.route("/start_next_match", methods=["POST"])
@login_required
def start_next_match():
    try:
        latest_match_id = get_latest_match_id()
        current_app.logger.info(f"最新の試合ID: {latest_match_id}")
        
        # 現在試合中のプレイヤーを取得
        current_players_by_court = get_match_players_by_court(latest_match_id)
        current_players = []
        for court_data in current_players_by_court.values():
            current_players.extend(court_data["team_a"])
            current_players.extend(court_data["team_b"])
        
        # 参加待ちプレイヤーも取得
        pending_players = get_players_status('pending')
        
        # 全てのプレイヤーを結合
        all_players = current_players + pending_players
        
        if not all_players:
            return "参加者が見つかりません", 400

        # 新しい試合IDを生成（YYYYMMDD_HHMMSS形式）
        new_match_id = generate_match_id()
        current_app.logger.info(f"🆕 新しい試合ID: {new_match_id}")
        
        match_table = current_app.dynamodb.Table("bad-game-match_entries")

        # 重複除去: user_id ごとに最新のエントリーだけを残す
        unique_players = {}
        for p in all_players:
            uid = p["user_id"]
            if uid not in unique_players:
                unique_players[uid] = p
            else:
                # より新しい joined_at を持つ方を残す
                if p.get("joined_at", "") > unique_players[uid].get("joined_at", ""):
                    unique_players[uid] = p

        # 重複除去後の新エントリー
        new_entries = []
        for p in unique_players.values():
            new_entries.append({
                'entry_id': str(uuid.uuid4()),
                'user_id': p['user_id'],
                'match_id': "pending",  # 初期状態は"pending"
                'entry_status': 'pending',
                'display_name': p['display_name'],
                'badminton_experience': p.get('badminton_experience', ''),
                'skill_score': p.get('skill_score', 50),  # デフォルト値を設定
                'joined_at': datetime.now().isoformat()
            })

        current_app.logger.info(f"次の試合エントリー数: {len(new_entries)}")
        for entry in new_entries:
            current_app.logger.info(f"  - {entry['display_name']}")

        # DynamoDBに新規エントリーを登録
        for entry in new_entries:
            match_table.put_item(Item=entry)

        # ペアリング処理を実行 - 統一形式のIDを渡す
        matches, rest = perform_pairing(new_entries, new_match_id)
        
        # 結果のサマリーをログに出力
        current_app.logger.info(f"ペアリング完了: {len(matches)}コート、{len(new_entries)-len(rest)}人参加、{len(rest)}人休憩")
        
        # フラッシュメッセージで通知（オプション）
        flash(f"新しい試合が開始されました (ID: {new_match_id}, コート数: {len(matches)})", "success")

        return redirect(url_for("game.court"))
        
    except Exception as e:
        current_app.logger.error(f"試合開始エラー: {str(e)}")
        import traceback
        current_app.logger.error(f"スタックトレース: {traceback.format_exc()}")
        flash(f"エラーが発生しました: {str(e)}", "danger")
        return redirect(url_for("game.court"))

@bp_game.route("/pairings", methods=["GET"])
@login_required
def show_pairings():
    try:
        match_id = get_latest_match_id()  # 最新のmatch_id取得（例: '20250701_027'）

        match_table = current_app.dynamodb.Table("bad-game-match_entries")
        response = match_table.scan(
            FilterExpression=Attr("match_id").eq(match_id) & Attr("type").ne("meta")
        )
        items = response.get("Items", [])

        # コートごとにまとめる
        court_dict = {}
        for item in items:
            court_no = item.get("court_number")
            team = item.get("team")  # 'A' or 'B'
            name = item.get("display_name")

            if court_no not in court_dict:
                court_dict[court_no] = {"team_a": [], "team_b": []}
            if team == "A":
                court_dict[court_no]["team_a"].append(name)
            elif team == "B":
                court_dict[court_no]["team_b"].append(name)

        # court_dict を match_data のリスト形式に変換
        match_data = []
        for court_no in sorted(court_dict):
            match_data.append({
                "court_number": court_no,
                "team_a": court_dict[court_no]["team_a"],
                "team_b": court_dict[court_no]["team_b"]
            })

        return render_template("game/court.html", match_data=match_data)

    except Exception as e:
        current_app.logger.error(f"[pairings] エラー: {str(e)}")
        return redirect(url_for("main.index"))

def generate_match_id():
    """試合IDを生成（時分秒を使用してユニーク性を保証）"""
    now = datetime.now()
    match_id = now.strftime("%Y%m%d_%H%M%S")  # "20250706_094309"
    
    current_app.logger.info(f"生成された試合ID: {match_id}")
    return match_id


@bp_game.route('/rest', methods=['POST'])
@login_required
def rest():
    """休憩モードに切り替え（POSTのみ）"""
    try:
        current_entry = get_user_current_entry(current_user.get_id())
        if current_entry:
            match_table.update_item(
                Key={'entry_id': current_entry['entry_id']},
                UpdateExpression='SET entry_status = :status, rest_started_at = :time',
                ExpressionAttributeValues={
                    ':status': 'resting',
                    ':time': datetime.now().isoformat()
                }
            )
    except Exception as e:
        current_app.logger.error(f'休憩エラー: {e}')

    return redirect(url_for('game.court'))


@bp_game.route('/api/toggle_player_status', methods=['POST'])
@login_required
def toggle_player_status():
    # 管理者権限チェック
    if not current_user.administrator:
        current_app.logger.warning(f'非管理者からのアクセス: {current_user.get_id()}')
        return jsonify({'success': False, 'message': '管理者権限が必要です'}), 403
    
    try:
        data = request.get_json()
        current_app.logger.info(f'受信データ: {data}')
        
        player_id = data.get('player_id')
        current_status = data.get('current_status')
        
        current_app.logger.info(f'プレイヤーID: {player_id}, 現在のステータス: {current_status}')
        
        if not player_id or not current_status:
            current_app.logger.error('パラメータが不足しています')
            return jsonify({'success': False, 'message': 'パラメータが不足しています'}), 400
        
        # DynamoDBからプレイヤーのエントリーを取得
        current_entry = get_user_current_entry(player_id)
        current_app.logger.info(f'取得したエントリー: {current_entry}')
        
        if not current_entry:
            current_app.logger.error(f'プレイヤー {player_id} のエントリーが見つかりません')
            return jsonify({'success': False, 'message': 'プレイヤーのエントリーが見つかりません'}), 404
        
        # プレイヤー名を取得
        player_name = current_entry.get('display_name', 'プレイヤー')
        current_app.logger.info(f'プレイヤー名: {player_name}')
        
        # 現在のステータスを確認
        actual_status = current_entry.get('entry_status')
        current_app.logger.info(f'実際のステータス: {actual_status}, 期待するステータス: {current_status}')
        
        # 現在の状態に応じて切り替え
        if current_status == 'pending' and actual_status == 'pending':
            # 参加待ち → 休憩中
            current_app.logger.info(f'{player_name}を休憩状態に変更中...')
            match_table.update_item(
                Key={'entry_id': current_entry['entry_id']},
                UpdateExpression='SET entry_status = :status, rest_started_at = :time',
                ExpressionAttributeValues={
                    ':status': 'resting',
                    ':time': datetime.now().isoformat()
                }
            )
            current_app.logger.info(f'{player_name}を休憩状態に変更完了')
            
            return jsonify({
                'success': True, 
                'message': f'{player_name}さんを休憩状態に変更しました',
                'new_status': 'resting'
            })
        
        elif current_status == 'resting' and actual_status == 'resting':
            # 休憩中 → 参加待ち
            current_app.logger.info(f'{player_name}を参加待ち状態に変更中...')
            match_table.update_item(
                Key={'entry_id': current_entry['entry_id']},
                UpdateExpression='SET entry_status = :status',
                ExpressionAttributeValues={
                    ':status': 'pending'
                }
            )
            current_app.logger.info(f'{player_name}を参加待ち状態に変更完了')
            
            return jsonify({
                'success': True, 
                'message': f'{player_name}さんを参加待ち状態に変更しました',
                'new_status': 'pending'
            })
        
        current_app.logger.error(f'状態の不一致: 期待={current_status}, 実際={actual_status}')
        return jsonify({'success': False, 'message': f'状態の変更に失敗しました。現在の状態: {actual_status}'}), 400
        
    except Exception as e:
        current_app.logger.error(f'状態変更エラー: {e}', exc_info=True)
        return jsonify({'success': False, 'message': f'エラーが発生しました: {str(e)}'}), 500
    

@bp_game.route('/resume', methods=['POST'])
@login_required
def resume():
    """復帰（アクティブに戻す）"""
    try:
        current_entry = get_user_current_entry(current_user.get_id())
        if current_entry:
            match_table.update_item(
                Key={'entry_id': current_entry['entry_id']},
                UpdateExpression='SET entry_status = :status, match_id = :match_id, resumed_at = :time',
                ExpressionAttributeValues={
                    ':status': 'pending',
                    ':match_id': 'pending',
                    ':time': datetime.now(JST).isoformat()  # ← JST追加
                }
            )

    except Exception as e:
        current_app.logger.error(f'復帰エラー: {e}')
    
    return redirect(url_for('game.court'))


@bp_game.route('/leave_court', methods=['POST'])
@login_required
def leave_court():
    """コートから出る（エントリー削除）"""
    try:
        current_entry = get_user_current_entry(current_user.get_id())
        if current_entry:
            # 試合中でないことを確認
            if current_entry.get('match_id') != 'pending':
                flash('試合中のため退出できません', 'warning')
                return redirect(url_for('game.court'))
            
            # エントリーを削除
            match_table.delete_item(Key={'entry_id': current_entry['entry_id']})
            flash('コートから退出しました', 'info')
            return redirect(url_for('index'))
        
    except Exception as e:
        current_app.logger.error(f'退出エラー: {e}')
        flash('退出に失敗しました', 'danger')
    
    return redirect(url_for('game.court'))

def get_user_current_entry(user_id):
    """ユーザーの現在のエントリー（参加中 or 休憩中）を取得"""
    try:
        response = match_table.scan(
            FilterExpression=Attr('user_id').eq(user_id) & Attr('entry_status').is_in(['pending', 'resting'])
        )
        items = response.get('Items', [])
        if items:
            return max(items, key=lambda x: x.get('joined_at', ''))
        return None
    except Exception as e:
        current_app.logger.error(f'ユーザーエントリ取得エラー: {e}')
        return None

@bp_game.route("/api/waiting_status")
@login_required
def waiting_status():
    pending_players = get_players_status("pending")
    resting_players = get_players_status("resting")

    latest_match_id = get_latest_match_id()
    current_app.logger.info(f"最新の試合ID: {latest_match_id}")

    in_progress = False

    if latest_match_id:
        match_table = current_app.dynamodb.Table("bad-game-match_entries")
        try:
            # latest_match_id の playing が1件でもあれば試合中
            resp = match_table.scan(
                FilterExpression=Attr("match_id").eq(latest_match_id) & Attr("entry_status").eq("playing"),
                ProjectionExpression="entry_id",
            )
            in_progress = len(resp.get("Items", [])) > 0

        except Exception as e:
            current_app.logger.error(f"試合中判定の取得に失敗: {e}")

    return jsonify({
        "pending_count": len(pending_players),
        "resting_count": len(resting_players),
        # フロントがこのキー名を使ってるなら残す（意味は「試合中/開始済み」に変更）
        "new_pairing_available": in_progress,
        # 追加で出しておくとデバッグしやすい
        "latest_match_id": latest_match_id,
        "in_progress": in_progress,
    })
   
    
@bp_game.route("/set_score_format", methods=["POST"])
@login_required
def set_score_format():
    selected_format = request.form.get("score_format")
    if selected_format in {"15", "21"}:
        session["score_format"] = selected_format
    return redirect(url_for("game.court"))

# @bp_game.route('/api/match_score_status/<match_id>')
# @login_required
# def match_score_status(match_id):    
#     game_meta_table = current_app.dynamodb.Table("bad-game-matches")
#     meta_entry_id = f"meta#{match_id}"

#     try:        
#         response = game_meta_table.get_item(Key={'match_id': match_id})
        
#         match_item = response.get('Item', {})

#         # コート数は事前にどこかに保存されているか、固定値でも可
#         court_count = 3  # 例
#         all_submitted = all(
#             match_item.get(f"court_{i}_score") for i in range(1, court_count + 1)
#         )

#         return jsonify({"all_submitted": all_submitted})
#     except Exception as e:
#         current_app.logger.error(f"[スコア確認エラー] {e}")
#         return jsonify({"error": "確認に失敗しました"}), 500
    

@bp_game.route('/api/match_score_status/<match_id>')
@login_required
def match_score_status(match_id):
    results_table = current_app.dynamodb.Table("bad-game-results")

    try:
        court_count = 3  # 固定でOK（将来は match_meta から取得でも可）

        items = []
        resp = results_table.scan(
            FilterExpression=Attr("match_id").eq(match_id)
        )
        items.extend(resp.get("Items", []))

        while "LastEvaluatedKey" in resp:
            resp = results_table.scan(
                FilterExpression=Attr("match_id").eq(match_id),
                ExclusiveStartKey=resp["LastEvaluatedKey"]
            )
            items.extend(resp.get("Items", []))

        # court_number -> item（同一コートが複数あったら、created_at が新しい方を採用）
        by_court = {}
        for it in items:
            cn = it.get("court_number")
            if cn is None:
                continue
            try:
                cn = int(cn)
            except Exception:
                continue

            prev = by_court.get(cn)
            if prev is None:
                by_court[cn] = it
            else:
                # created_at がある場合は新しい方を優先（ないなら上書き）
                if (it.get("created_at") or "") >= (prev.get("created_at") or ""):
                    by_court[cn] = it

        # 1..court_count の全コートにスコアがあるか
        for i in range(1, court_count + 1):
            it = by_court.get(i)
            if not it:
                return jsonify({"all_submitted": False})
            if it.get("team1_score") is None or it.get("team2_score") is None:
                return jsonify({"all_submitted": False})

        return jsonify({"all_submitted": True})

    except Exception as e:
        current_app.logger.error(f"[スコア確認エラー] {e}", exc_info=True)
        return jsonify({"error": "確認に失敗しました"}), 500
    

# @bp_game.route("/score_input", methods=["GET", "POST"])
# @login_required
# def score_input():
#     match_id = get_latest_match_id()
#     match_table = current_app.dynamodb.Table("bad-game-match_entries")
#     response = match_table.scan(
#         FilterExpression=Attr("match_id").eq(match_id)
#     )
#     items = response.get("Items", [])

#     # コート別に整理
#     court_data = {}
#     for item in items:
#         court = item.get("court")
#         team = item.get("team")
#         if court and team:
#             court_entry = court_data.setdefault(court, {"team_a": [], "team_b": []})
#             court_entry[team].append(item)

#     return render_template("game/score_input.html", court_data=court_data, match_id=match_id)

@bp_game.route("/score_input", methods=["GET", "POST"])
@login_required
def score_input():
    match_id = get_latest_match_id()
    current_app.logger.info(f"[score_input] match_id = {match_id}")
    
    # 共通関数を使用
    match_courts = get_organized_match_data(match_id)
    
    return render_template("game/score_input.html", match_courts=match_courts, match_id=match_id) 


@bp_game.route("/submit_score/<match_id>/court/<int:court_number>", methods=["POST"])
@login_required
def submit_score(match_id, court_number):
    try:
        # リクエストデータをログに記録
        current_app.logger.info(f"スコア送信開始: match_id={match_id}, court={court_number}")
        current_app.logger.info(f"リクエストデータ: {dict(request.form)}")
        
        # 入力値の検証
        team1_score = int(request.form.get("team1_score"))
        team2_score = int(request.form.get("team2_score"))

        if team1_score == team2_score:
            return "スコアが同点です。勝者を決めてください。", 400

        winner = "A" if team1_score > team2_score else "B"
        
        # 試合IDの形式を検証
        import re
        match_id_pattern = re.compile(r'^\d{8}_\d{6}$')
        if not match_id_pattern.match(match_id):
            current_app.logger.warning(f"非標準形式の試合ID: {match_id}")
        
        # 試合エントリーからチームデータを取得
        match_table = current_app.dynamodb.Table("bad-game-match_entries")
        
        # まず'court'フィールドで試す
        response = match_table.scan(
            FilterExpression=Attr("match_id").eq(match_id) & Attr("court").eq(str(court_number))
        )
        entries = response.get("Items", [])
        
        # エントリーがない場合、'court_number'でも試す
        if not entries:
            try:
                alt_response = match_table.scan(
                    FilterExpression=Attr("match_id").eq(match_id) & Attr("court_number").eq(court_number)
                )
                entries = alt_response.get("Items", [])
                current_app.logger.info(f"代替フィールド名'court_number'を使用: {len(entries)}件取得")
            except Exception as e:
                current_app.logger.warning(f"代替クエリ失敗: {str(e)}")
        
        current_app.logger.info(f"取得したエントリー数: {len(entries)}")
        
        # エントリーIDとユーザーIDのマッピングを作成
        entry_mapping = {}
        for entry in entries:
            user_id = entry.get("user_id")
            entry_id = entry.get("entry_id")
            if user_id and entry_id:
                entry_mapping[user_id] = entry_id
        
        # チームごとに分類
        team_a = []
        team_b = []
        
        for entry in entries:
            player_data = {
                "user_id": str(entry.get("user_id", "")),
                "display_name": str(entry.get("display_name", "不明")),
                "entry_id": str(entry.get("entry_id", ""))  # entry_idも含める
            }
            
            # team と team_side の両方を確認
            team_value = entry.get("team", entry.get("team_side"))
            
            if team_value == "A":
                team_a.append(player_data)
            elif team_value == "B":
                team_b.append(player_data)
        
        current_app.logger.info(f"チームA: {team_a}")
        current_app.logger.info(f"チームB: {team_b}")
        
        # エントリーがない場合はエラー
        if not team_a or not team_b:
            current_app.logger.error(f"コート{court_number}のチームデータが不完全です")
            return "コートのチームデータが不完全です", 404

        # 結果テーブル
        result_table = current_app.dynamodb.Table("bad-game-results")

        # タイムスタンプを生成（タイムゾーン付き）
        timestamp = datetime.now(JST).isoformat()
        
        # 結果アイテムを作成
        result_item = {
            "result_id": str(uuid.uuid4()),
            "match_id": str(match_id),
            "court_number": int(court_number),
            "team1_score": int(team1_score),
            "team2_score": int(team2_score),
            "winner": str(winner),
            "team_a": team_a,
            "team_b": team_b,
            "created_at": str(timestamp)
        }
        
        # 保存する内容をログに出力
        current_app.logger.info(f"保存する結果アイテム: {result_item}")

        # 試合結果保存
        try:
            response = result_table.put_item(Item=result_item)
            current_app.logger.info(f"スコア送信成功: {match_id}, コート {court_number}, スコア {team1_score}-{team2_score}")
            current_app.logger.info(f"DynamoDB応答: {response}")
        except Exception as e:
            current_app.logger.error(f"❌ 結果保存エラー: {str(e)}")
            return "スコアの保存に失敗しました", 500        
        return "", 200

    except Exception as e:
        current_app.logger.error(f"[submit_score ERROR] {str(e)}")
        import traceback
        current_app.logger.error(traceback.format_exc())
        return "スコアの送信中にエラーが発生しました", 500

def clean_team(team):
    from flask import current_app
    current_app.logger.info(f"🧼 clean_team() 入力: {team}")

    cleaned = []
    for p in team:
        if isinstance(p, dict):
            cleaned.append({
                "user_id": p.get("user_id"),
                "display_name": p.get("display_name"),
                "skill_score": int(p.get("skill_score", 50))
            })
        elif isinstance(p, str):
            # 文字列（user_id）の場合、仮の名前とデフォルトスコアを付ける
            cleaned.append({
                "user_id": p,
                "display_name": p,
                "skill_score": 50
            })
    current_app.logger.info(f"🧼 clean_team() 出力: {cleaned}")
    return cleaned
    

@bp_game.route('/reset_participants', methods=['POST'])
@login_required
def reset_participants():
    """全てのエントリーを削除（練習終了 or エラーリセット）"""
    if not current_user.administrator:
        flash('管理者のみ実行できます', 'danger')
        return redirect(url_for('index'))

    try:
        # 1. match_entries テーブルの全削除
        match_table = current_app.dynamodb.Table("bad-game-match_entries")
        deleted_count = 0
        last_evaluated_key = None

        current_app.logger.info("🔄 全エントリー削除開始")
        
        while True:
            if last_evaluated_key:
                response = match_table.scan(ExclusiveStartKey=last_evaluated_key)
            else:
                response = match_table.scan()

            items = response.get('Items', [])
            for item in items:
                try:
                    match_table.delete_item(Key={'entry_id': item['entry_id']})
                    deleted_count += 1
                    current_app.logger.info(f"🗑️ 削除: {item.get('display_name', 'Unknown')} - {item['entry_id']}")
                except Exception as e:
                    current_app.logger.error(f"❌ エントリー削除エラー: {item.get('display_name', 'Unknown')} - {str(e)}")

            last_evaluated_key = response.get('LastEvaluatedKey')
            if not last_evaluated_key:
                break

        # 2. 削除完了後の確認
        time.sleep(0.5)  # DynamoDB の一貫性を待つ
        
        # 確認スキャン
        check_response = match_table.scan()
        remaining_items = check_response.get('Items', [])
        
        if remaining_items:
            current_app.logger.warning(f"⚠️ 削除後も残っているエントリー: {len(remaining_items)}件")
            for item in remaining_items:
                current_app.logger.warning(f"⚠️ 残存: {item.get('display_name', 'Unknown')} - {item['entry_id']}")
        else:
            current_app.logger.info("全エントリー削除完了")

        # 3. (オプション) results テーブルのメンテナンス
        # ここでresultsテーブルに対する処理を行う場合は追加
        
        current_app.logger.info(f"[全削除成功] エントリー削除件数: {deleted_count} by {current_user.email}")

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        current_app.logger.error(f"[全削除失敗] {str(e)}")
        current_app.logger.error(f"スタックトレース: {error_trace}")        

    return redirect(url_for('game.court'))


def get_organized_match_data(match_id):
    """指定試合の 'playing' エントリーだけをコート別に整形して返す"""
    match_table = current_app.dynamodb.Table("bad-game-match_entries")

    # playing のみ取得（終了後の pending は除外）
    players = _scan_all(
        match_table,
        ProjectionExpression=(
            "user_id, display_name, skill_score, entry_status, "
            "court_number, team_side, team, team_name"
        ),
        FilterExpression=Attr("match_id").eq(match_id) & Attr("entry_status").eq("playing"),
        ConsistentRead=True,
    )

    current_app.logger.info(
        f"[get_organized_match_data] match_id={match_id}, playing件数: {len(players)}"
    )
    if not players:
        # 進行中の試合が無い/全員終了済みなら空を返す
        return {}

    def norm_team(item):
        v = item.get("team_side") or item.get("team") or item.get("team_name")
        if v is None:
            return None
        s = str(v).strip().upper()
        if s in ("A", "TEAM_A", "LEFT"):
            return "A"
        if s in ("B", "TEAM_B", "RIGHT"):
            return "B"
        return None

    def to_int_court(v):
        try:
            return int(str(v))
        except Exception:
            return 999

    # 並びを安定させる
    players.sort(key=lambda x: (to_int_court(x.get("court_number")), (norm_team(x) or "Z")))

    match_courts = {}
    for item in players:
        court = item.get("court_number")
        team = norm_team(item)
        display_name = item.get("display_name", "(no name)")
        current_app.logger.info(f"[item] court={court}, team={team}, display_name={display_name}")

        if court is None or team not in ("A", "B"):
            continue

        court_num = to_int_court(court)
        court_data = match_courts.setdefault(
            court_num,
            {"court_number": court_num, "team_a": [], "team_b": []}
        )
        (court_data["team_a"] if team == "A" else court_data["team_b"]).append(item)

    # 確認ログ
    for court, data in match_courts.items():
        a_names = [p.get("display_name", "") for p in data["team_a"]]
        b_names = [p.get("display_name", "") for p in data["team_b"]]
        current_app.logger.info(f"Court {court}: Team A = {a_names}, Team B = {b_names}")

    return match_courts

@bp_game.route("/api/skill_score")
@login_required
def api_skill_score():
    user_id = current_user.get_id()
    table = current_app.dynamodb.Table("bad-users")
    response = table.get_item(Key={"user#user_id": user_id})

    if "Item" not in response:
        return jsonify({"error": "User not found"}), 404

    score = float(response["Item"].get("skill_score", 50))
    return jsonify({"skill_score": round(score, 2)})



@bp_game.route('/create_test_data')
@login_required
def create_test_data():
    """開発用：テストデータを作成（新設計対応）- ユーザーテーブルも含む"""
    from decimal import Decimal
    from datetime import datetime
    import uuid
    
    if not current_user.administrator:        
        return redirect(url_for('index'))
    
    test_players = [
        {'display_name': 'テスト太郎', 'skill_score': 40},
        {'display_name': 'テスト花子', 'skill_score': 60},
        {'display_name': 'テスト一郎', 'skill_score': 50},
        {'display_name': 'テスト美咲', 'skill_score': 70},
        {'display_name': 'テスト健太', 'skill_score': 35},
        {'display_name': 'テスト淳二', 'skill_score': 65},
        {'display_name': '悟空', 'skill_score': 45},
        {'display_name': 'テスト愛', 'skill_score': 55},
        {'display_name': 'テスト翔太', 'skill_score': 42},
        {'display_name': 'ノーマン', 'skill_score': 58},  
        {'display_name': 'ロバート', 'skill_score': 35},  
        {'display_name': 'キャメロン', 'skill_score': 100},  
    ]
    
    now = datetime.now().isoformat()
    user_table = current_app.dynamodb.Table("bad-users")
    
    for i, player in enumerate(test_players):            
        entry_id = str(uuid.uuid4())
        user_id = f'test_user_{i}'

        # マッチテーブルにエントリを作成
        match_item = {
            'entry_id': entry_id,
            'user_id': user_id,
            'display_name': player['display_name'],
            'joined_at': now,
            'created_at': now,
            'match_id': "pending",
            'entry_status': "pending",
            'skill_score': Decimal(str(player.get('skill_score', 50))),
            'rest_count': 0,
        }
        match_table.put_item(Item=match_item)
        
        # ユーザーテーブルにユーザーを作成
        user_item = {
            'user#user_id': user_id,
            'user_id': user_id,
            'display_name': player['display_name'],
            'user_name': f"テスト_{player['display_name']}",
            'email': f"{user_id}@example.com",
            'skill_score': Decimal(str(player.get('skill_score', 50))),
            'gender': "unknown",
            'badminton_experience': "テスト",
            'organization': "テスト組織",
            'administrator': False,
            'wins': Decimal("0"),
            'losses': Decimal("0"),
            'match_count': Decimal("0"),
            'created_at': now,
            'last_updated': now
        }
        
        user_table.put_item(Item=user_item)

    return redirect(url_for('game.court'))

@bp_game.route('/clear_test_data')
@login_required
def clear_test_data():
    """開発用：test_user_ のテストデータを削除（マッチテーブルとユーザーテーブル）"""
    from boto3.dynamodb.conditions import Attr
    
    if not current_user.administrator:
        return redirect(url_for('index'))

    # マッチテーブルから削除
    last_evaluated_key = None
    while True:
        scan_kwargs = {
            'FilterExpression': Attr('user_id').begins_with("test_user_")
        }
        if last_evaluated_key:
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key

        response = match_table.scan(**scan_kwargs)
        items = response.get('Items', [])

        for item in items:
            match_table.delete_item(Key={
                'entry_id': item['entry_id']
            })

        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
    
    # ユーザーテーブルから削除
    user_table = current_app.dynamodb.Table("bad-users")
    last_evaluated_key = None
    
    while True:
        scan_kwargs = {
            'FilterExpression': 'begins_with(#uid, :prefix)',
            'ExpressionAttributeNames': {'#uid': 'user_id'},
            'ExpressionAttributeValues': {':prefix': 'test_user_'}
        }
        if last_evaluated_key:
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key
            
        response = user_table.scan(**scan_kwargs)
        items = response.get('Items', [])
        
        for item in items:
            user_table.delete_item(Key={
                'user#user_id': item['user#user_id']
            })
        
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
            
    return redirect(url_for('game.court'))

@bp_game.route('/test_data_status')
@login_required
def test_data_status():
    """開発用：テストデータの状態を確認（ユーザーテーブルも含む）"""
    if not current_user.administrator:
        flash('管理者のみ実行可能です', 'danger')
        return redirect(url_for('index'))
    
    try:
        # 1. マッチテーブルのテストデータを取得
        match_response = match_table.scan(
            FilterExpression="begins_with(user_id, :prefix)",
            ExpressionAttributeValues={":prefix": "test_user_"}
        )
        
        match_items = match_response.get('Items', [])
        
        # match_idごとにグループ化
        match_groups = {}
        for item in match_items:
            match_id = item.get('match_id', 'unknown')
            if match_id not in match_groups:
                match_groups[match_id] = []
            match_groups[match_id].append(item)
        
        # 2. ユーザーテーブルのテストデータを取得
        user_table = current_app.dynamodb.Table("bad-users")
        user_response = user_table.scan(
            FilterExpression='begins_with(#uid, :prefix)',
            ExpressionAttributeNames={'#uid': 'user_id'},
            ExpressionAttributeValues={':prefix': 'test_user_'}
        )
        
        user_items = user_response.get('Items', [])
        
        # 結果をHTMLで表示
        output = "<h1>テストデータの状態</h1>"
        
        # マッチテーブルの情報
        output += "<h2>マッチテーブル</h2>"
        output += f"<p>テストデータの総数: {len(match_items)}件</p>"
        
        for match_id, group_items in match_groups.items():
            output += f"<h3>マッチID: {match_id} ({len(group_items)}件)</h3>"
            output += "<ul>"
            for item in group_items:
                output += f"<li>{item.get('display_name')} (ID: {item.get('user_id')}, スキルスコア: {item.get('skill_score')})</li>"
            output += "</ul>"
        
        # ユーザーテーブルの情報
        output += "<h2>ユーザーテーブル</h2>"
        output += f"<p>テストデータの総数: {len(user_items)}件</p>"
        
        if user_items:
            output += "<ul>"
            for item in user_items:
                output += f"<li>{item.get('display_name')} (ID: {item.get('user_id')}, スキルスコア: {item.get('skill_score', '不明')})</li>"
            output += "</ul>"
        else:
            output += "<p>ユーザーテーブルにテストデータはありません</p>"
            
        # 操作ボタンを追加
        output += "<div style='margin-top: 20px;'>"
        output += f"<a href='{url_for('game.create_test_data')}' class='btn btn-primary'>テストデータを作成</a> "
        output += f"<a href='{url_for('game.clear_test_data')}' class='btn btn-danger'>テストデータを削除</a> "
        output += f"<a href='{url_for('game.court')}' class='btn btn-secondary'>コート画面に戻る</a>"
        output += "</div>"
        
        return output
        
    except Exception as e:
        return f"エラー: {e}"
    

  # ペアリングを実行するボタンの制御  
def has_ongoing_matches():
    """進行中の試合があるかチェック（DynamoDB版）"""
    try:
        match_table = current_app.dynamodb.Table("bad-game-match_entries")
        
        # entry_statusが"playing"のエントリーがあるかチェック
        response = match_table.scan(
            FilterExpression=Attr("entry_status").eq("playing")
        )
        
        ongoing_count = len(response.get("Items", []))
        current_app.logger.debug("has_ongoing_matches: playing_count=%d", ongoing_count)
        
        return ongoing_count > 0
        
    except Exception as e:
        current_app.logger.error(f"進行中試合チェックエラー: {str(e)}")
        return False  # エラー時は安全側に倒してペアリングを許可

def get_match_progress():
    """試合進行状況を取得（DynamoDB版）"""
    try:
        match_table = current_app.dynamodb.Table("bad-game-match_entries")
        
        # 現在の試合セッションのプレイヤーを取得
        response = match_table.scan(
            FilterExpression=Attr("entry_status").is_in(["playing", "finished"])
        )
        
        items = response.get("Items", [])
        
        # 最新のmatch_idを取得して、そのセッションのみを対象にする
        if not items:
            return 0, 0
            
        # match_idでグループ化
        match_sessions = {}
        for item in items:
            match_id = item.get("match_id", "")
            if match_id:
                if match_id not in match_sessions:
                    match_sessions[match_id] = {"playing": 0, "finished": 0}
                status = item.get("entry_status", "")
                if status in ["playing", "finished"]:
                    match_sessions[match_id][status] += 1
        
        # 最新のセッション（最も多くのプレイヤーがいるセッション）を取得
        if not match_sessions:
            return 0, 0
            
        latest_session = max(match_sessions.items(), key=lambda x: sum(x[1].values()))
        session_data = latest_session[1]
        
        total_players = session_data["playing"] + session_data["finished"]
        finished_players = session_data["finished"]
        
        # 試合数に変換（4人で1試合）
        total_matches = total_players // 4
        # 完了した試合数を推定（全員が完了したコートを計算）
        completed_matches = finished_players // 4
        
        current_app.logger.info(f"試合進行状況: {completed_matches}/{total_matches} 試合完了")
        
        return completed_matches, total_matches
        
    except Exception as e:
        current_app.logger.error(f"試合進行状況取得エラー: {str(e)}")
        return 0, 0

def get_current_match_status():
    """現在の試合状況の詳細を取得"""
    try:
        match_table = current_app.dynamodb.Table("bad-game-match_entries")
        
        response = match_table.scan(
            FilterExpression=Attr("entry_status").eq("playing")
        )
        
        playing_players = response.get("Items", [])
        
        # コート別にグループ化
        courts = {}
        for player in playing_players:
            court_num = player.get("court_number", 0)
            if court_num not in courts:
                courts[court_num] = []
            courts[court_num].append(player)
        
        return courts
        
    except Exception as e:
        current_app.logger.error(f"試合状況取得エラー: {str(e)}")
        return {}

def complete_match_for_player(entry_id):
    """プレイヤーの試合完了処理"""
    try:
        match_table = current_app.dynamodb.Table("bad-game-match_entries")
        
        # エントリーのステータスをfinishedに更新
        response = match_table.scan(
            FilterExpression=Attr("entry_id").eq(entry_id)
        )
        
        items = response.get("Items", [])
        if items:
            entry = items[0]
            match_table.update_item(
                Key={
                    'user_id': entry['user_id'],
                    'joined_at': entry['joined_at']
                },
                UpdateExpression='SET entry_status = :status',
                ExpressionAttributeValues={
                    ':status': 'finished'
                }
            )
            
            # 全試合完了チェック
            if not has_ongoing_matches():
                current_app.logger.info("全ての試合が完了しました！")
                # 必要に応じて通知やクリーンアップ処理を追加
            
        return True
        
    except Exception as e:
        current_app.logger.error(f"試合完了処理エラー: {str(e)}")
        return False
    
# routes.py の修正版
@bp_game.route('/game')  # または適切なルート名
def game_view():
    try:
        # 既存のpendingプレイヤー取得処理
        match_table = current_app.dynamodb.Table("bad-game-match_entries")
        response = match_table.scan(FilterExpression=Attr("entry_status").eq("pending"))
        entries_by_user = {}
        for e in response.get("Items", []):
            uid, joined_at = e["user_id"], e.get("joined_at", "")
            if uid not in entries_by_user or joined_at > entries_by_user[uid].get("joined_at", ""):
                entries_by_user[uid] = e
        pending_players = list(entries_by_user.values())
        
        # 進行中の試合チェック
        has_ongoing = has_ongoing_matches()
        completed, total = get_match_progress()
        current_courts = get_current_match_status()
        
        return render_template('game.html',
            pending_players=pending_players,
            has_ongoing_matches=has_ongoing,
            completed_matches=completed,
            total_matches=total,
            current_courts=current_courts
        )
        
    except Exception as e:
        current_app.logger.error(f"ゲーム画面表示エラー: {str(e)}")
        flash("データの取得中にエラーが発生しました。", "error")
        return render_template('game.html', pending_players=[], has_ongoing_matches=False)


# 管理者用のリセット機能（オプション）
@bp_game.route('/reset_ongoing_matches', methods=['POST'])
@login_required
def reset_ongoing_matches():
    """管理者が進行中の試合を強制リセット"""
    if not current_user.administrator:
        flash('管理者権限が必要です。', 'error')
        return redirect(url_for('game.court'))
    
    try:
        match_table = current_app.dynamodb.Table("bad-game-match_entries")
        
        # playing状態のエントリーを取得
        response = match_table.scan(
            FilterExpression=Attr("entry_status").eq("playing")
        )
        
        playing_entries = response.get("Items", [])
        reset_count = 0
        
        # 各エントリーをpendingに戻す
        for entry in playing_entries:
            try:
                match_table.update_item(
                    Key={
                        'user_id': entry['user_id'],
                        'joined_at': entry['joined_at']
                    },
                    UpdateExpression='SET entry_status = :status REMOVE match_id, court_number, team',
                    ExpressionAttributeValues={
                        ':status': 'pending'
                    }
                )
                reset_count += 1
            except Exception as update_error:
                current_app.logger.error(f"エントリーリセット失敗 {entry.get('user_id')}: {update_error}")
        
        flash(f'進行中の試合をリセットしました。{reset_count}人をエントリー待ちに戻しました。', 'warning')
        current_app.logger.info(f"管理者による試合リセット: {reset_count}人")
        
    except Exception as e:
        current_app.logger.error(f"試合リセットエラー: {str(e)}")
        flash('試合リセット中にエラーが発生しました。', 'error')
    
    return redirect(url_for('game.court'))

# スコア入力完了時の処理を更新する関数（既存のスコア入力処理に追加）
def complete_match_for_player(entry_id):
    """プレイヤーの試合完了処理"""
    try:
        match_table = current_app.dynamodb.Table("bad-game-match_entries")
        
        # エントリーのステータスをfinishedに更新
        response = match_table.scan(
            FilterExpression=Attr("entry_id").eq(entry_id)
        )
        
        items = response.get("Items", [])
        if items:
            entry = items[0]
            match_table.update_item(
                Key={
                    'user_id': entry['user_id'],
                    'joined_at': entry['joined_at']
                },
                UpdateExpression='SET entry_status = :status',
                ExpressionAttributeValues={
                    ':status': 'finished'
                }
            )
            
            # 全試合完了チェック
            if not has_ongoing_matches():
                current_app.logger.info("全ての試合が完了しました！")
                # 必要に応じて通知やクリーンアップ処理を追加
            
        return True
        
    except Exception as e:
        current_app.logger.error(f"試合完了処理エラー: {str(e)}")
        return False
   


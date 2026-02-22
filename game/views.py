from flask import Blueprint, render_template, redirect, url_for, flash, request, current_app
from flask_login import login_required, current_user
import boto3
import uuid
from datetime import datetime, date, time, timedelta, timezone
import random
from boto3.dynamodb.conditions import Key, Attr, And
from flask import jsonify, session
from collections import Counter
from .game_utils import (
    Player,
    generate_ai_best_pairings,
    generate_balanced_pairs_and_matches,
    parse_players,
    sync_match_entries_with_updated_skills,
    update_trueskill_for_players_and_return_updates
)
from utils.timezone import JST
import re
from decimal import Decimal
import time
import logging
from botocore.exceptions import ClientError
from zoneinfo import ZoneInfo
from typing import List, Tuple, Dict, Any
from .game_utils import normalize_user_pk


JST = ZoneInfo("Asia/Tokyo")

logger = logging.getLogger(__name__)

bp_game = Blueprint('game', __name__)


# DynamoDBリソース取得
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
match_table = dynamodb.Table('bad-game-match_entries')
game_meta_table = dynamodb.Table('bad-game-matches')
user_table = dynamodb.Table("bad-users") 


def debug_duplicates(entry_table):
    # rest_event 除外（typeなし or type != rest_event）
    flt = (Attr("type").ne("rest_event") | Attr("type").not_exists())
    resp = entry_table.scan(FilterExpression=flt)
    items = resp.get("Items", [])

    by_uid_status = defaultdict(int)
    by_uid = defaultdict(int)

    for it in items:
        uid = it.get("user_id", "-")
        st  = it.get("entry_status", "-")
        by_uid[uid] += 1
        by_uid_status[(uid, st)] += 1

    # “異常候補”だけ出す：pending が2以上 or playing が2以上
    suspicious = []
    for (uid, st), cnt in by_uid_status.items():
        if st in ("pending", "playing") and cnt >= 2:
            suspicious.append((uid, st, cnt))

    current_app.logger.info("[dup-check] normal_entries=%d unique_users=%d suspicious=%s",
                            len(items), len(by_uid), suspicious[:20])
    

@bp_game.route("/court")
@login_required
def court():
    logger = current_app.logger

    try:
        # セッション初期値
        session.setdefault("score_format", "21")

        match_table = current_app.dynamodb.Table("bad-game-match_entries")

        # ★履歴(rest_event)は現役一覧から除外
        filter_expr = (
            Attr("entry_status").is_in(["pending", "resting", "playing"])
            & (Attr("type").not_exists() | Attr("type").ne("rest_event"))
        )

        items = _scan_all(
            match_table,
            FilterExpression=filter_expr,
            ConsistentRead=True
        )

        # --- デフォルト値補完（ログ無し） ---
        for it in items:
            it["rest_count"]  = it.get("rest_count")  or 0
            it["match_count"] = it.get("match_count") or 0
            it["join_count"]  = it.get("join_count")  or 0

        # --- ステータス別に分類 ---
        pending_players = []
        resting_players = []
        playing_players = []

        for it in items:
            st = it.get("entry_status")
            if st == "pending":
                pending_players.append(it)
            elif st == "resting":
                resting_players.append(it)
            elif st == "playing":
                playing_players.append(it)

        # --- ユーザー状態の判定（自分の entry は「最新1件」を採用） ---
        user_id = current_user.get_id()

        is_registered = False
        is_resting = False
        is_playing = False
        skill_score = 50
        match_count = 0

        def _ts(it):
            # 文字列ISO前提。無ければ空文字で最小扱い
            return str(it.get("updated_at") or it.get("joined_at") or it.get("created_at") or "")

        my_entries = [it for it in items if it.get("user_id") == user_id]

        me = None
        if my_entries:
            me = max(my_entries, key=_ts)  # 最新を採用
            st = me.get("entry_status")

            is_registered = (st == "pending")
            is_resting    = (st == "resting")
            is_playing    = (st == "playing")

            # いったん entry の値（無ければ50）
            skill_score = me.get("skill_score", 50)
            match_count = me.get("match_count", 0) or 0

        # =========================================================
        # ★ 追加：ユーザーテーブルを正として skill_score を上書き ★
        #   -> 試合後に「スキル永続化」した値(例:63.74)がここで反映される
        # =========================================================
        try:
            # あなたのユーザーテーブル名に合わせて変更
            users_table = current_app.dynamodb.Table("bad-users")  # 例: "bad-users"
           
            resp_u = users_table.get_item(
                Key={"user#user_id": user_id},
                ConsistentRead=True
            )
            u = resp_u.get("Item")

            if u and u.get("skill_score") is not None:
                skill_score = u["skill_score"]  # DecimalのままでOK
        except Exception:
            logger.exception("[court] user skill_score reload failed")

        # --- 進行中試合関連（INFO最小、詳細はDEBUG） ---
        has_ongoing = has_ongoing_matches()
        completed, total = get_match_progress()
        current_courts = get_current_match_status()

        match_id = get_latest_match_id()
        if match_id:
            match_courts = get_organized_match_data(match_id)
        else:
            match_courts = {}

        logger.info(
            "[court] total=%d pending=%d resting=%d playing=%d user=%s state=%s ongoing=%s progress=%s/%s match_id=%s",
            len(items), len(pending_players), len(resting_players), len(playing_players),
            user_id,
            ("playing" if is_playing else "resting" if is_resting else "pending" if is_registered else "none"),
            has_ongoing, completed, total, match_id or "-"
        )

        if logger.isEnabledFor(10):  # DEBUG
            logger.debug("[court] current_courts=%s", current_courts)
            if match_id:
                logger.debug("[court] match_courts keys=%d", len(match_courts))
            if my_entries and me:
                logger.debug(
                    "[court][me] entries=%d picked_ts=%s picked_entry_skill=%s final_skill=%s",
                    len(my_entries), _ts(me), me.get("skill_score"), skill_score
                )

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
        logger.exception("[court] error")
        return "コート画面でエラーが発生しました", 500


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
                    logger.debug("[get_latest_match_id] ongoing match_id=%s (meta)", current_match_id)
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

        logger.debug("[get_latest_match_id] ongoing match_id=%s (meta)", current_match_id)
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


def get_players_status(status, user_id=None, debug_dump_all=False, debug_sample=0):
    """
    status のエントリーを取得して返す。
    - 本番: 件数のみ（INFO）
    - 開発: サンプル表示（DEBUG）、debug_sample > 0 で有効化
    - 全件dump: debug_dump_all=True（トラブルシュート専用）
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
            
            # 最大10件に制限（ログ爆発防止）
            for it in all_items[:10]:
                logger.debug(
                    "[all] name=%s status=%s user_id=%s",
                    it.get("display_name"),
                    it.get("entry_status"),
                    it.get("user_id"),
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

        # --- デフォルト値補完（ログなし） ---
        for it in items:
            it["rest_count"] = it.get("rest_count", 0)
            it["match_count"] = it.get("match_count", 0)
            it["join_count"] = it.get("join_count", 0)

        # --- INFO：件数のみ（常時） ---
        logger.debug(
            "[get_players_status] status=%s user_id=%s count=%d",
            status,
            user_id or "-",
            len(items),
        )

        # --- DEBUG：サンプル表示（debug_sample > 0 の時のみ） ---
        if debug_sample > 0 and logger.isEnabledFor(logging.DEBUG):
            for it in items[:debug_sample]:
                logger.debug(
                    "[%s] name=%s user_id=%s entry_id=%s",
                    status,
                    it.get("display_name"),
                    it.get("user_id"),
                    it.get("entry_id"),
                )

        return items

    except Exception as e:
        logger.exception(
            "[get_players_status] error status=%s user_id=%s",
            status,
            user_id or "-",
        )
        return []    
    
   
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

        # entry_statusのみでフィルタ。メタ行は除外。強整合読みを推奨
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


@bp_game.route("/entry", methods=["POST"])
@login_required
def entry():
    """明示的な参加登録（重複チェック＋新規登録）"""
    user_id = current_user.get_id()
    now = datetime.now(JST).isoformat()
    current_app.logger.info(f"[ENTRY] 参加登録開始: {user_id}")

    match_table = current_app.dynamodb.Table("bad-game-match_entries")
    user_table = current_app.dynamodb.Table("bad-users")

    # 1) すでにpending登録されていないかチェック
    response = match_table.scan(
        FilterExpression=Attr("user_id").eq(user_id) & Attr("entry_status").is_in(["pending", "resting", "playing"])
    )
    existing = response.get("Items", [])

    if existing:
        current_app.logger.info("[ENTRY] すでに参加登録済みのためスキップ")
        return redirect(url_for("game.court"))

    # 2) 他の状態のエントリがあれば削除（念のため）
    cleanup_response = match_table.scan(
        FilterExpression=Attr("user_id").eq(user_id)
    )
    for item in cleanup_response.get("Items", []):
        match_table.delete_item(Key={"entry_id": item["entry_id"]})

    # -------------------------------------------------------
    # 3) ユーザー情報取得（user# プレフィックスの有無を吸収する暫定ロジック）
    # -------------------------------------------------------
    user_data = None
    
    # まずは現在の user_id でそのまま検索
    resp = user_table.get_item(Key={"user#user_id": user_id})
    user_data = resp.get("Item")    

    # 最終的にデータが見つかったか確認
    if user_data:
        skill_score = user_data.get("skill_score", 50)
        display_name = user_data.get("display_name", "未設定")
    else:
        # どちらでも見つからなかった場合のフォールバック
        current_app.logger.warning(f"[ENTRY] ユーザーデータがDBに見つかりません: {user_id}")
        skill_score = 50
        display_name = "未設定"
    # -------------------------------------------------------

    # 4) 新規登録
    entry_item = {
        "entry_id": str(uuid.uuid4()),
        "user_id": user_id,
        "match_id": "pending",
        "entry_status": "pending",
        "display_name": display_name,
        "skill_score": Decimal(str(skill_score)),
        "joined_at": now,
        "created_at": now,
        "rest_count": 0,
        "match_count": 0,
        "join_count": 1
    }
    match_table.put_item(Item=entry_item)
    current_app.logger.info(f"[ENTRY] 新規参加登録完了: {entry_item['entry_id']}, 名前: {display_name}")

    return redirect(url_for("game.court"))


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


# @bp_game.route('/create_pairings', methods=["POST"])
# @login_required
# def create_pairings():

#     if has_ongoing_matches():
#         flash('進行中の試合があるため、新しいペアリングを実行できません。全ての試合のスコア入力を完了してください。', 'warning')
#         return redirect(url_for('game.court'))

#     try:
#         import boto3
#         from botocore.exceptions import ClientError
#         from boto3.dynamodb.conditions import Attr
#         from datetime import datetime
#         import random
#         from decimal import Decimal

#         max_courts = min(max(int(request.form.get("max_courts", 3)), 1), 6)

#         # 1) pendingエントリー取得 & ユーザーごとに最新だけ残す
#         entry_table = current_app.dynamodb.Table("bad-game-match_entries")
#         response = entry_table.scan(FilterExpression=Attr("entry_status").eq("pending"))

#         entries_by_user = {}
#         for e in response.get("Items", []):
#             uid, joined_at = e["user_id"], e.get("joined_at", "")
#             if uid not in entries_by_user or joined_at > entries_by_user[uid].get("joined_at", ""):
#                 entries_by_user[uid] = e
#         entries = list(entries_by_user.values())

#         current_app.logger.info(
#             "[pairing] pending_entries=%d max_courts=%d",
#             len(entries), max_courts
#         )

#         if len(entries) < 4:
#             flash("4人以上のエントリーが必要です。", "warning")
#             return redirect(url_for("game.court"))

#         # 2) 優先順位（試合少ない→ランダム）
#         sorted_entries = sorted(entries, key=lambda e: (
#             e.get("match_count", 0),
#             random.random()
#         ))

#         # 3) required_players / waiting_count
#         cap_by_courts = min(max_courts * 4, len(sorted_entries))
#         required_players = cap_by_courts - (cap_by_courts % 4)
#         waiting_count = len(sorted_entries) - required_players

#         current_app.logger.info(
#             "[pairing] cap_by_courts=%d required_players=%d waiting_count=%d",
#             cap_by_courts, required_players, waiting_count
#         )

#         # 4) 待機枠バイアス（skill低い2名の待機確率を微増）
#         if waiting_count > 0:
#             skill_sorted = sorted(
#                 [(e["entry_id"], Decimal(e.get("skill_score", 50))) for e in sorted_entries],
#                 key=lambda x: x[1]
#             )
#             low2_ids = {eid for eid, _ in skill_sorted[:2]}

#             LOW_BIAS = random.uniform(1.15, 1.3)
#             weights = [(LOW_BIAS if e["entry_id"] in low2_ids else 1.0) for e in sorted_entries]

#             chosen_waiting = weighted_sample_no_replace(sorted_entries, weights, waiting_count)
#             waiting_ids = {e["entry_id"] for e in chosen_waiting}

#             active_entries = [e for e in sorted_entries if e["entry_id"] not in waiting_ids]
#             waiting_entries = [e for e in sorted_entries if e["entry_id"] in waiting_ids]

#             current_app.logger.info("[wait-bias] waiting_count=%s, low_bias=%s", waiting_count, LOW_BIAS)

#             # --- 追加ログ：待機が確定した瞬間 ---
#             current_app.logger.info(
#                 "[wait] required=%d waiting=%d active=%d",
#                 required_players, waiting_count, len(active_entries)
#             )
#             current_app.logger.info(
#                 "[wait] waiting_names=%s",
#                 ", ".join([e.get("display_name", "?") for e in waiting_entries]) or "(none)"
#             )
#             current_app.logger.info(
#                 "[wait] waiting_ids=%s",
#                 ", ".join([e.get("entry_id", "?") for e in waiting_entries]) or "(none)"
#             )
#         else:
#             active_entries = sorted_entries
#             waiting_entries = []

#             # --- 追加ログ：待機なし ---
#             current_app.logger.info("[wait] waiting_count=0 (none)")

#         random.shuffle(active_entries)

#         # --- 追加ログ：active をシャッフルした結果 ---
#         current_app.logger.info(
#             "[active] active_names(shuffled)=%s",
#             ", ".join([e.get("display_name", "?") for e in active_entries]) or "(none)"
#         )

#         # 5) Player変換
#         name_to_id, players, waiting_players = {}, [], []

#         for e in active_entries:
#             name = e["display_name"]

#             # skill_score と skill_sigma を取得
#             skill_score = float(e.get("skill_score", 50.0))
#             skill_sigma = float(e.get("skill_sigma", 8.333))

#             # 保守的スキルを計算
#             conservative = skill_score - 3 * skill_sigma

#             # Player オブジェクト作成
#             p = Player(name, conservative, e.get("gender", "M"))
#             p.skill_score = skill_score
#             p.skill_sigma = skill_sigma
#             p.match_count = e.get("match_count", 0)
#             p.rest_count = e.get("rest_count", 0)
#             name_to_id[name] = e["entry_id"]
#             players.append(p)

#         # 👇 waiting_entries の処理
#         for e in waiting_entries:
#             name = e["display_name"]

#             # skill_score と skill_sigma を取得
#             skill_score = float(e.get("skill_score", 50.0))
#             skill_sigma = float(e.get("skill_sigma", 8.333))

#             # 保守的スキルを計算
#             conservative = skill_score - 3 * skill_sigma

#             # Player オブジェクト作成
#             p = Player(name, conservative, e.get("gender", "M"))
#             p.skill_score = skill_score
#             p.skill_sigma = skill_sigma
#             p.match_count = e.get("match_count", 0)
#             p.rest_count = e.get("rest_count", 0)
#             name_to_id[name] = e["entry_id"]
#             waiting_players.append(p)

#         # --- 追加ログ：Player変換後の人数 ---
#         current_app.logger.info(
#             "[players] active_players=%d waiting_players(initial)=%d",
#             len(players), len(waiting_players)
#         )

#         # 6) ペア生成
#         match_id = generate_match_id()
#         pairs, matches, additional_waiting_players = generate_balanced_pairs_and_matches(players, max_courts)
#         # matches, additional_waiting_players = generate_ai_best_pairings(players, max_courts, iterations=1000)

#         # --- 追加ログ：matches 確定直後 ---
#         current_app.logger.info(
#             "[matches] match_id=%s courts=%d additional_waiting=%d",
#             match_id, len(matches), len(additional_waiting_players)
#         )
#         for i, ((a1, a2), (b1, b2)) in enumerate(matches, 1):
#             current_app.logger.info(
#                 "[match] C%d: A=[%s,%s] vs B=[%s,%s]",
#                 i, a1.name, a2.name, b1.name, b2.name
#             )
#         if additional_waiting_players:
#             current_app.logger.info(
#                 "[wait] additional_waiting(from_unused_pairs)=%s",
#                 ", ".join([p.name for p in additional_waiting_players])
#             )

#         waiting_players.extend(additional_waiting_players)

#         if not matches:
#             flash("試合を作成できませんでした（人数不足など）。", "warning")
#             return redirect(url_for("game.court"))

#         # --- 追加ログ：待機者の最終確定 ---
#         current_app.logger.info(
#             "[wait] final_waiting_names=%s",
#             ", ".join([p.name for p in waiting_players]) or "(none)"
#         )

#         # -------------------------------------------------
#         # TransactWriteItems：metaロック + 試合参加者更新
#         #   最大 25件制限：meta(1) + 4*len(matches)
#         # -------------------------------------------------
#         max_tx = 25
#         need_tx = 1 + 4 * len(matches)
#         if need_tx > max_tx:
#             current_app.logger.error("[meta] tx items exceed limit: need=%s", need_tx)
#             flash("試合数が多すぎて作成できませんでした。コート数を減らしてください。", "danger")
#             return redirect(url_for("game.court"))

#         now_jst = datetime.now(JST).isoformat()
#         dynamodb_client = boto3.client('dynamodb', region_name='ap-northeast-1')

#         tx_items = []
#         meta_pk_str = "meta#current"

#         # (1) meta#current を playing に（すでに playing なら弾く）
#         tx_items.append({
#             "Update": {
#                 "TableName": "bad-game-matches",
#                 "Key": {"match_id": {"S": "meta#current"}},
#                 "UpdateExpression": (
#                     "SET #st = :playing, #cm = :mid, #cc = :cc, #ua = :now, #sa = :now"
#                 ),
#                 "ConditionExpression": "attribute_not_exists(#st) OR #st <> :playing",
#                 "ExpressionAttributeNames": {
#                     "#st": "status",
#                     "#cm": "current_match_id",
#                     "#cc": "court_count",
#                     "#ua": "updated_at",
#                     "#sa": "started_at",
#                 },
#                 "ExpressionAttributeValues": {
#                     ":playing": {"S": "playing"},
#                     ":mid": {"S": str(match_id)},
#                     ":cc": {"N": str(len(matches))},
#                     ":now": {"S": now_jst},
#                 },
#             }
#         })

#         # (2) pending の参加者を playing に（試合ID・コート・チームを付与）
#         for court_num, ((a1, a2), (b1, b2)) in enumerate(matches, 1):
#             for pl, team in [(a1, "A"), (a2, "A"), (b1, "B"), (b2, "B")]:
#                 entry_id = str(name_to_id[pl.name])

#                 tx_items.append({
#                     "Update": {
#                         "TableName": "bad-game-match_entries",
#                         "Key": {"entry_id": {"S": entry_id}},
#                         "UpdateExpression": (
#                             "SET entry_status=:playing, match_id=:mid, court_number=:c, team=:t, updated_at=:now"
#                         ),
#                         "ConditionExpression": "entry_status = :pending",
#                         "ExpressionAttributeValues": {
#                             ":playing": {"S": "playing"},
#                             ":pending": {"S": "pending"},
#                             ":mid": {"S": str(match_id)},
#                             ":c": {"N": str(court_num)},
#                             ":t": {"S": team},
#                             ":now": {"S": now_jst},
#                         },
#                     }
#                 })

#         try:
#             dynamodb_client.transact_write_items(TransactItems=tx_items)

#             current_app.logger.info(
#                 "[meta] lock+players committed: current_match_id=%s court_count=%s",
#                 match_id, len(matches)
#             )

#         except ClientError as e:
#             if e.response.get("Error", {}).get("Code") == "TransactionCanceledException":
#                 current_app.logger.warning("[meta] lock tx canceled: %s", e)
#                 flash("進行中の試合があるためペアリングできませんでした。", "warning")
#                 return redirect(url_for("game.court"))
#             raise

#         current_app.logger.info("ペアリング成功: %s試合, %s人待機", len(matches), len(waiting_players))
#         return redirect(url_for("game.court"))

#     except Exception as e:
#         current_app.logger.error("[ペア生成エラー] %s", str(e), exc_info=True)
#         flash("試合の作成中にエラーが発生しました。", "danger")
#         return redirect(url_for("game.court"))


# @bp_game.route('/create_pairings', methods=["POST"])
# @login_required
# def create_pairings():

#     if has_ongoing_matches():
#         flash('進行中の試合があるため、新しいペアリングを実行できません。全ての試合のスコア入力を完了してください。', 'warning')
#         return redirect(url_for('game.court'))

#     try:
#         import boto3
#         import uuid
#         from botocore.exceptions import ClientError
#         from boto3.dynamodb.conditions import Attr
#         from datetime import datetime
#         import random

#         max_courts = min(max(int(request.form.get("max_courts", 3)), 1), 6)

#         # =========================================================
#         # 0) 「交互に使う」ための状態取得
#         #    - meta#pairing に last_mode を保存して試合ごとに交互切り替え
#         # =========================================================
#         meta_table = current_app.dynamodb.Table("bad-game-matches")

#         pairing_meta = meta_table.get_item(
#             Key={"match_id": "meta#pairing"},
#             ConsistentRead=True
#         ).get("Item", {}) or {}

#         last_mode = pairing_meta.get("last_mode")  # "random" / "ai"

#         # 試合ごとに交互切り替え
#         mode = "ai" if last_mode == "random" else "random"

#         current_app.logger.info(
#             "[pairing-mode] last=%s -> now=%s", last_mode, mode
#         )
#         # =========================================================
#         # 1) pendingエントリー取得 & ユーザーごとに最新だけ残す
#         # =========================================================
#         entry_table = current_app.dynamodb.Table("bad-game-match_entries")
#         response = entry_table.scan(FilterExpression=Attr("entry_status").eq("pending"))

#         entries_by_user = {}
#         for e in response.get("Items", []):
#             uid, joined_at = e["user_id"], e.get("joined_at", "")
#             if uid not in entries_by_user or joined_at > entries_by_user[uid].get("joined_at", ""):
#                 entries_by_user[uid] = e
#         entries = list(entries_by_user.values())

#         current_app.logger.info(
#             "[pairing] pending_entries=%d max_courts=%d",
#             len(entries), max_courts
#         )

#         if len(entries) < 4:
#             flash("4人以上のエントリーが必要です。", "warning")
#             return redirect(url_for("game.court"))

#         # 2) 優先順位（試合少ない→ランダム）
#         sorted_entries = sorted(entries, key=lambda e: (
#             e.get("match_count", 0),
#             random.random()
#         ))

#         # 3) required_players / waiting_count
#         cap_by_courts = min(max_courts * 4, len(sorted_entries))
#         required_players = cap_by_courts - (cap_by_courts % 4)
#         waiting_count = len(sorted_entries) - required_players

#         current_app.logger.info(
#             "[pairing] cap_by_courts=%d required_players=%d waiting_count=%d",
#             cap_by_courts, required_players, waiting_count
#         )

#         # 4) 待機者選出（キュー方式）
#         if waiting_count > 0:
#             active_entries, waiting_entries = _select_waiting_entries(sorted_entries, waiting_count)
#         else:
#             active_entries, waiting_entries = sorted_entries, []
#             current_app.logger.info("[wait] waiting_count=0 (none)")

#         # active 側だけシャッフル（待機はキュー順のまま）
#         random.shuffle(active_entries)
#         current_app.logger.info(
#             "[active] active_names(shuffled)=%s",
#             ", ".join([e.get("display_name", "?") for e in active_entries]) or "(none)"
#         )

#         # 5) Player変換
#         name_to_id, players, waiting_players = {}, [], []

#         for e in active_entries:
#             name = e["display_name"]
#             skill_score = float(e.get("skill_score", 50.0))
#             skill_sigma = float(e.get("skill_sigma", 8.333))
#             conservative = skill_score - 3 * skill_sigma

#             p = Player(name, conservative, e.get("gender", "M"))
#             p.user_id = e.get("user_id")
#             p.conservative = conservative
#             p.skill_score = skill_score
#             p.skill_sigma = skill_sigma
#             p.match_count = e.get("match_count", 0)
#             p.rest_count = e.get("rest_count", 0)

#             name_to_id[name] = e["entry_id"]
#             players.append(p)

#         for e in waiting_entries:
#             name = e["display_name"]
#             skill_score = float(e.get("skill_score", 50.0))
#             skill_sigma = float(e.get("skill_sigma", 8.333))
#             conservative = skill_score - 3 * skill_sigma

#             p = Player(name, conservative, e.get("gender", "M"))
#             p.user_id = e.get("user_id")
#             p.conservative = conservative
#             p.skill_score = skill_score
#             p.skill_sigma = skill_sigma
#             p.match_count = e.get("match_count", 0)
#             p.rest_count = e.get("rest_count", 0)

#             name_to_id[name] = e["entry_id"]
#             waiting_players.append(p)

#         current_app.logger.info(
#             "[players] active_players=%d waiting_players(initial)=%d",
#             len(players), len(waiting_players)
#         )

#         # =========================================================
#         # 6) ペア生成（交互に実行）
#         # =========================================================
#         match_id = generate_match_id()

#         if mode == "ai":
#             matches, additional_waiting_players = generate_ai_best_pairings(players, max_courts, iterations=1000)
#             pairs = []
#         else:
#             pairs, matches, additional_waiting_players = generate_balanced_pairs_and_matches(players, max_courts)

#         current_app.logger.info(
#             "[matches] match_id=%s courts=%d additional_waiting=%d mode=%s",
#             match_id, len(matches), len(additional_waiting_players), mode
#         )

#         for i, ((a1, a2), (b1, b2)) in enumerate(matches, 1):
#             current_app.logger.info(
#                 "[match] C%d: A=[%s,%s] vs B=[%s,%s]",
#                 i, a1.name, a2.name, b1.name, b2.name
#             )

#         if additional_waiting_players:
#             current_app.logger.info(
#                 "[wait] additional_waiting(from_unused_pairs)=%s",
#                 ", ".join([p.name for p in additional_waiting_players])
#             )

#         waiting_players.extend(additional_waiting_players)

#         if not matches:
#             flash("試合を作成できませんでした（人数不足など）。", "warning")
#             return redirect(url_for("game.court"))

#         current_app.logger.info(
#             "[wait] final_waiting_names=%s",
#             ", ".join([p.name for p in waiting_players]) or "(none)"
#         )

#         # -------------------------------------------------
#         # TransactWriteItems：metaロック + 試合参加者更新
#         #   最大 25件制限：meta(1) + 4*len(matches)
#         # -------------------------------------------------
#         max_tx = 25
#         need_tx = 1 + 4 * len(matches)
#         if need_tx > max_tx:
#             current_app.logger.error("[meta] tx items exceed limit: need=%s", need_tx)
#             flash("試合数が多すぎて作成できませんでした。コート数を減らしてください。", "danger")
#             return redirect(url_for("game.court"))

#         now_jst = datetime.now(JST).isoformat()
#         dynamodb_client = boto3.client('dynamodb', region_name='ap-northeast-1')

#         tx_items = []

#         # (1) meta#current を playing に（すでに playing なら弾く）
#         tx_items.append({
#             "Update": {
#                 "TableName": "bad-game-matches",
#                 "Key": {"match_id": {"S": "meta#current"}},
#                 "UpdateExpression": (
#                     "SET #st = :playing, #cm = :mid, #cc = :cc, #ua = :now, #sa = :now"
#                 ),
#                 "ConditionExpression": "attribute_not_exists(#st) OR #st <> :playing",
#                 "ExpressionAttributeNames": {
#                     "#st": "status",
#                     "#cm": "current_match_id",
#                     "#cc": "court_count",
#                     "#ua": "updated_at",
#                     "#sa": "started_at",
#                 },
#                 "ExpressionAttributeValues": {
#                     ":playing": {"S": "playing"},
#                     ":mid": {"S": str(match_id)},
#                     ":cc": {"N": str(len(matches))},
#                     ":now": {"S": now_jst},
#                 },
#             }
#         })

#         # (2) pending の参加者を playing に（試合ID・コート・チームを付与）
#         for court_num, ((a1, a2), (b1, b2)) in enumerate(matches, 1):
#             for pl, team in [(a1, "A"), (a2, "A"), (b1, "B"), (b2, "B")]:
#                 entry_id = str(name_to_id[pl.name])

#                 tx_items.append({
#                     "Update": {
#                         "TableName": "bad-game-match_entries",
#                         "Key": {"entry_id": {"S": entry_id}},
#                         "UpdateExpression": (
#                             "SET entry_status=:playing, match_id=:mid, court_number=:c, team=:t, updated_at=:now"
#                         ),
#                         "ConditionExpression": "entry_status = :pending",
#                         "ExpressionAttributeValues": {
#                             ":playing": {"S": "playing"},
#                             ":pending": {"S": "pending"},
#                             ":mid": {"S": str(match_id)},
#                             ":c": {"N": str(court_num)},
#                             ":t": {"S": team},
#                             ":now": {"S": now_jst},
#                         },
#                     }
#                 })

#         try:
#             dynamodb_client.transact_write_items(TransactItems=tx_items)
#             current_app.logger.info(
#                 "[meta] lock+players committed: current_match_id=%s court_count=%s",
#                 match_id, len(matches)
#             )
#         except ClientError as e:
#             if e.response.get("Error", {}).get("Code") == "TransactionCanceledException":
#                 current_app.logger.warning("[meta] lock tx canceled: %s", e)
#                 flash("進行中の試合があるためペアリングできませんでした。", "warning")
#                 return redirect(url_for("game.court"))
#             raise

#         # =========================================================
#         # ★交互モードの状態を保存（成功後だけ）
#         # =========================================================
#         meta_table.update_item(
#             Key={"match_id": "meta#pairing"},
#             UpdateExpression=(
#                 "SET last_mode=:m, cycle_gen=:g, cycle_started_at=:cs, last_match_id=:mid, updated_at=:now"
#             ),
#             ExpressionAttributeValues={
#                 ":m": mode,
#                 ":g": current_gen,
#                 ":cs": current_cycle_started_at,
#                 ":mid": str(match_id),
#                 ":now": now_jst,
#             }
#         )

#         # ==============================
#         # ★ここ：休み「累計」更新 + 休み「イベント」追加
#         #   ※ user_id は「uid未定義」にならないように必ず後で入れる
#         # ==============================
#         if waiting_players:
#             for wp in waiting_players:
#                 entry_id = str(name_to_id.get(wp.name))
#                 if not entry_id:
#                     continue

#                 # (1) 既存レコードに「休み累計/最新」だけ追記
#                 entry_table.update_item(
#                     Key={"entry_id": entry_id},
#                     UpdateExpression=(
#                         "SET last_rest_match_id=:mid, last_rest_at=:now, last_rest_reason=:rr, updated_at=:now "
#                         "ADD rest_count :one"
#                     ),
#                     ExpressionAttributeValues={
#                         ":mid": str(match_id),
#                         ":now": now_jst,
#                         ":rr": "not_selected",
#                         ":one": 1,
#                     },
#                 )

#                 # (2) 休みイベントを別レコードとして追加
#                 rest_event_id = str(uuid.uuid4())
#                 rest_item = {
#                     "entry_id": rest_event_id,
#                     "type": "rest_event",
#                     "match_id": str(match_id),
#                     "display_name": wp.name,
#                     "entry_status": "resting",
#                     "reason": "not_selected",
#                     "court_number": 0,
#                     "team": "R",
#                     "created_at": now_jst,
#                     "updated_at": now_jst,
#                     "source_entry_id": entry_id,
#                 }

#                 uid = getattr(wp, "user_id", None)
#                 if uid:  # None/空文字なら入れない（NULLを書かない）
#                     rest_item["user_id"] = str(uid)

#                 entry_table.put_item(Item=rest_item)

#         current_app.logger.info("ペアリング成功: %s試合, %s人待機 (mode=%s)", len(matches), len(waiting_players), mode)
#         return redirect(url_for("game.court"))

#     except Exception as e:
#         current_app.logger.error("[ペア生成エラー] %s", str(e), exc_info=True)
#         flash("試合の作成中にエラーが発生しました。", "danger")
#         return redirect(url_for("game.court"))

    
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


def persist_skill_to_bad_users(updated_skills: dict):
    users_table = current_app.dynamodb.Table("bad-users")
    ok, ng = 0, 0

    for uid, vals in (updated_skills or {}).items():
        try:
            new_s = Decimal(str(round(float(vals.get("skill_score", 25.0)), 2)))
            new_g = Decimal(str(round(float(vals.get("skill_sigma", 8.333)), 4)))

            users_table.update_item(
                Key={"user#user_id": normalize_user_pk(uid)},
                UpdateExpression="SET skill_score=:s, skill_sigma=:g",
                ExpressionAttributeValues={":s": new_s, ":g": new_g},
                # ★存在しないユーザーは作らない
                ConditionExpression="attribute_exists(#pk)",
                ExpressionAttributeNames={"#pk": "user#user_id"},
                ReturnValues="NONE",
            )

            current_app.logger.info(
                "[bad-users] Updated uid=%s (score: %s, sigma: %s)",
                str(uid), str(new_s), str(new_g)
            )
            ok += 1

        except ClientError as e:
            ng += 1
            code = e.response.get("Error", {}).get("Code", "Unknown")
            if code == "ConditionalCheckFailedException":
                current_app.logger.warning("[bad-users] Skip (not exists) uid=%s", str(uid))
            else:
                current_app.logger.error("[bad-users] ClientError uid=%s code=%s", str(uid), code)

        except Exception as e:
            ng += 1
            current_app.logger.error("[bad-users] Unexpected error uid=%s err=%s", str(uid), str(e))

    current_app.logger.info("[bad-users] Persist finished. ok=%d ng=%d", ok, ng)
    return ok, ng

    
@bp_game.route("/finish_current_match", methods=["POST"])
@login_required
def finish_current_match():
    """
    安全版 finish_current_match

    目的:
    - meta#current が playing のときだけ終了処理
    - 「全コート分のスコア送信が揃っていない場合」は 409 で拒否（重要）
    - 送信が揃ってから TrueSkill 更新 → エントリー同期 → meta解除&playing→pending を確定

    前提:
    - meta#current に court_count を保存している（推奨）
    - bad-game-results に match_id + court_number の結果が保存される
    """

    import re
    from datetime import datetime
    import boto3
    from botocore.exceptions import ClientError
    from boto3.dynamodb.conditions import Attr

    try:
        # =========================================================
        # 0) meta#current から進行中 match_id / court_count を取得
        # =========================================================
        meta_pk = "meta#current"
        meta_table = current_app.dynamodb.Table("bad-game-matches")

        meta_resp = meta_table.get_item(Key={"match_id": meta_pk}, ConsistentRead=True)
        meta_item = meta_resp.get("Item") or {}

        status = meta_item.get("status")
        match_id = meta_item.get("current_match_id")

        # court_count は "3" のように str の可能性があるので int へ
        court_count_raw = meta_item.get("court_count")
        try:
            court_count = int(court_count_raw) if court_count_raw is not None else None
        except Exception:
            court_count = None

        if status != "playing" or not match_id:
            current_app.logger.warning(
                "⚠️ アクティブな試合が見つかりません(meta). status=%s, current_match_id=%s",
                status, match_id
            )
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"success": False, "error": "アクティブな試合が見つかりません"}), 400
            return "アクティブな試合が見つかりません", 400

        current_app.logger.info("🏁 試合終了処理開始(meta): match_id=%s court_count=%s", match_id, court_count)

        # (任意) ID形式チェック
        if not re.compile(r"^\d{8}_\d{6}$").match(match_id):
            current_app.logger.warning("⚠️ 非標準形式の試合ID: %s", match_id)

        now_jst = datetime.now(JST).isoformat()

        # 直接 boto3 client を作成（Transaction 用）
        dynamodb_client = boto3.client("dynamodb", region_name="ap-northeast-1")

        # =========================================================
        # 1) 「全コート送信済み」チェック（ここが最重要）
        #    → 未送信があれば finish させない（409）
        # =========================================================
        results_table = current_app.dynamodb.Table("bad-game-results")

        def scan_all_results_for_match(mid: str):
            items = []
            kwargs = {"FilterExpression": Attr("match_id").eq(mid)}
            while True:
                resp = results_table.scan(**kwargs)
                items.extend(resp.get("Items", []))
                lek = resp.get("LastEvaluatedKey")
                if not lek:
                    break
                kwargs["ExclusiveStartKey"] = lek
            return items

        match_results = scan_all_results_for_match(match_id)
        submitted_results_count = len(match_results)

        # court_count が取れるなら厳密にチェック（推奨）
        if court_count is not None:
            if submitted_results_count < court_count:
                current_app.logger.warning(
                    "⚠️ 未送信コートあり: submitted=%d required=%d match_id=%s",
                    submitted_results_count, court_count, match_id
                )
                if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                    return jsonify({
                        "success": False,
                        "error": f"未送信のコートがあります（{submitted_results_count}/{court_count}）"
                    }), 409
                return f"未送信のコートがあります（{submitted_results_count}/{court_count}）", 409
        else:
            # court_count が無い場合は「結果が0なら拒否」程度の緩い安全策
            # ※本当は meta に court_count を必ず入れる運用にしてください
            if submitted_results_count == 0:
                current_app.logger.warning("⚠️ 結果が0件のため終了できません: match_id=%s", match_id)
                if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                    return jsonify({"success": False, "error": "スコアが未送信のため終了できません"}), 409
                return "スコアが未送信のため終了できません", 409

        current_app.logger.info("🎮 試合結果数(送信済み): %d", submitted_results_count)

        # =========================================================
        # 2) playing プレイヤー一覧（後で transaction に使う）
        # =========================================================
        match_table = current_app.dynamodb.Table("bad-game-match_entries")

        def scan_all_playing(mid: str):
            items = []
            kwargs = {
                "FilterExpression": (
                    Attr("match_id").eq(mid) &
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

        playing_players = scan_all_playing(match_id)

        if len(playing_players) > 24:
            current_app.logger.error("playing_players too many: %d", len(playing_players))
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"success": False, "error": "playingプレイヤー数が多すぎます"}), 500
            return "playingプレイヤー数が多すぎます", 500

        player_mapping = {
            p["user_id"]: p["entry_id"]
            for p in playing_players
            if "user_id" in p and "entry_id" in p
        }

        # =========================================================
        # 3) TrueSkill 更新
        # =========================================================
        updated_skills = {}
        skill_update_count = 0

        for result in match_results:
            try:
                
                # 取得した結果(result)から直接スコアを取り出す
                t1_score = result.get("team1_score")
                t2_score = result.get("team2_score")
                winner = result.get("winner", "A")

                team_a = parse_players(result.get("team_a", []))
                team_b = parse_players(result.get("team_b", []))

                current_app.logger.info(
                    "[finish-debug] court=%s raw_team_a=%s raw_team_b=%s | parsed_len A=%d B=%d",
                    result.get("court_number"),
                    type(result.get("team_a")).__name__,
                    type(result.get("team_b")).__name__,
                    len(team_a), len(team_b),
                )

                skill_map = {
                    str(p["user_id"]): {
                        "skill_score": float(p.get("skill_score", 25.0)),
                        "skill_sigma": float(p.get("skill_sigma", 8.333)),
                    }
                    for p in playing_players if "user_id" in p
                }                

                for pl in team_a + team_b:
                    uid = str(pl.get("user_id"))
                    if uid in skill_map:
                        pl["skill_score"] = skill_map[uid]["skill_score"]
                        pl["skill_sigma"] = skill_map[uid]["skill_sigma"]

                # entry_id 補完
                for pl in team_a + team_b:
                    uid = pl.get("user_id")
                    if uid in player_mapping:
                        pl["entry_id"] = player_mapping[uid]

                current_app.logger.info(
                    "[finish-skill-inject] court=%s | A0_uid=%s A0_mu=%s A0_sig=%s | B0_uid=%s B0_mu=%s B0_sig=%s",
                    result.get("court_number"),
                    (team_a[0].get("user_id") if team_a else None),
                    (team_a[0].get("skill_score") if team_a else None),
                    (team_a[0].get("skill_sigma") if team_a else None),
                    (team_b[0].get("user_id") if team_b else None),
                    (team_b[0].get("skill_score") if team_b else None),
                    (team_b[0].get("skill_sigma") if team_b else None),
                )

                # ★ 修正ポイント: スコアを明示的に result_item に含める
                result_item = {
                    "team_a": team_a,
                    "team_b": team_b,
                    "winner": winner,
                    "match_id": match_id,
                    "court_number": result.get("court_number"),
                    "team1_score": t1_score,  # 追加
                    "team2_score": t2_score   # 追加
                }

                current_app.logger.info("コート%s: %sチーム勝利 (スコア: %s-%s)", 
                                        result.get("court_number"), winner, t1_score, t2_score)

                # これで関数内の .get("team1_score") が正しく値を拾えるようになります
                updated_user_skills = update_trueskill_for_players_and_return_updates(result_item)
                updated_skills.update(updated_user_skills)

                skill_update_count += 1

            except Exception as e:
                current_app.logger.error("スキル更新エラー (court=%s): %s", result.get("court_number"), e)

        current_app.logger.info("スキル更新完了: %d/%dコート", skill_update_count, len(match_results))

        # =========================================================
        # 4) エントリーテーブル同期（スキル値の反映）
        # =========================================================
        sync_count = sync_match_entries_with_updated_skills(player_mapping, updated_skills)
        current_app.logger.info("エントリーテーブル同期完了: %d件", sync_count)

        persist_skill_to_bad_users(updated_skills)

        # =========================================================
        # 5) meta解除 + playing→pending を 1トランザクションで確定
        #    meta(1) + 最大24人 = 25件（上限内）
        # =========================================================
        tx_items = []

        # (a) meta を idle に戻す（status=playing かつ current_match_id 一致）
        # ここで court_count も削除する運用なら REMOVE #cc はOK
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
        # ★注意: REMOVE court_number/team をすると「遅延してきたスコア送信」が復旧不能になる
        # 全送信チェックを入れているので基本は大丈夫ですが、心配なら REMOVE を外すのがより安全です。
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
                        "REMOVE court_number, team, team_side, match_id" 
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
            current_app.logger.info("[meta] unlocked + players pending committed: match_id=%s", match_id)

        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code == "TransactionCanceledException":
                current_app.logger.error(
                    "⚠️ Transaction canceled for match_id=%s: %s",
                    match_id, e.response.get("Error", {}).get("Message")
                )
                if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                    return jsonify({"success": False, "error": "finish transaction canceled"}), 409
                return "finish transaction canceled", 409
            raise

        # =========================================================
        # Ajax / 通常レスポンス
        # =========================================================
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({
                "success": True,
                "message": "試合が正常に終了しました",
                "match_id": match_id,
                "results_count": submitted_results_count,
                "court_count": court_count,
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
        current_app.logger.debug(f"最新の試合ID: {latest_match_id}")
        
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
        match_id = get_latest_match_id()

        match_table = current_app.dynamodb.Table("bad-game-match_entries")
        response = match_table.scan(
            FilterExpression=Attr("match_id").eq(match_id) & Attr("type").ne("meta")
        )
        items = response.get("Items", [])

        court_dict = {}
        for item in items:
            court_no = item.get("court_number")
            team = item.get("team")  # 'A' or 'B'

            # ★テンプレで使いたい情報をここでまとめる
            player = {
                "user_id": item.get("user_id"),
                "display_name": item.get("display_name") or "名前なし",
                "join_count": int(item.get("join_count", 0)),
                "match_count": int(item.get("match_count", 0)),
                "rest_count": int(item.get("rest_count", 0)),
                "skill_score": item.get("skill_score"),  # DecimalのままでもOK
            }

            if court_no not in court_dict:
                court_dict[court_no] = {"team_a": [], "team_b": []}

            if team == "A":
                court_dict[court_no]["team_a"].append(player)
            elif team == "B":
                court_dict[court_no]["team_b"].append(player)

        match_data = []
        for court_no in sorted(court_dict):
            match_data.append({
                "court_number": court_no,
                "team_a": court_dict[court_no]["team_a"],
                "team_b": court_dict[court_no]["team_b"],
            })

        return render_template(
            "game/court.html",
            match_data=match_data,
            selected_max_courts=selected_max_courts
        )

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
        user_id = current_user.get_id()
        current_entry = get_user_current_entry(user_id)

        current_app.logger.info(
            "[leave_court] user_id=%s entry=%s",
            user_id, current_entry
        )

        if not current_entry:
            flash("エントリーが見つかりませんでした", "warning")
            return redirect(url_for("game.court"))

        entry_status = (current_entry.get("entry_status") or "").strip().lower()
        match_id = current_entry.get("match_id")

        # 判定は match_id ではなく entry_status
        if entry_status == "playing":
            flash("試合中のため退出できません", "warning")
            return redirect(url_for("game.court"))

        entry_id = current_entry.get("entry_id")
        if not entry_id:
            flash("退出に失敗しました（entry_id がありません）", "danger")
            return redirect(url_for("game.court"))

        # テーブル参照（あなたの環境に合わせて）
        table = current_app.dynamodb.Table("bad-game-match_entries")

        current_app.logger.info(
            "[leave_court] deleting entry_id=%s status=%s match_id=%s",
            entry_id, entry_status, match_id
        )

        res = table.delete_item(Key={"entry_id": entry_id})

        current_app.logger.info("[leave_court] delete_item response=%s", res)

        flash("コートから退出しました", "info")
        return redirect(url_for("index"))

    except Exception as e:
        current_app.logger.exception(f"退出エラー: {e}")
        flash("退出に失敗しました", "danger")
        return redirect(url_for("game.court"))

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
    current_app.logger.debug(f"最新の試合ID: {latest_match_id}")

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
    """
    submit_score 整理版
    - 成功時のログを最小限に抑え、エラー時の追跡性は維持
    """
    try:
        # ---- 1. 入力値の検証 ----
        team1_raw = request.form.get("team1_score")
        team2_raw = request.form.get("team2_score")
        
        if team1_raw is None or team2_raw is None:
            return "スコアが送信されていません", 400

        try:
            team1_score = int(team1_raw)
            team2_score = int(team2_raw)
        except ValueError:
            return "スコアが数値ではありません", 400

        if team1_score == team2_score:
            return "スコアが同点です。勝者を決めてください。", 400

        winner = "A" if team1_score > team2_score else "B"
        court_number_int = int(court_number)

        # ---- 2. エントリー取得 (Scan) ----
        match_table = current_app.dynamodb.Table("bad-game-match_entries")
        
        resp = match_table.scan(
            FilterExpression=Attr("match_id").eq(str(match_id)) & Attr("court_number").eq(court_number_int)
        )
        entries = resp.get("Items", [])

        if not entries:
            current_app.logger.error("❌ エントリー不在: match=%s, court=%d", match_id, court_number_int)
            return "コートのエントリーが見つかりません", 404

        # ---- 3. チーム分類（正規化は維持） ----
        team_a, team_b = [], []
        for entry in entries:
            # 揺れに対応する取得ロジックは維持
            t_val = entry.get("team") or entry.get("team_side") or entry.get("side") or entry.get("teamName")
            team_norm = str(t_val).strip().upper() if t_val is not None else ""

            player = {
                "user_id": str(entry.get("user_id", "")),
                "display_name": str(entry.get("display_name", "不明")),
                "entry_id": str(entry.get("entry_id", "")),
            }

            if team_norm in ("A", "TEAM_A", "TEAM A"):
                team_a.append(player)
            elif team_norm in ("B", "TEAM_B", "TEAM B"):
                team_b.append(player)

        if not team_a or not team_b:
            current_app.logger.error("❌ チーム不完全: match=%s, court=%d", match_id, court_number_int)
            return "コートのチームデータが不完全です", 404

        # ---- 4. 結果保存 ----
        result_table = current_app.dynamodb.Table("bad-game-results")
        result_item = {
            "result_id": str(uuid.uuid4()),
            "match_id": str(match_id),
            "court_number": court_number_int,
            "team1_score": team1_score,
            "team2_score": team2_score,
            "winner": winner,
            "team_a": team_a,
            "team_b": team_b,
            "created_at": datetime.now(JST).isoformat(),
        }

        result_table.put_item(Item=result_item)
        
        # 成功時はこの1行のみ
        current_app.logger.info("Score: Match=%s, Court=%d, %d-%d (Win:%s)", 
                                 match_id, court_number_int, team1_score, team2_score, winner)

        return "", 200

    except Exception as e:
        current_app.logger.error("[submit_score ERROR] %s", str(e), exc_info=True)
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

        current_app.logger.info("🔄全エントリー削除開始")
        
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
                    current_app.logger.info(f"削除: {item.get('display_name', 'Unknown')} - {item['entry_id']}")
                except Exception as e:
                    current_app.logger.error(f"エントリー削除エラー: {item.get('display_name', 'Unknown')} - {str(e)}")

            last_evaluated_key = response.get('LastEvaluatedKey')
            if not last_evaluated_key:
                break

        # 2. 削除完了後の確認
        time.sleep(0.5)  # DynamoDB の一貫性を待つ
        
        check_response = match_table.scan()
        remaining_items = check_response.get('Items', [])
        
        if remaining_items:
            current_app.logger.warning(f"削除後も残っているエントリー: {len(remaining_items)}件")
        else:
            current_app.logger.info("全エントリー削除完了")

        # 3. メタデータ (進行状況・ローテーション) のリセット
        try:
            meta_table = current_app.dynamodb.Table("bad-game-matches")
            
            # (A) meta#current をリセット (進行中の試合をクリア)
            meta_table.update_item(
                Key={"match_id": "meta#current"},
                UpdateExpression="SET #st = :idle REMOVE current_match_id, court_count",
                ExpressionAttributeNames={"#st": "status"},
                ExpressionAttributeValues={":idle": "idle"},
            )
            current_app.logger.info("[reset] meta#current をリセットしました")

            # (B) rest_queue を削除 (ローテーション順番をリセット) ★重要★
            # これを消すことで、次回のペアリング時に古い順番が引き継がれなくなります
            meta_table.delete_item(Key={"match_id": "rest_queue"})
            current_app.logger.info("[reset] rest_queue を完全に削除しました")

        except Exception as e:
            current_app.logger.error(f"[reset] メタデータリセットエラー: {e}")

        current_app.logger.info(f"[全削除成功] エントリー削除件数: {deleted_count} by {current_user.email}")
        flash(f'全エントリーとローテーション順序をリセットしました', 'success')

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        current_app.logger.error(f"[全削除失敗] {str(e)}")
        current_app.logger.error(f"スタックトレース: {error_trace}")
        flash('リセット処理中にエラーが発生しました', 'danger')

    return redirect(url_for('game.court'))


def get_organized_match_data(match_id):
    """指定試合の 'playing' エントリーだけをコート別に整形して返す"""
    match_table = current_app.dynamodb.Table("bad-game-match_entries")

    players = _scan_all(
        match_table,
        ProjectionExpression=(
            "user_id, display_name, skill_score, entry_status, "
            "court_number, team_side, team, team_name"
        ),
        FilterExpression=Attr("match_id").eq(match_id) & Attr("entry_status").eq("playing"),
        ConsistentRead=True,
    )

    if not players:
        return {}

    # ヘルパー関数群
    def norm_team(item):
        v = item.get("team_side") or item.get("team") or item.get("team_name")
        if v is None: return None
        s = str(v).strip().upper()
        if s in ("A", "TEAM_A", "LEFT"): return "A"
        if s in ("B", "TEAM_B", "RIGHT"): return "B"
        return None

    def to_int_court(v):
        try: return int(str(v))
        except: return 999

    # 並びを安定させる
    players.sort(key=lambda x: (to_int_court(x.get("court_number")), (norm_team(x) or "Z")))

    match_courts = {}
    for item in players:
        court = item.get("court_number")
        team = norm_team(item)

        # 【削除】1人ずつの詳細ログ [item] は削除しました

        if court is None or team not in ("A", "B"):
            continue

        court_num = to_int_court(court)
        court_data = match_courts.setdefault(
            court_num,
            {"court_number": court_num, "team_a": [], "team_b": []}
        )
        (court_data["team_a"] if team == "A" else court_data["team_b"]).append(item)

    # --- 整理されたサマリーログ ---
    # 1試合全体の状況を1行で出す
    summary_list = []
    for c_num, data in sorted(match_courts.items()):
        a_names = ",".join([p.get("display_name", "") for p in data["team_a"]])
        b_names = ",".join([p.get("display_name", "") for p in data["team_b"]])
        summary_list.append(f"C{c_num}:[{a_names} vs {b_names}]")
    
    current_app.logger.info(
        f"試合データ取得: match_id={match_id} | 構成: {' / '.join(summary_list)}"
    )

    return match_courts

@bp_game.route("/api/skill_score")
@login_required
def api_skill_score():
    uid = current_user.get_id()
    user_table = current_app.dynamodb.Table("bad-users")

    k = {"user#user_id": uid}
    resp = user_table.get_item(Key=k, ConsistentRead=True)
    item = resp.get("Item") or {}

    def f(x):
        return float(x) if x is not None else None  

    return jsonify({
        "pk": k["user#user_id"],
        "skill_score": f(item.get("skill_score")),
        "skill_sigma": f(item.get("skill_sigma")),
        "last_participation_date": item.get("last_participation_date"),
        "last_participation_updated_at": item.get("last_participation_updated_at"),
    })


@bp_game.route('/create_test_data')
@login_required
def create_test_data():
    """開発用：テストデータを作成（bad-users のPK=user#user_id に対応版）"""
    from decimal import Decimal
    from datetime import datetime, timezone
    import uuid

    if not current_user.administrator:
        return redirect(url_for('index'))

    test_players = [
        {'display_name': 'テスト太郎', 'skill_score': 40},
        {'display_name': 'テスト花子', 'skill_score': 60},
        {'display_name': 'テスト一郎', 'skill_score': 50},
        {'display_name': 'ノーマン', 'skill_score': 58},
        {'display_name': 'ロバート', 'skill_score': 35},
        {'display_name': 'キャメロン', 'skill_score': 100},
        {'display_name': 'テスト01', 'skill_score': 52},
        {'display_name': 'テスト02', 'skill_score': 48},
        {'display_name': 'テスト03', 'skill_score': 62},
        {'display_name': 'テスト04', 'skill_score': 33},
        {'display_name': 'テスト05', 'skill_score': 57},
        {'display_name': 'テスト06', 'skill_score': 41},
        {'display_name': 'テスト07', 'skill_score': 73},
        {'display_name': 'テスト08', 'skill_score': 38},
    ]

    now = datetime.now(timezone.utc).isoformat()

    match_table = current_app.dynamodb.Table("bad-game-match_entries")
    user_table  = current_app.dynamodb.Table("bad-users")

    for player in test_players:
        user_id  = str(uuid.uuid4())   # 正規と同じ（UUID）
        entry_id = str(uuid.uuid4())

        # 1) match_entries（pending）
        match_table.put_item(Item={
            "entry_id": entry_id,
            "user_id": user_id,
            "display_name": player["display_name"],
            "joined_at": now,
            "created_at": now,
            "match_id": "pending",
            "entry_status": "pending",
            "skill_score": Decimal(str(player.get("skill_score", 50))),
            "skill_sigma": Decimal("8.333"),
            "rest_count": Decimal("0"),
        })

        # 2) bad-users（PK=user#user_id が必須）
        user_table.put_item(Item={
            # ★これが無いと ValidationException
            "user#user_id": f"user#{user_id}",

            # 任意（ただしアプリ内で user_id を参照してるなら残すのが安全）
            "user_id": user_id,

            "display_name": player["display_name"],
            "user_name": f"テスト_{player['display_name']}",
            "email": f"{user_id}@example.com",
            "skill_score": Decimal(str(player.get("skill_score", 50))),
            "skill_sigma": Decimal("8.333"),
            "gender": "unknown",
            "badminton_experience": "テスト",
            "organization": "テスト組織",
            "administrator": False,
            "wins": Decimal("0"),
            "losses": Decimal("0"),
            "match_count": Decimal("0"),
            "created_at": now,
            "updated_at": now,
        })

    return redirect(url_for('game.court'))

@bp_game.route('/clear_test_data')
@login_required
def clear_test_data():
    """開発用：test_user_ の削除 ＋ ローテーションと進行状況の完全リセット"""
    from boto3.dynamodb.conditions import Attr
    
    if not current_user.administrator:
        return redirect(url_for('index'))

    match_table = current_app.dynamodb.Table("bad-game-match_entries") # テーブル定義の不足分を追加
    
    # 1. マッチエントリー（参加者）から削除
    last_evaluated_key = None
    while True:
        scan_kwargs = {'FilterExpression': Attr('user_id').begins_with("test_user_")}
        if last_evaluated_key: scan_kwargs['ExclusiveStartKey'] = last_evaluated_key
        response = match_table.scan(**scan_kwargs)
        for item in response.get('Items', []):
            match_table.delete_item(Key={'entry_id': item['entry_id']})
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key: break
    
    # 2. ユーザーテーブル（スキル等）から削除
    user_table = current_app.dynamodb.Table("bad-users")
    last_evaluated_key = None
    while True:
        scan_kwargs = {
            'FilterExpression': 'begins_with(#uid, :prefix)',
            'ExpressionAttributeNames': {'#uid': 'user_id'},
            'ExpressionAttributeValues': {':prefix': 'test_user_'}
        }
        if last_evaluated_key: scan_kwargs['ExclusiveStartKey'] = last_evaluated_key
        response = user_table.scan(**scan_kwargs)
        for item in response.get('Items', []):
            user_table.delete_item(Key={'user#user_id': item['user#user_id']})
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key: break

    # 3. ★ここが最重要：メタデータ（進行状況とキュー）を消去★
    try:
        meta_table = current_app.dynamodb.Table("bad-game-matches")
        
        # 進行状況を idle にリセット
        meta_table.update_item(
            Key={"match_id": "meta#current"},
            UpdateExpression="SET #st = :idle REMOVE current_match_id, court_count",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={":idle": "idle"},
        )
        
        # 休み順のキューを物理削除（これで次から Generation 1 に戻る）
        meta_table.delete_item(Key={"match_id": "rest_queue"})
        
        current_app.logger.info("[clear_test_data] meta#current reset and rest_queue DELETED.")
    except Exception as e:
        current_app.logger.error(f"[clear_test_data] Meta reset failed: {e}")

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
        now = datetime.now(JST).isoformat()
        
        # 各エントリーをpendingに戻す
        for entry in playing_entries:
            try:
                match_table.update_item(
                    Key={
                        'user_id': entry['user_id'],
                        'joined_at': entry['joined_at']
                    },
                    UpdateExpression="SET entry_status=:pending, updated_at=:now",
                    ExpressionAttributeValues={
                        ':pending': 'pending',
                        ":now": now,
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
    

@bp_game.route('/create_pairings', methods=["POST"])
@login_required
def create_pairings():

    if has_ongoing_matches():
        flash('進行中の試合があるため、新しいペアリングを実行できません。全ての試合のスコア入力を完了してください。', 'warning')
        return redirect(url_for('game.court'))

    try:
        import boto3
        import uuid
        from botocore.exceptions import ClientError
        from boto3.dynamodb.conditions import Attr
        from datetime import datetime
        import random

        max_courts = min(max(int(request.form.get("max_courts", 3)), 1), 6)

        # ★最後に選んだ面数を保存（次回以降のデフォルトになる）
        session["selected_max_courts"] = max_courts

        # =========================================================
        # 0) 「交互に使う」ための状態取得
        #    - meta#pairing に last_mode を保存して試合ごとに交互切り替え
        # =========================================================
        meta_table = current_app.dynamodb.Table("bad-game-matches")

        pairing_meta = meta_table.get_item(
            Key={"match_id": "meta#pairing"},
            ConsistentRead=True
        ).get("Item", {}) or {}

        last_mode = pairing_meta.get("last_mode")  # "random" / "ai"

        # 試合ごとに交互切り替え
        mode = "ai" if last_mode == "random" else "random"

        current_app.logger.info(
            "[pairing-mode] last=%s -> now=%s", last_mode, mode
        )
        # =========================================================
        # 1) pendingエントリー取得 & ユーザーごとに最新だけ残す
        # =========================================================
        entry_table = current_app.dynamodb.Table("bad-game-match_entries")
        response = entry_table.scan(FilterExpression=Attr("entry_status").eq("pending"))

        entries_by_user = {}
        for e in response.get("Items", []):
            uid, joined_at = e["user_id"], e.get("joined_at", "")
            if uid not in entries_by_user or joined_at > entries_by_user[uid].get("joined_at", ""):
                entries_by_user[uid] = e
        entries = list(entries_by_user.values())

        current_app.logger.info(
            "[pairing] pending_entries=%d max_courts=%d",
            len(entries), max_courts
        )

        if len(entries) < 4:
            flash("4人以上のエントリーが必要です。", "warning")
            return redirect(url_for("game.court"))

        # 2) 優先順位（試合少ない→ランダム）
        sorted_entries = sorted(entries, key=lambda e: (
            e.get("match_count", 0),
            random.random()
        ))

        # 3) required_players / waiting_count
        cap_by_courts = min(max_courts * 4, len(sorted_entries))
        required_players = cap_by_courts - (cap_by_courts % 4)
        waiting_count = len(sorted_entries) - required_players

        current_app.logger.info(
            "[pairing] cap_by_courts=%d required_players=%d waiting_count=%d",
            cap_by_courts, required_players, waiting_count
        )

        # 4) 待機者選出（キュー方式）
        # ここで「絶対休む人」と「絶対試合に出る人」を物理的に切り分ける
        if waiting_count > 0:
            active_entries, waiting_entries = _select_waiting_entries(sorted_entries, waiting_count)
        else:
            active_entries, waiting_entries = sorted_entries, []
            current_app.logger.info("[wait] waiting_count=0 (none)")

        # ★修正ポイント：AIに渡すのは active_entries (12人) だけに限定する
        # (以前はここで players に全員分追加していたのが原因でした)

        # 5) Player変換 (active_entries = 試合に出る人 12人)
        players = []
        for e in active_entries:
            skill_score = float(e.get("skill_score", 50.0))
            skill_sigma = float(e.get("skill_sigma", 8.333))
            # conservative の計算
            conservative_val = skill_score - 3 * skill_sigma
            
            # 引数の順番は Player クラスの定義に合わせてください
            p = Player(e["display_name"], skill_score, e.get("gender", "M"))
            
            # ★ここで明示的に属性をセットする
            p.user_id = e.get("user_id")
            p.entry_id = e.get("entry_id")
            p.conservative = conservative_val  # ← これがAIモードに必須
            p.skill_score = skill_score
            p.skill_sigma = skill_sigma
            players.append(p)

        # 待機確定組も同様に修正
        waiting_players = []
        for e in waiting_entries:
            skill_score = float(e.get("skill_score", 50.0))
            skill_sigma = float(e.get("skill_sigma", 8.333))
            conservative_val = skill_score - 3 * skill_sigma
            
            p = Player(e["display_name"], skill_score, e.get("gender", "M"))
            p.user_id = e.get("user_id")
            p.entry_id = e.get("entry_id")
            p.conservative = conservative_val
            waiting_players.append(p)

       # 6) ペア生成
        match_id = generate_match_id()

        if mode == "ai":
            # AIモードは通常 2つ の戻り値
            matches, additional_waiting_players = generate_ai_best_pairings(players, max_courts, iterations=1000)
        else:
            # Randomモードは 3つ の戻り値（pairs が最初に入る）
            # ここで ValueError: too many values to unpack が起きていました
            pairs, matches, additional_waiting_players = generate_balanced_pairs_and_matches(players, max_courts)

        current_app.logger.info(
            "[matches] match_id=%s courts=%d additional_waiting=%d mode=%s",
            match_id, len(matches), len(additional_waiting_players), mode
        )

        for i, ((a1, a2), (b1, b2)) in enumerate(matches, 1):
            current_app.logger.info(
                "[match] C%d: A=[%s,%s] vs B=[%s,%s]",
                i, a1.name, a2.name, b1.name, b2.name
            )

        def _team_strength(p1, p2):
            return float(getattr(p1, "conservative", 0.0)) + float(getattr(p2, "conservative", 0.0))

        diffs = []
        for i, ((a1, a2), (b1, b2)) in enumerate(matches, 1):
            sa = _team_strength(a1, a2)
            sb = _team_strength(b1, b2)
            d = abs(sa - sb)
            diffs.append(d)
            current_app.logger.info(
                "[eval] C%d A_strength=%.2f B_strength=%.2f diff=%.2f",
                i, sa, sb, d
            )

        if diffs:
            current_app.logger.info(
                "[eval] diff_avg=%.2f diff_max=%.2f",
                sum(diffs) / len(diffs), max(diffs)
            )        

        if additional_waiting_players:
            current_app.logger.info(
                "[wait] additional_waiting(from_unused_pairs)=%s",
                ", ".join([p.name for p in additional_waiting_players])
            )

        waiting_players.extend(additional_waiting_players)

        if not matches:
            flash("試合を作成できませんでした（人数不足など）。", "warning")
            return redirect(url_for("game.court"))

        current_app.logger.info(
            "[wait] final_waiting_names=%s",
            ", ".join([p.name for p in waiting_players]) or "(none)"
        )
    # =========================================================
   
        # -------------------------------------------------
        # TransactWriteItems：metaロック + 試合参加者更新
        #   最大 25件制限：meta(1) + 4*len(matches)
        # -------------------------------------------------
        max_tx = 25
        need_tx = 1 + 4 * len(matches)
        if need_tx > max_tx:
            current_app.logger.error("[meta] tx items exceed limit: need=%s", need_tx)
            flash("試合数が多すぎて作成できませんでした。コート数を減らしてください。", "danger")
            return redirect(url_for("game.court"))

        now_jst = datetime.now(JST).isoformat()
        dynamodb_client = boto3.client('dynamodb', region_name='ap-northeast-1')

        tx_items = []

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
                entry_id = str(getattr(pl, "entry_id", "") or "")
                if not entry_id:
                    raise RuntimeError(f"entry_id missing for player name={pl.name} uid={getattr(pl,'user_id',None)}")
                
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
                "[meta] lock+players committed: current_match_id=%s court_count=%s",
                match_id, len(matches)
            )
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "TransactionCanceledException":
                current_app.logger.warning("[meta] lock tx canceled: %s", e)
                flash("進行中の試合があるためペアリングできませんでした。", "warning")
                return redirect(url_for("game.court"))
            raise

        # =========================================================
        # ★交互モードの状態を保存（成功後だけ）
        # =========================================================
        meta_table.update_item(
            Key={"match_id": "meta#pairing"},
            UpdateExpression="SET last_mode=:m, last_match_id=:mid, updated_at=:now",
            ExpressionAttributeValues={
                ":m": mode,
                ":mid": str(match_id),
                ":now": now_jst,
            }
        )

        # ==============================
        # ★ここ：休み「累計」更新 + 休み「イベント」追加
        #   ※ user_id は「uid未定義」にならないように必ず後で入れる
        # ==============================
        if waiting_players:
            for wp in waiting_players:
                entry_id = str(getattr(wp, "entry_id", "") or "")
                if not entry_id:
                    continue

                # (1) 既存レコードに「休み累計/最新」だけ追記
                entry_table.update_item(
                    Key={"entry_id": entry_id},
                    UpdateExpression=(
                        "SET last_rest_match_id=:mid, last_rest_at=:now, last_rest_reason=:rr, updated_at=:now "
                        "ADD rest_count :one"
                    ),
                    ExpressionAttributeValues={
                        ":mid": str(match_id),
                        ":now": now_jst,
                        ":rr": "not_selected",
                        ":one": 1,
                    },
                )

                # (2) 休みイベントを別レコードとして追加
                rest_event_id = str(uuid.uuid4())
                rest_item = {
                    "entry_id": rest_event_id,
                    "type": "rest_event",
                    "match_id": str(match_id),
                    "display_name": wp.name,
                    "entry_status": "resting",
                    "reason": "not_selected",
                    "court_number": 0,
                    "team": "R",
                    "created_at": now_jst,
                    "updated_at": now_jst,
                    "source_entry_id": entry_id,
                }

                uid = getattr(wp, "user_id", None)
                if uid:  # None/空文字なら入れない（NULLを書かない）
                    rest_item["user_id"] = str(uid)

                entry_table.put_item(Item=rest_item)

        current_app.logger.info("ペアリング成功: %s試合, %s人待機 (mode=%s)", len(matches), len(waiting_players), mode)
        return redirect(url_for("game.court"))

    except Exception as e:
        current_app.logger.error("[ペア生成エラー] %s", str(e), exc_info=True)
        flash("試合の作成中にエラーが発生しました。", "danger")
        return redirect(url_for("game.court"))
    
    
# 待機者選出ロジック（キュー方式）
def _select_waiting_entries(sorted_entries: list, waiting_count: int) -> tuple[list, list]:
    ...
    active_entries, waiting_entries, meta = _pick_waiters_by_rest_queue(
        entries=sorted_entries,
        waiting_count=waiting_count,
    )

    # ★追加：UID重複チェック
    w_uids = [e.get("user_id") for e in waiting_entries]
    dup = [uid for uid, c in Counter(w_uids).items() if uid and c > 1]

    current_app.logger.info(
        "[rest_queue] gen=%s ver=%s waiting_count=%s waiting_len=%s dup_uids=%s waiting=%s queue_remaining=%s",
        meta.get("generation"),
        meta.get("version"),
        waiting_count,
        len(waiting_entries),
        dup,
        ", ".join([e.get("display_name", "?") for e in waiting_entries]),
        meta.get("queue_remaining"),
    )
    return active_entries, waiting_entries


def _pick_waiters_by_rest_queue(entries, waiting_count, *, queue_key="rest_queue"):
    meta_table = current_app.dynamodb.Table("bad-game-matches")
    current_user_ids = [e["user_id"] for e in entries]
    by_id = {e["user_id"]: e for e in entries}

    resp = meta_table.get_item(Key={"match_id": queue_key}, ConsistentRead=True)
    qi = resp.get("Item") or {}
    
    queue = list(qi.get("queue", []))
    generation = int(qi.get("generation", 1))
    version = int(qi.get("version", 0))

    # 1. 退出者除外
    queue = [uid for uid in queue if uid in set(current_user_ids)]

    # 2. キューが空、または足りない場合の処理
    if len(queue) < waiting_count:
        generation += 1
        new_cycle = list(current_user_ids)
        random.shuffle(new_cycle)
        current_app.logger.info("[rest_queue] --- Gen %d: Queue refilled with new shuffle ---", generation)

        waiting_pick = new_cycle[:waiting_count]
        queue_next   = new_cycle[waiting_count:]
    else:
        waiting_pick = queue[:waiting_count]
        queue_next   = queue[waiting_count:]
        current_app.logger.info("[rest_queue] Rotating. Remaining in queue: %d", len(queue_next))

    # --- あとは queue_next を保存するだけ ---

    # 5. DynamoDBへ保存（put_itemで確実に上書き）
    try:
        meta_table.put_item(
            Item={
                "match_id": queue_key,
                "queue": queue_next,
                "generation": generation,
                "version": version + 1,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        
        current_app.logger.info(
            "[rest_queue][ROTATE] Gen %d, Waiters: %s",
            generation, ", ".join([by_id[uid]["display_name"] for uid in waiting_pick if uid in by_id])
        )

        return [e for e in entries if e["user_id"] not in set(waiting_pick)], \
               [by_id[uid] for uid in waiting_pick if uid in by_id], \
               {"generation": generation, "version": version + 1}

    except Exception as e:
        current_app.logger.error("[rest_queue] Save failed: %s", e)
        return entries, [], {"error": True}
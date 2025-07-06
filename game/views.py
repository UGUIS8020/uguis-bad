from flask import Blueprint, render_template, redirect, url_for, flash, request, current_app
from flask_login import login_required, current_user
import boto3
import uuid
from datetime import datetime, date, time
import random
from boto3.dynamodb.conditions import Key, Attr, And
from flask import jsonify
from flask import session
from .game_utils import update_trueskill_for_players, parse_players, BadmintonPairing, Player, generate_balanced_pairs_and_matches
import pytz
import re



bp_game = Blueprint('game', __name__)

# DynamoDBリソース取得
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
match_table = dynamodb.Table('bad-game-match_entries')
game_meta_table = dynamodb.Table('bad-game-matches')
user_table = dynamodb.Table("bad-users")
    
@bp_game.route("/court")
@login_required
def court():
    try:
        current_app.logger.info("=== コート入場開始 ===")

        # ✅ 参加希望のプレイヤーだけ取得（休憩中など除外）
        match_table = current_app.dynamodb.Table("bad-game-match_entries")
        response = match_table.scan(
            FilterExpression=Attr("entry_status").eq("pending") | Attr("entry_status").eq("resting"),
            ConsistentRead=True
        )
        items = response.get("Items", [])
        current_app.logger.info(f"📊 参加待ちプレイヤー数: {len(items)}")
        
        # デフォルト値を設定
        for item in items:
            if 'rest_count' not in item or item['rest_count'] is None:
                item['rest_count'] = 0
            if 'match_count' not in item or item['match_count'] is None:
                item['match_count'] = 0
            if 'join_count' not in item or item['join_count'] is None:
                item['join_count'] = 0
        
        # ステータス別に分類
        pending_players = [item for item in items if item.get('entry_status') == 'pending']
        resting_players = [item for item in items if item.get('entry_status') == 'resting']
        playing_players = [item for item in items if item.get('entry_status') == 'playing']
        
        current_app.logger.info(f"📊 参加待ちプレイヤー数: {len(pending_players)}")
        current_app.logger.info(f"📊 休憩中プレイヤー数: {len(resting_players)}")
        current_app.logger.info(f"📊 試合中プレイヤー数: {len(playing_players)}")
        
        # ユーザー状態の判定
        user_id = current_user.get_id()
        is_registered = any(p['user_id'] == user_id for p in pending_players)
        is_resting = any(p['user_id'] == user_id for p in resting_players)
        
        # スキルスコアの取得
        user_entries = [p for p in items if p['user_id'] == user_id]
        skill_score = user_entries[0]['skill_score'] if user_entries else 50
        
        # 試合回数の取得
        match_count = user_entries[0].get('match_count', 0) if user_entries else 0
        
        # 試合情報の取得
        match_id = get_latest_match_id()
        current_app.logger.info(f"🔍 取得したmatch_id: {match_id}")
        
        if match_id:
            # get_match_players_by_court関数の代わりに共通関数を使用
            match_courts = get_organized_match_data(match_id)
            current_app.logger.info(f"🔍 match_courts取得結果: {match_courts}")
            current_app.logger.info(f"🔍 match_courtsのキー数: {len(match_courts)}")
        else:
            match_courts = {}
            current_app.logger.warning("⚠️ match_idが取得できませんでした")
        
        return render_template(
            'game/court.html',
            pending_players=pending_players,
            resting_players=resting_players,
            is_registered=is_registered,
            is_resting=is_resting,
            current_user_skill_score=skill_score,
            current_user_match_count=match_count,
            match_courts=match_courts,
            match_id=match_id
        )
        
    except Exception as e:
        current_app.logger.error(f"コート入場エラー詳細: {str(e)}")
        import traceback
        current_app.logger.error(f"スタックトレース: {traceback.format_exc()}")
        return f"エラー: {e}"

    
def get_latest_match_id():
    """最新の試合IDを取得"""
    try:
        today_prefix = datetime.now().strftime("%Y%m%d")
        current_app.logger.info(f"🔍 検索する今日のプレフィックス: {today_prefix}")
        
        match_table = current_app.dynamodb.Table("bad-game-match_entries")
        
        # まず全てのアイテムを確認
        all_response = match_table.scan()
        all_items = all_response.get("Items", [])
        current_app.logger.info(f"🔍 全アイテム数: {len(all_items)}")
        
        # match_idを持つアイテムを確認
        items_with_match_id = [item for item in all_items if item.get("match_id") and item.get("match_id") != "pending"]
        current_app.logger.info(f"🔍 有効なmatch_idを持つアイテム数: {len(items_with_match_id)}")
        
        for item in items_with_match_id[:5]:  # 最初の5件
            current_app.logger.info(f"🔍 match_id={item.get('match_id')}, name={item.get('display_name')}, status={item.get('entry_status')}")
        
        # 今日のプレフィックスでフィルタリング
        response = match_table.scan(
            FilterExpression=Attr("match_id").begins_with(today_prefix)
        )
        items = response.get("Items", [])
        
        current_app.logger.info(f"🔍 今日のmatch_idを持つアイテム数: {len(items)}")
        
        if not items:
            current_app.logger.info("✅ 今日の試合はまだ登録されていません。")
            return None
        
        latest = max(items, key=lambda x: x.get("match_id", ""))
        match_id = latest.get("match_id")
        
        current_app.logger.info(f"🎯 最新の試合ID: {match_id}")
        return match_id
        
    except Exception as e:
        current_app.logger.error(f"❌ match_id取得エラー: {e}")
        return None

def get_match_players_by_court(match_id):
    """指定された試合IDに対するコート別のプレイヤー構成を取得"""
    match_table = current_app.dynamodb.Table("bad-game-match_entries")
    
    current_app.logger.info(f"🔍 試合情報取得開始: match_id={match_id}")
    
    response = match_table.scan(
        FilterExpression=Attr("match_id").eq(match_id) & Attr("entry_status").eq("playing")
    )
    players = response.get("Items", [])
    
    current_app.logger.info(f"🔍 試合プレイヤー取得: {len(players)}人")
    
    # 実際のデータ構造を確認
    for i, p in enumerate(players):
        current_app.logger.info(f"🔍 プレイヤー{i+1}の全フィールド: {p}")
        current_app.logger.info(f"🔍 利用可能なキー: {list(p.keys())}")
    
    courts = {}

    for p in players:
        # court の取得
        court_raw = p.get("court_number")  # データベースでは court_number を使用
        if not court_raw:
            current_app.logger.warning(f"⚠️ court_numberが見つかりません: {p}")
            continue
        
        try:
            court_num = int(court_raw)
        except (ValueError, TypeError):
            current_app.logger.warning(f"⚠️ 無効なコート番号: {court_raw}")
            continue

        # team の取得（フィールド名を確認）
        team_raw = p.get("team") or p.get("team_name") or p.get("team_side")
        if not team_raw:
            current_app.logger.warning(f"⚠️ チーム情報が見つかりません: {p}")
            # チーム情報がない場合の対処法を検討
            continue
        
        # プレイヤー情報を整形
        player_info = {
            "user_id": p.get("user_id"),
            "display_name": p.get("display_name", "匿名"),
            "skill_score": int(p.get("skill_score", 0)),
            "gender": p.get("gender", "unknown"),
            "organization": p.get("organization", ""),
            "badminton_experience": p.get("badminton_experience", "")
        }

        # court 番号に基づいて分類
        if court_num not in courts:
            courts[court_num] = {
                "court_number": court_num,
                "team_a": [],
                "team_b": [],
            }

        # チーム分けのロジックを修正
        # team情報がない場合は、プレイヤーの順番で分ける
        if len(courts[court_num]["team_a"]) <= len(courts[court_num]["team_b"]):
            courts[court_num]["team_a"].append(player_info)
        else:
            courts[court_num]["team_b"].append(player_info)

    current_app.logger.info(f"🔍 構築されたコート情報: {len(courts)}面")
    for court_num, court_info in courts.items():
        current_app.logger.info(f"🔍 コート{court_num}: チームA={len(court_info['team_a'])}人, チームB={len(court_info['team_b'])}人")
    
    return courts      
    
def get_latest_match_id():
    """最新の試合IDを取得"""
    try:
        today_prefix = datetime.now().strftime("%Y%m%d")
        current_app.logger.info(f"🔍 検索する今日のプレフィックス: {today_prefix}")
        
        # 🔥 同じテーブルから検索
        match_table = current_app.dynamodb.Table("bad-game-match_entries")
        
        response = match_table.scan(
            FilterExpression=Attr("match_id").begins_with(today_prefix)
        )
        items = response.get("Items", [])
        
        current_app.logger.info(f"🔍 今日のmatch_idを持つアイテム数: {len(items)}")
        
        if not items:
            current_app.logger.info("✅ 今日の試合はまだ登録されていません。")
            return None
        
        # ユニークなmatch_idを抽出
        unique_match_ids = set()
        for item in items:
            match_id = item.get("match_id")
            if match_id and match_id != "pending":
                unique_match_ids.add(match_id)
        
        current_app.logger.info(f"🔍 ユニークなmatch_id: {list(unique_match_ids)}")
        
        if not unique_match_ids:
            return None
        
        # 最新のmatch_idを返す
        latest_match_id = max(unique_match_ids)
        current_app.logger.info(f"🎯 最新の試合ID: {latest_match_id}")
        
        return latest_match_id
        
    except Exception as e:
        current_app.logger.error(f"❌ match_id取得エラー: {e}")
        return None

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
def get_players_status(status, user_id=None):
    try:
        match_table = current_app.dynamodb.Table("bad-game-match_entries")
        
        # デバッグ: 全データを取得して確認
        all_response = match_table.scan()
        all_items = all_response.get("Items", [])
        current_app.logger.info(f"🔍 全データ取得: {len(all_items)}件")
        
        for item in all_items:
            current_app.logger.info(f"🔍 全データ: {item.get('display_name')} - status: {item.get('entry_status')} - user_id: {item.get('user_id')}")
        
        # 指定されたステータスのデータを取得
        if user_id:
            response = match_table.scan(
                FilterExpression=Attr("entry_status").eq(status) & Attr("user_id").eq(user_id)
            )
        else:
            response = match_table.scan(
                FilterExpression=Attr("entry_status").eq(status)
            )
        
        items = response.get("Items", [])
        current_app.logger.info(f"🔍 [{status.upper()}] フィルタ後: {len(items)}件")
        
        # フィルタ結果の詳細ログ
        for item in items:
            current_app.logger.info(f"🔍 [{status}] データ: {item.get('display_name')} - entry_status: {item.get('entry_status')}")
        
        # デフォルト値の設定
        for item in items:
            # rest_count が存在しない場合は 0 を設定
            if 'rest_count' not in item or item['rest_count'] is None:
                item['rest_count'] = 0
                current_app.logger.info(f"🔧 {item.get('display_name')} の rest_count を 0 に設定")
            
            # 他のフィールドのデフォルト値も設定
            if 'match_count' not in item or item['match_count'] is None:
                item['match_count'] = 0
            
            if 'join_count' not in item or item['join_count'] is None:
                item['join_count'] = 0
        
        return items
        
    except Exception as e:
        current_app.logger.error(f"🚨 {status}プレイヤー取得エラー: {str(e)}")
        import traceback
        current_app.logger.error(f"🚨 スタックトレース: {traceback.format_exc()}")
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
    """参加待ちプレイヤーを取得"""
    try:
        today = date.today().isoformat()
        history_table = current_app.dynamodb.Table("bad-users-history")
        response = match_table.scan(
            FilterExpression=Attr('match_id').eq('pending') & Attr('entry_status').eq('pending')
        )

        players = []
        for item in response.get('Items', []):
            user_id = item['user_id']

            # ユーザー詳細情報を取得
            user_response = user_table.get_item(Key={"user#user_id": user_id})
            user_data = user_response.get("Item", {})

            # 🟢 履歴から参加回数を取得
            try:
                history_response = history_table.scan(
                    FilterExpression=Attr('user_id').eq(user_id)
                )
                history_items = history_response.get('Items', [])
                join_count = sum(1 for h in history_items if h.get('date') and h['date'] < today)
            except Exception as e:
                current_app.logger.warning(f"[履歴取得エラー] user_id={user_id}: {str(e)}")
                join_count = 0

            player_info = {
                'entry_id': item['entry_id'],
                'user_id': user_id,
                'display_name': item.get('display_name', user_data.get('display_name', '不明')),
                'skill_score': item.get('skill_score', user_data.get('skill_score', 50)),
                'badminton_experience': user_data.get('badminton_experience', '未設定'),
                'joined_at': item.get('joined_at'),
                'join_count': join_count  # 🔽 参加回数を追加
            }
            players.append(player_info)

        # 参加時刻でソート
        players.sort(key=lambda x: x.get('joined_at', ''))

        current_app.logger.info(f"[PENDING PLAYERS] 表示件数: {len(players)}")
        for p in players:
            current_app.logger.info(f"  - {p['display_name']}（{p['skill_score']}点）参加時刻: {p['joined_at']}")

        return players

    except Exception as e:
        current_app.logger.error(f"参加待ちプレイヤー取得エラー: {str(e)}")
        return []
    

def get_resting_players():
    """休憩中プレイヤーを取得"""
    try:
        today = date.today().isoformat()
        history_table = current_app.dynamodb.Table("bad-users-history")

        response = match_table.scan(
            FilterExpression=Attr('entry_status').eq('resting')
        )

        players = []
        for item in response.get('Items', []):
            user_id = item['user_id']

            # ユーザー詳細情報を取得
            user_response = user_table.get_item(Key={"user#user_id": user_id})
            user_data = user_response.get("Item", {})

            # 🔽 履歴から参加回数を取得
            try:
                history_response = history_table.scan(
                    FilterExpression=Attr('user_id').eq(user_id)
                )
                history_items = history_response.get('Items', [])
                join_count = sum(1 for h in history_items if h.get('date') and h['date'] < today)
            except Exception as e:
                current_app.logger.warning(f"[履歴取得エラー] user_id={user_id}: {str(e)}")
                join_count = 0

            player_info = {
                'entry_id': item['entry_id'],
                'user_id': user_id,
                'display_name': item.get('display_name', user_data.get('display_name', '不明')),
                'skill_score': item.get('skill_score', user_data.get('skill_score', 50)),
                'badminton_experience': user_data.get('badminton_experience', '未設定'),
                'joined_at': item.get('joined_at'),
                'join_count': join_count,  # ✅ 追加
                'is_current_user': user_id == current_user.get_id()  # ✅ 追加
            }
            players.append(player_info)

        return players

    except Exception as e:
        current_app.logger.error(f"休憩中プレイヤー取得エラー: {str(e)}")
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
    now = datetime.now().isoformat()
    current_app.logger.info(f"[ENTRY] 参加登録開始: {user_id}")

    # すでにpending登録されていないかチェック
    response = match_table.scan(
        FilterExpression=Attr("user_id").eq(user_id) & Attr("match_id").eq("pending")
    )
    existing = response.get("Items", [])

    if existing:
        current_app.logger.info("[ENTRY] すでに参加登録済みのためスキップ")
        flash("すでに参加登録されています", "info")
        return redirect(url_for("game.court"))

    # 他の状態（restingなど）があれば削除
    cleanup_response = match_table.scan(
        FilterExpression=Attr("user_id").eq(user_id) & Attr("match_id").is_in(["resting", "active"])
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
            "match_id": "pending",          # NoneまたはDBの制約に合わせて""などを使用
            "entry_status": "pending",  # 状態を示すフィールドはこちらを使用
            # "status": "pending",        # statusフィールドも設定
            "display_name": display_name,
            "skill_score": skill_score,
            "joined_at": now,
            "created_at": now,
            "rest_count": 0,      # 休憩回数を初期化
            "match_count": 0,     # 試合回数を初期化
        }
    match_table.put_item(Item=entry_item)
    current_app.logger.info(f"[ENTRY] 新規参加登録完了: {entry_item['entry_id']}")
    flash("試合への参加登録が完了しました", "success")

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
        current_app.logger.error(f"❌ rest_count 更新失敗: {e}")


def update_player_for_match(entry_id, match_id, court_number, team_side):
    """プレイヤーを試合用に更新（match_countもインクリメント）"""
    table = current_app.dynamodb.Table("bad-game-match_entries")
    try:
        # 🔍 更新前の確認
        current_app.logger.info(f"🔄 更新開始: entry_id={entry_id}, match_id={match_id}, court={court_number}, team={team_side}")
        
        # 🔍 更新前の状態を確認
        response = table.get_item(Key={"entry_id": entry_id})
        before_item = response.get("Item", {})
        current_app.logger.info(f"🔍 更新前: status={before_item.get('entry_status')}, match_id={before_item.get('match_id')}")
        
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
        
        # 🔍 更新後の確認
        response = table.get_item(Key={"entry_id": entry_id})
        after_item = response.get("Item", {})
        current_app.logger.info(f"🔍 更新後: status={after_item.get('entry_status')}, match_id={after_item.get('match_id')}, court={after_item.get('court_number')}, team={after_item.get('team_side')}")
        
        current_app.logger.info(f"✅ プレイヤー更新: entry_id={entry_id}, コート{court_number}, チーム{team_side}")
        
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
        current_app.logger.info(f"✅ 休憩プレイヤー更新: entry_id={entry_id}")
    except Exception as e:
        current_app.logger.error(f"❌ 休憩プレイヤー更新エラー: {e}")

@bp_game.route('/create_pairings', methods=["POST"])
@login_required
def create_pairings():
    try:
        max_courts = min(max(int(request.form.get("max_courts", 3)), 1), 6)        

        # 1. pendingエントリー取得 & ユーザーごとに最新だけ残す
        match_table = current_app.dynamodb.Table("bad-game-match_entries")
        response = match_table.scan(FilterExpression=Attr("entry_status").eq("pending"))
        entries_by_user = {}
        for e in response.get("Items", []):
            uid, joined_at = e["user_id"], e.get("joined_at", "")
            if uid not in entries_by_user or joined_at > entries_by_user[uid].get("joined_at", ""):
                entries_by_user[uid] = e
        entries = list(entries_by_user.values())

        if len(entries) < 4:
            flash("4人以上のエントリーが必要です。", "warning")
            return redirect(url_for("game.court"))

        # ✅ 2. 完全シャッフル（偏り解消）
        random.shuffle(entries)

        # 3. Player変換
        name_to_id, players = {}, []
        for e in entries:
            name = e["display_name"]
            p = Player(name, int(e.get("skill_score", 50)), e.get("gender", "M"))
            p.match_count = e.get("match_count", 0)
            p.rest_count = e.get("rest_count", 0)
            name_to_id[name] = e["entry_id"]
            players.append(p)

        # 4. ペア生成 & マッチ生成
        match_id = generate_match_id()
        pairs, matches, waiting_players = generate_balanced_pairs_and_matches(players, max_courts)     

        # 5. 試合参加プレイヤー更新
        used_names = {p.name for match in matches for team in match for p in team}
        for court_num, ((a1, a2), (b1, b2)) in enumerate(matches, 1):
            for name, team in [(a1.name, "A"), (a2.name, "A"), (b1.name, "B"), (b2.name, "B")]:
                update_player_for_match(name_to_id[name], match_id, court_num, team)

        for p in waiting_players:
            entry_id = name_to_id.get(p.name)
            if entry_id:
                increment_rest_count(entry_id)

        # 6. 待機プレイヤー表示（更新なし）
        pending_names = [p.name for p in waiting_players]
        if pending_names:
            flash(f"{len(matches)}件の試合を作成しました。参加待ち: {', '.join(pending_names)}", "success")
        else:
            flash(f"{len(matches)}件の試合を作成しました。", "success")

        return redirect(url_for("game.court"))

    except Exception as e:
        current_app.logger.error(f"[ペア生成エラー] {str(e)}", exc_info=True)
        flash("試合の作成中にエラーが発生しました。", "danger")
        return redirect(url_for("game.court"))


# def update_players_to_playing(matches, match_id, match_table):
#     """選ばれた人を"playing"に更新する関数"""
    
#     for match in matches:
#         try:
#             # データ構造の検証
#             if not isinstance(match, dict):
#                 current_app.logger.error(f"❌ match is not dict, got {type(match)}: {match}")
#                 continue
            
#             # コートデータの取得（新旧両方の構造に対応）
#             courts_data = match.get("courts", match)
            
#             for court_num, court_data in courts_data.items():
#                 # court_dataが辞書であることを確認
#                 if not isinstance(court_data, dict):
#                     current_app.logger.error(f"❌ court_data for {court_num} is not dict, got {type(court_data)}: {court_data}")
#                     continue
                
#                 for team_key in ["team_a", "team_b"]:
#                     players = court_data.get(team_key, [])
                    
#                     if not isinstance(players, list):
#                         current_app.logger.error(f"❌ players for {court_num}-{team_key} is not list: {type(players)}")
#                         continue
                    
#                     for player in players:
#                         if not isinstance(player, dict) or "entry_id" not in player:
#                             current_app.logger.error(f"❌ Invalid player data: {player}")
#                             continue
                        
#                         # DynamoDB更新
#                         match_table.update_item(
#                             Key={"entry_id": player["entry_id"]},
#                             UpdateExpression=(
#                                 "SET entry_status = :s, match_id = :m, court = :c, team = :t, "
#                                 "match_count = if_not_exists(match_count, :zero) + :one"
#                             ),
#                             ExpressionAttributeValues={
#                                 ":s": "playing",
#                                 ":m": match_id,
#                                 ":c": str(court_num),
#                                 ":t": team_key,
#                                 ":zero": 0,
#                                 ":one": 1
#                             }
#                         )
#                         current_app.logger.info(f"✅ Updated player {player.get('display_name', 'Unknown')} to playing on {court_num}-{team_key}")
                        
#         except Exception as e:
#             current_app.logger.error(f"❌ Error updating players in match: {e}")
#             continue

def update_players_to_playing(matches, match_id, match_table):
    """選ばれた人を"playing"に更新する関数"""
    
    current_app.logger.info(f"🟢 [START] update_players_to_playing - match_id: {match_id}")
    
    for match_idx, match in enumerate(matches):
        try:
            current_app.logger.info(f"🔍 処理中の match[{match_idx}]: {match}")

            if not isinstance(match, dict):
                current_app.logger.error(f"❌ match[{match_idx}] は dict ではありません: {type(match)}")
                continue
            
            courts_data = match.get("courts", match)
            current_app.logger.info(f"📦 使用する courts_data: {list(courts_data.keys())}")

            for court_num, court_data in courts_data.items():
                if not isinstance(court_data, dict):
                    current_app.logger.error(f"❌ court_data[{court_num}] が dict ではありません: {type(court_data)}")
                    continue
                
                for team_key in ["team_a", "team_b"]:
                    players = court_data.get(team_key, [])
                    
                    if not isinstance(players, list):
                        current_app.logger.error(f"❌ players[{court_num}][{team_key}] が list ではありません: {type(players)}")
                        continue
                    
                    current_app.logger.info(f"🧩 court={court_num}, team={team_key}, players={len(players)}人")

                    for player in players:
                        if not isinstance(player, dict) or "entry_id" not in player:
                            current_app.logger.error(f"❌ 無効なプレイヤーデータ: {player}")
                            continue
                        
                        entry_id = player["entry_id"]
                        display_name = player.get("display_name", "Unknown")
                        user_id = player.get("user_id", "N/A")

                        current_app.logger.info(f"↪️ DynamoDB更新開始: {display_name} (entry_id={entry_id})")

                        # DynamoDB 更新処理
                        result = match_table.update_item(
                            Key={"entry_id": entry_id},
                            UpdateExpression=(
                                "SET entry_status = :s, match_id = :m, court = :c, team = :t, "
                                "match_count = if_not_exists(match_count, :zero) + :one"
                            ),
                            ExpressionAttributeValues={
                                ":s": "playing",
                                ":m": match_id,
                                ":c": court_num,
                                ":t": team_key,
                                ":zero": 0,
                                ":one": 1
                            },
                            ReturnValues="UPDATED_NEW"
                        )

                        updated_attrs = result.get("Attributes", {})
                        current_app.logger.info(
                            f"✅ 更新完了: {display_name} (user_id={user_id}, entry_id={entry_id}) "
                            f"→ court={court_num}, team={team_key}, 更新後: {updated_attrs}"
                        )
        except Exception as e:
            current_app.logger.error(f"❌ 例外発生（match[{match_idx}]）: {e}")
            import traceback
            current_app.logger.error(traceback.format_exc())
            continue

    current_app.logger.info(f"✅ [END] update_players_to_playing - match_id: {match_id}")


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
    
    current_app.logger.info(f"🔍 ペアリング開始: 総エントリー数={len(entries)}, 最大コート数={max_courts}")
    current_app.logger.info(f"🔍 使用する試合ID: {match_id}")
    
    random.shuffle(entries)
    
    # 4人ずつのグループを作成
    for i in range(0, len(entries), 4):
        if court_number > max_courts:
            remaining_players = entries[i:]
            current_app.logger.info(f"🔍 コート数超過 - 残り{len(remaining_players)}人は休憩")
            rest.extend(remaining_players)
            break
        
        group = entries[i:i + 4]
        current_app.logger.info(f"🔍 グループ{court_number}: {len(group)}人")
        
        if len(group) == 4:
            teamA = group[:2]
            teamB = group[2:]
            
            current_app.logger.info(f"🔍 コート{court_number}で試合作成")
            
            # プレイヤーのエントリーステータスを更新
            for p in teamA:
                try:
                    match_table.update_item(
                        Key={'entry_id': p['entry_id']},
                        UpdateExpression="SET #status = :playing, entry_status = :playing, match_id = :mid, court_number = :court, team_side = :team",
                        ExpressionAttributeNames={"#status": "entry_status"},
                        ExpressionAttributeValues={
                            ":playing": "playing",
                            ":mid": match_id,  # 統一形式のIDを使用
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
                            ":mid": match_id,  # 統一形式のIDを使用
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
            current_app.logger.info(f"🔍 グループ{court_number}は{len(group)}人なので休憩")
            rest.extend(group)
    
    current_app.logger.info(f"🔍 ペアリング結果: {len(matches)}コート使用, {len(rest)}人休憩")
    
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


def perform_pairing_v2(entries, match_id, max_courts=6):
    """
    プレイヤーのDB更新を行わない版（データ構造のみ返す）
    create_pairings関数で一括更新する場合に使用
    """
    matches = []
    rest = []
    court_number = 1

    print(f"🔍 DEBUG: 総エントリー数 = {len(entries)}")
    print(f"🔍 DEBUG: 最大コート数 = {max_courts}")

    random.shuffle(entries)

    # 4人ずつのグループを作成
    for i in range(0, len(entries), 4):
        if court_number > max_courts:
            # コート数を超えた場合、残りは全て休憩
            remaining_players = entries[i:]
            print(f"🔍 DEBUG: コート数超過 - 残り{len(remaining_players)}人は休憩")
            rest.extend(remaining_players)
            break

        group = entries[i:i + 4]
        print(f"🔍 DEBUG: グループ{court_number}: {len(group)}人")
        
        if len(group) == 4:
            # 4人なので試合を作成
            teamA = group[:2]
            teamB = group[2:]

            print(f"🔍 DEBUG: コート{court_number}で試合作成")
            for p in teamA:
                print(f"🔍 DEBUG: チームA: {p.get('display_name')}")
            for p in teamB:
                print(f"🔍 DEBUG: チームB: {p.get('display_name')}")

            # 新しい辞書形式でマッチデータを作成
            match_data = {
                f"court_{court_number}": {
                    "team_a": teamA,
                    "team_b": teamB
                }
            }
            
            matches.append(match_data)
            court_number += 1

        else:
            # 4人未満なので休憩
            print(f"🔍 DEBUG: グループ{court_number}は{len(group)}人なので休憩")
            rest.extend(group)

    print(f"🎉 DEBUG: ペアリング完了 - 試合数: {len(matches)}, 休憩者数: {len(rest)}")
    return matches, rest


# @bp_game.route("/finish_current_match", methods=["POST"])
# @login_required
# def finish_current_match():
#     try:
#         match_table = current_app.dynamodb.Table("bad-game-match_entries")
#         match_id = get_latest_match_id()
#         if not match_id:
#             current_app.logger.warning("最新の試合IDが見つかりません")
#             return "試合IDが見つかりません", 400

#         # 今の試合に出ているプレイヤーを取得（metaデータ除外）
#         playing_response = match_table.scan(
#             FilterExpression=Attr("match_id").eq(match_id) & ~Attr("entry_id").contains("meta")
#         )
#         playing_players = playing_response.get("Items", [])

#         for player in playing_players:
#             match_table.update_item(
#                 Key={'entry_id': player['entry_id']},
#                 UpdateExpression="SET #status = :pending, entry_status = :pending, match_id = :mid REMOVE court, team",
#                 ExpressionAttributeNames={"#status": "status"},
#                 ExpressionAttributeValues={
#                     ":pending": "pending",
#                     ":mid": "pending"
#                 }
#             )
#             current_app.logger.info(f"→ {player.get('display_name')} ({player.get('user_id')}) を pending に戻しました")

#         return "OK", 200

#     except Exception as e:
#         current_app.logger.error(f"[試合終了処理エラー] {str(e)}")
#         return "エラー", 500

@bp_game.route("/finish_current_match", methods=["POST"])
@login_required
def finish_current_match():
    try:
        # 最新の試合IDを取得
        match_id = get_latest_match_id()
        if not match_id:
            current_app.logger.warning("⚠️ アクティブな試合IDが見つかりません")
            return "アクティブな試合が見つかりません", 400

        current_app.logger.info(f"🏁 試合終了処理開始: match_id={match_id}")

        # 試合ID形式の検証（オプション）
        match_id_pattern = re.compile(r'^\d{8}_\d{6}$')
        if not match_id_pattern.match(match_id):
            current_app.logger.warning(f"⚠️ 非標準形式の試合ID: {match_id}")
        
        # 試合に出ていたプレイヤーを pending に戻す処理
        match_table = current_app.dynamodb.Table("bad-game-match_entries")
        playing_response = match_table.scan(
            FilterExpression=Attr("match_id").eq(match_id) & ~Attr("entry_id").contains("meta")
        )
        playing_players = playing_response.get("Items", [])
        
        updated_count = 0
        for player in playing_players:
            try:
                match_table.update_item(
                    Key={'entry_id': player['entry_id']},
                    UpdateExpression="SET entry_status = :pending, match_id = :mid REMOVE court_number, team_side",
                    ExpressionAttributeValues={
                        ":pending": "pending",
                        ":mid": "pending"
                    }
                )
                updated_count += 1
            except Exception as e:
                current_app.logger.error(f"⚠️ プレイヤー更新エラー: {player.get('display_name', 'Unknown')} - {str(e)}")
        
        current_app.logger.info(f"✅ {updated_count}/{len(playing_players)}人のプレイヤーを待機状態に更新")

        # TrueSkill評価の呼び出し
        results_table = current_app.dynamodb.Table("bad-game-results")
        response = results_table.scan(
            FilterExpression=Attr("match_id").eq(match_id)
        )
        match_results = response.get("Items", [])
        
        current_app.logger.info(f"🎮 試合結果数: {len(match_results)}")

        skill_update_count = 0
        for result in match_results:
            try:
                team_a = parse_players(result["team_a"])
                team_b = parse_players(result["team_b"])
                winner = result.get("winner", "A")

                result_item = {
                    "team_a": team_a,
                    "team_b": team_b,
                    "winner": winner
                }

                current_app.logger.info(f"🎯 コート{result.get('court_number')}: {winner}チーム勝利")
                update_trueskill_for_players(result_item)
                skill_update_count += 1
            except Exception as e:
                court_number = result.get('court_number', 'Unknown')
                current_app.logger.error(f"⚠️ スキル更新エラー (コート{court_number}): {str(e)}")

        current_app.logger.info(f"✅ スキル更新完了: {skill_update_count}/{len(match_results)}コート, match_id={match_id}")
        
        # フラッシュメッセージでユーザーに通知（オプション）
        try:
            flash(f"試合が終了しました。{updated_count}人のプレイヤーを待機状態に戻しました。", "success")
        except Exception:
            pass
        
        # Ajaxリクエストの場合はJSONを返し、それ以外はリダイレクト
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({
                "success": True,
                "message": "試合が正常に終了しました",
                "updated_players": updated_count,
                "skill_updates": skill_update_count
            })
        
        return redirect(url_for('game.court'))

    except Exception as e:
        current_app.logger.error(f"[試合終了処理エラー] {str(e)}")
        import traceback
        current_app.logger.error(traceback.format_exc())
        
        # Ajaxリクエストの場合はJSONエラーを返し、それ以外はリダイレクト
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"success": False, "error": str(e)}), 500
        
        flash(f"エラーが発生しました: {str(e)}", "danger")
        return redirect(url_for('game.court'))

@bp_game.route("/start_next_match", methods=["POST"])
@login_required
def start_next_match():
    try:
        latest_match_id = get_latest_match_id()
        current_app.logger.info(f"🔍 最新の試合ID: {latest_match_id}")
        
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

        current_app.logger.info(f"🔍 次の試合エントリー数: {len(new_entries)}")
        for entry in new_entries:
            current_app.logger.info(f"  - {entry['display_name']}")

        # DynamoDBに新規エントリーを登録
        for entry in new_entries:
            match_table.put_item(Item=entry)

        # ペアリング処理を実行 - 統一形式のIDを渡す
        matches, rest = perform_pairing(new_entries, new_match_id)
        
        # 結果のサマリーをログに出力
        current_app.logger.info(f"✅ ペアリング完了: {len(matches)}コート、{len(new_entries)-len(rest)}人参加、{len(rest)}人休憩")
        
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
    
    current_app.logger.info(f"🎯 生成された試合ID: {match_id}")
    return match_id


@bp_game.route('/rest', methods=['GET', 'POST'])
@login_required
def rest():
    """休憩モードに切り替え"""
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
            flash('休憩モードになりました', 'info')
        
    except Exception as e:
        current_app.logger.error(f'休憩エラー: {e}')
        flash('休憩モードの設定に失敗しました', 'danger')
    
    return redirect(url_for('game.court'))

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
                    ':match_id': 'pending',  # ← ここが重要！
                    ':time': datetime.now().isoformat()
                }
            )
            flash('復帰しました！試合をお待ちください', 'success')
        else:
            flash('現在のエントリが見つかりませんでした', 'warning')

    except Exception as e:
        current_app.logger.error(f'復帰エラー: {e}')
        flash('復帰に失敗しました', 'danger')
    
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
    pending_players = get_pending_players()
    resting_players = get_resting_players()

    latest_match_id = get_latest_match_id()
    print(f"✅ 最新の試合ID: {latest_match_id}")

    new_pairing_available = False

    if latest_match_id:
        game_meta_table = current_app.dynamodb.Table("bad-game-matches")
        try:
            # ✅ entry_id の形式に合わせて meta# を追加            
            response = game_meta_table.get_item(Key={"match_id": latest_match_id})
            match_item = response.get("Item", {})

            print(f"✅ 試合データ: {match_item}")

            if match_item and not match_item.get("is_started", True):
                new_pairing_available = True
        except Exception as e:
            current_app.logger.error(f"試合情報の取得に失敗: {e}")

    return jsonify({
        "pending_count": len(pending_players),
        "resting_count": len(resting_players),
        "new_pairing_available": new_pairing_available
    })
   
    
@bp_game.route("/set_score_format", methods=["POST"])
@login_required
def set_score_format():
    selected_format = request.form.get("score_format")
    if selected_format in {"15", "21"}:
        session["score_format"] = selected_format
    return redirect(url_for("game.court"))

@bp_game.route('/api/match_score_status/<match_id>')
@login_required
def match_score_status(match_id):    
    game_meta_table = current_app.dynamodb.Table("bad-game-matches")
    meta_entry_id = f"meta#{match_id}"

    try:        
        response = game_meta_table.get_item(Key={'match_id': match_id})
        
        match_item = response.get('Item', {})

        # コート数は事前にどこかに保存されているか、固定値でも可
        court_count = 3  # 例
        all_submitted = all(
            match_item.get(f"court_{i}_score") for i in range(1, court_count + 1)
        )

        return jsonify({"all_submitted": all_submitted})
    except Exception as e:
        current_app.logger.error(f"[スコア確認エラー] {e}")
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
        current_app.logger.info(f"💬 スコア送信開始: match_id={match_id}, court={court_number}")
        current_app.logger.info(f"💬 リクエストデータ: {dict(request.form)}")
        
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
            current_app.logger.warning(f"⚠️ 非標準形式の試合ID: {match_id}")
        
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
        
        # チームごとに分類
        team_a = []
        team_b = []
        
        for entry in entries:
            player_data = {
                "user_id": str(entry.get("user_id", "")),  # ← これに合わせる
                "display_name": str(entry.get("display_name", "不明"))
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
        timestamp = datetime.now(pytz.timezone("Asia/Tokyo")).isoformat()
        
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
        current_app.logger.info(f"💾 保存する結果アイテム: {result_item}")

        # 試合結果保存
        try:
            response = result_table.put_item(Item=result_item)
            current_app.logger.info(f"✅ スコア送信成功: {match_id}, コート {court_number}, スコア {team1_score}-{team2_score}")
            current_app.logger.info(f"📥 DynamoDB応答: {response}")
        except Exception as e:
            current_app.logger.error(f"❌ 結果保存エラー: {str(e)}")
            return "スコアの保存に失敗しました", 500

        # スキルスコア更新
        try:
            update_trueskill_for_players(result_item)
        except Exception as e:
            current_app.logger.error(f"[TrueSkill 更新エラー] {str(e)}")
            # エラーがあってもスコア自体は保存されているので、200を返す
            return "スコアは保存されましたが、スキルスコアの更新に失敗しました", 200

        # JavaScriptが制御するので明示的にリダイレクトせずOKだけ返す
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
            current_app.logger.info("✅ 全エントリー削除完了")

        # 3. (オプション) results テーブルのメンテナンス
        # ここでresultsテーブルに対する処理を行う場合は追加

        flash(f"練習終了しました！エントリー {deleted_count} 件を削除", 'success')
        current_app.logger.info(f"[全削除成功] エントリー削除件数: {deleted_count} by {current_user.email}")

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        current_app.logger.error(f"[全削除失敗] {str(e)}")
        current_app.logger.error(f"スタックトレース: {error_trace}")
        flash("参加者の全削除に失敗しました", 'danger')

    return redirect(url_for('game.court'))


def get_organized_match_data(match_id):
    """試合データを整理して返す共通関数"""
    match_table = current_app.dynamodb.Table("bad-game-match_entries")
    response = match_table.scan(
        FilterExpression=Attr("match_id").eq(match_id)
    )
    items = response.get("Items", [])
    current_app.logger.info(f"[get_organized_match_data] match_id={match_id}, 取得エントリ数: {len(items)}")
    
    # ソートしてからチームに割り当てる（一貫性を保つため）
    # team_sideでソートすると、AとBが順番に並ぶ
    sorted_items = sorted(items, key=lambda x: (x.get("court_number", 999), x.get("team_side", "")))
    
    match_courts = {}
    for item in sorted_items:
        court = item.get("court_number")
        team = item.get("team_side")
        display_name = item.get("display_name", "(no name)")
        current_app.logger.info(f"[item] court={court}, team={team}, display_name={display_name}")
        
        if court is not None and team in ["A", "B"]:
            court_data = match_courts.setdefault(court, {
                "court_number": court,
                "team_a": [],
                "team_b": []
            })
            if team == "A":
                court_data["team_a"].append(item)
            elif team == "B":
                court_data["team_b"].append(item)
    
    # 各コートのチームメンバーが正しく割り当てられているか確認ログ
    for court, data in match_courts.items():
        team_a_names = [player.get("display_name", "") for player in data["team_a"]]
        team_b_names = [player.get("display_name", "") for player in data["team_b"]]
        current_app.logger.info(f"Court {court}: Team A = {team_a_names}, Team B = {team_b_names}")
    
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
    """開発用：テストデータを作成（新設計対応）"""
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
    
    try:
        now = datetime.now().isoformat()
        for i, player in enumerate(test_players):            
            entry_id = str(uuid.uuid4())
            user_id = f'test_user_{i}'

            # 新設計に必要なフィールドを明示的に付与
            item = {
            'entry_id': entry_id,
            'user_id': user_id,
            'display_name': player['display_name'],
            'joined_at': now,
            'created_at': now,
            'match_id': "pending",
            'entry_status': "pending",
            'skill_score': player.get('skill_score', 50),
            'rest_count': 0,
        }
            match_table.put_item(Item=item)        

    except Exception as e:
        current_app.logger.error(f'[create_test_data] 失敗: {str(e)}')        

    return redirect(url_for('game.court'))

@bp_game.route('/test_data_status')
@login_required
def test_data_status():
    """開発用：テストデータの状態を確認"""
    if not current_user.administrator:
        flash('管理者のみ実行可能です', 'danger')
        return redirect(url_for('index'))
    
    try:
        # test_user_ で始まるuser_idを持つすべてのエントリを検索
        response = match_table.scan(
            FilterExpression="begins_with(user_id, :prefix)",
            ExpressionAttributeValues={":prefix": "test_user_"}
        )
        
        items = response.get('Items', [])
        
        # match_idごとにグループ化
        groups = {}
        for item in items:
            match_id = item.get('match_id', 'unknown')
            if match_id not in groups:
                groups[match_id] = []
            groups[match_id].append(item)
        
        # 結果をHTMLで表示
        output = "<h1>テストデータの状態</h1>"
        output += f"<p>テストデータの総数: {len(items)}件</p>"
        
        for match_id, group_items in groups.items():
            output += f"<h2>マッチID: {match_id} ({len(group_items)}件)</h2>"
            output += "<ul>"
            for item in group_items:
                output += f"<li>{item.get('display_name')} (ID: {item.get('entry_id')})</li>"
            output += "</ul>"
        
        return output
        
    except Exception as e:
        return f"エラー: {e}"
    
@bp_game.route('/clear_test_data')
@login_required
def clear_test_data():
    """開発用：test_user_ のテストデータを削除"""
    if not current_user.administrator:
        
        return redirect(url_for('index'))

    deleted_count = 0
    last_evaluated_key = None

    try:
        while True:
            scan_kwargs = {
                'FilterExpression': Attr('user_id').begins_with("test_user_")
            }
            if last_evaluated_key:
                scan_kwargs['ExclusiveStartKey'] = last_evaluated_key

            response = match_table.scan(**scan_kwargs)
            items = response.get('Items', [])

            for item in items:
                try:
                    match_table.delete_item(Key={                        
                        'entry_id': item['entry_id']
                    })
                    deleted_count += 1
                except Exception as e:
                    current_app.logger.error(f"削除失敗: {e}, item: {item}")

            last_evaluated_key = response.get('LastEvaluatedKey')
            if not last_evaluated_key:
                break
        
    except Exception as e:

        pass
        

   


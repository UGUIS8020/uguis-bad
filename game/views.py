from flask import Blueprint, render_template, redirect, url_for, flash, request, current_app
from flask_login import login_required, current_user
import boto3
import uuid
from datetime import datetime, date
import random
from boto3.dynamodb.conditions import Key, Attr, And
from flask import jsonify
from collections import defaultdict
import re
from flask import session



bp_game = Blueprint('game', __name__)

# DynamoDBリソース取得
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
match_table = dynamodb.Table('bad-game-match_entries')
user_table = dynamodb.Table("bad-users")

# @bp_game.route("/enter_the_court")
# @login_required
# def enter_the_court():
#     try:
#         current_app.logger.info("=== コート入場開始 ===")
        
#         current_app.logger.info("auto_register_user 開始")
#         auto_register_user()
#         current_app.logger.info("auto_register_user 完了")
        
#         current_app.logger.info("get_pending_players 開始")
#         pending_players = get_pending_players()
#         current_app.logger.info(f"pending_players: {len(pending_players)}人")
        
#         current_app.logger.info("get_resting_players 開始")
#         resting_players = get_resting_players()
#         current_app.logger.info(f"resting_players: {len(resting_players)}人")
        
#         current_app.logger.info("get_user_status 開始")
#         user_status = get_user_status(current_user.get_id())
#         current_app.logger.info(f"user_status: {user_status}")
        
#         current_app.logger.info("履歴データ取得開始")
#         today = date.today().isoformat()
#         history_table = current_app.dynamodb.Table("bad-users-history")
#         history_response = history_table.scan(
#             FilterExpression=Attr('user_id').eq(current_user.get_id())
#         )
#         history_items = history_response.get('Items', [])
#         match_count = sum(1 for h in history_items if h.get('date') and h['date'] < today)
#         current_app.logger.info(f"match_count: {match_count}")
        
#         current_app.logger.info("テンプレート表示開始")
#         return render_template(
#             'game/court.html',
#             pending_players=pending_players,
#             resting_players=resting_players,
#             is_registered=user_status['is_registered'],
#             is_resting=user_status['is_resting'],
#             current_user_skill_score=user_status['skill_score'],
#             current_user_match_count=match_count
#         )
        
#     except Exception as e:
#         current_app.logger.error(f"コート入場エラー詳細: {str(e)}")
#         import traceback
#         current_app.logger.error(f"スタックトレース: {traceback.format_exc()}")
#         return f"エラー: {e}"
    
@bp_game.route("/enter_the_court")
@login_required
def enter_the_court():
    try:
        current_app.logger.info("=== コート入場開始 ===")

        auto_register_user()
        pending_players = get_pending_players()
        resting_players = get_resting_players()
        user_status = get_user_status(current_user.get_id())

        # 🆕 最新試合の取得と試合データ構築
        match_id = get_latest_match_id()
        match_courts = get_match_players_by_court(match_id) if match_id else {}

        # 試合履歴取得
        today = date.today().isoformat()
        history_table = current_app.dynamodb.Table("bad-users-history")
        history_response = history_table.scan(
            FilterExpression=Attr('user_id').eq(current_user.get_id())
        )
        history_items = history_response.get('Items', [])
        match_count = sum(1 for h in history_items if h.get('date') and h['date'] < today)

        return render_template(
            'game/court.html',
            pending_players=pending_players,
            resting_players=resting_players,
            is_registered=user_status['is_registered'],
            is_resting=user_status['is_resting'],
            current_user_skill_score=user_status['skill_score'],
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
    today_prefix = datetime.now().strftime("%Y%m%d")
    response = match_table.scan(
        FilterExpression=Attr("match_id").begins_with(today_prefix)
    )
    items = response.get("Items", [])
    if not items:
        return None
    latest = max(items, key=lambda x: x.get("match_id", ""))
    return latest.get("match_id")

def get_match_players_by_court(match_id):
    match_table = current_app.dynamodb.Table("bad-game-match_entries")
    response = match_table.scan(
        FilterExpression=Attr("match_id").eq(match_id)
    )
    players = response.get("Items", [])
    courts = {}

    for p in players:
        # court_numがDecimalならintに変換、または0でスキップ
        try:
            court_num = int(p.get("court", 0))
        except:
            continue

        team_id = p.get("team", "")

        # チーム名の末尾がA/Bか判定（例：20250701_099_1A）
        team_suffix = team_id.split('_')[-1] if team_id else ""
        team_flag = team_suffix[-1] if team_suffix else ""

        # プレイヤー情報を辞書として抽出
        player_info = {
            "user_id": p.get("user_id"),
            "display_name": p.get("display_name", "匿名"),
            "skill_score": int(p.get("skill_score", 0)),
            "gender": p.get("gender", "unknown"),
            "organization": p.get("organization", ""),
            "badminton_experience": p.get("badminton_experience", "")
        }

        # court 番号も team_id も有効なら分類
        if court_num and team_flag in ["A", "B"]:
            if court_num not in courts:
                courts[court_num] = {
                    "court_number": court_num,
                    "team_a": [],
                    "team_b": [],
                }

            if team_flag == "A":
                courts[court_num]["team_a"].append(player_info)
            elif team_flag == "B":
                courts[court_num]["team_b"].append(player_info)

    print(courts)
    return courts        
    
def get_latest_match_id():
    """
    DynamoDBから今日の日付のmatch_idをプレフィックスに持つ試合の中で
    最新（連番が最大）のmatch_idを取得する。
    例: "20250701_001", "20250701_002", ... の中から最大値を取得。
    """
    try:
        today_prefix = datetime.now().strftime("%Y%m%d")
        match_table = current_app.dynamodb.Table("bad-game-match_entries")

        response = match_table.scan(
            FilterExpression=Attr("match_id").begins_with(today_prefix)
        )
        items = response.get("Items", [])

        if not items:
            current_app.logger.info("✅ 今日の試合はまだ登録されていません。")
            return None

        # match_idの文字列比較で最大を取得
        latest = max(items, key=lambda x: x.get("match_id", ""))
        latest_match_id = latest.get("match_id")

        current_app.logger.info(f"✅ 最新の match_id: {latest_match_id}")
        return latest_match_id

    except Exception as e:
        current_app.logger.error(f"get_latest_match_id() エラー: {str(e)}")
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
            'status': 'success'
        })
    except Exception as e:
        current_app.logger.error(f"コート状況API エラー: {str(e)}")
        return jsonify({'error': str(e), 'status': 'error'}), 500


def get_pending_players():
    """参加待ちプレイヤーを取得"""
    try:
        today = date.today().isoformat()
        history_table = current_app.dynamodb.Table("bad-users-history")
        response = match_table.scan(
            FilterExpression=Attr('match_id').eq('pending') & Attr('entry_status').eq('active')
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
        
        # スキルスコアを取得
        skill_score = None
        
        # pending_itemsまたはresting_itemsからスキルスコアを取得
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
    
def auto_register_user():
    """自動参加登録（休憩中でも入場すれば自動的に pending 登録）"""
    user_id = current_user.get_id()

    # すでに "pending" 状態か確認
    pending_response = match_table.scan(
        FilterExpression=Attr('user_id').eq(user_id) & Attr('match_id').eq('pending')
    )
    if pending_response.get('Items'):
        return  # すでに登録済み

    # 休憩中データがあれば削除
    resting_response = match_table.scan(
        FilterExpression=Attr('user_id').eq(user_id) & Attr('match_id').eq('resting')
    )
    for item in resting_response.get('Items', []):
        match_table.delete_item(Key={'entry_id': item['entry_id']})

    # 新規エントリー（pending 登録）
    entry_id = str(uuid.uuid4())
    now = datetime.now().isoformat()

    user_response = user_table.get_item(Key={"user#user_id": user_id})
    skill_score = user_response.get("Item", {}).get("skill_score", 50)

    match_table.put_item(Item={
        'entry_id': entry_id,
        'user_id': user_id,
        'match_id': "pending",
        'entry_status': "active",
        'display_name': current_user.display_name,
        'skill_score': skill_score,
        'joined_at': now
    })

@bp_game.route('/create_pairings', methods=["GET", "POST"])
@login_required
def create_pairings():
    try:
        max_courts = int(request.form.get("max_courts", 6))
        max_courts = max(1, min(max_courts, 6))
    except (ValueError, TypeError):
        max_courts = 6

    response = match_table.scan(
        FilterExpression=Attr("match_id").eq("pending") & Attr("entry_status").eq("active")
    )
    entries = [e for e in response.get("Items", []) if str(e.get("entry_status")) == "active"]

    if len(entries) < 4:
        flash("4人以上のエントリーが必要です。", "danger")
        return redirect(url_for("game.enter_the_court"))

    match_id = generate_match_id()
    matches = perform_pairing(entries, match_id, max_courts)

    flash(f"ペアリングが完了しました！{len(matches)}試合が開始されます", "success")
    return redirect(url_for('game.enter_the_court'))

def perform_pairing(entries, match_id, max_courts=6):
    matches = []
    rest = []
    court_number = 1

    random.shuffle(entries)
    max_players = max_courts * 4
    players = entries[:max_players]
    rest = entries[max_players:]

    for i in range(0, len(players), 4):
        if court_number > max_courts:
            rest.extend(players[i:])
            break

        group = players[i:i + 4]
        if len(group) == 4:
            teamA = group[:2]
            teamB = group[2:]
            team_a_id = f"{match_id}_{court_number}A"
            team_b_id = f"{match_id}_{court_number}B"

            for p in teamA:
                match_table.update_item(
                    Key={"entry_id": p["entry_id"]},
                    UpdateExpression="SET match_id = :m, court = :c, team = :t, entry_status = :s",
                    ExpressionAttributeValues={
                        ":m": match_id,
                        ":c": court_number,
                        ":t": team_a_id,
                        ":s": "playing"
                    }
                )

            for p in teamB:
                match_table.update_item(
                    Key={"entry_id": p["entry_id"]},
                    UpdateExpression="SET match_id = :m, court = :c, team = :t, entry_status = :s",
                    ExpressionAttributeValues={
                        ":m": match_id,
                        ":c": court_number,
                        ":t": team_b_id,
                        ":s": "playing"
                    }
                )

            matches.append({
                "court": court_number,
                "teamA": teamA,
                "teamB": teamB
            })
            court_number += 1

    # 休憩者処理
    for p in rest:
        match_table.update_item(
            Key={"entry_id": p["entry_id"]},
            UpdateExpression="SET match_id = :m, entry_status = :s REMOVE court, team",
            ExpressionAttributeValues={
                ":m": match_id,
                ":s": "playing"
            }
        )

    # メタデータ保存
    match_table.put_item(Item={
        'entry_id': f"meta#{match_id}",
        'match_id': match_id,
        'is_started': False,
        'type': 'meta',
        'created_at': datetime.now().isoformat()
    })

    return matches

@bp_game.route("/game/start_next_match", methods=["POST"])
@login_required
def start_next_match():
    latest_match_id = get_latest_match_id()
    current_players_by_court = get_match_players_by_court(latest_match_id)

    current_players = []
    for court_data in current_players_by_court.values():
        current_players.extend(court_data["team_a"])
        current_players.extend(court_data["team_b"])

    if not current_players:
        return "参加者が見つかりません", 400

    new_match_id = generate_match_id()
    new_entries = []

    match_table = current_app.dynamodb.Table("bad-game-match_entries")  # ← 追加

    for p in current_players:
        new_entries.append({
            'entry_id': str(uuid.uuid4()),
            'user_id': p['user_id'],
            'match_id': "pending",
            'entry_status': 'active',
            'display_name': p['display_name'],
            'badminton_experience': p.get('badminton_experience'),
            'skill_score': p.get('skill_score'),
            'joined_at': datetime.now().isoformat()
        })

    for entry in new_entries:
        match_table.put_item(Item=entry)

    perform_pairing(new_entries, new_match_id)

    return redirect(url_for("game.enter_the_court"))

@bp_game.route("/game/pairings", methods=["GET"])
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
    today_str = datetime.now().strftime("%Y%m%d")  # "20250623"
    
    # すでに存在する今日のmatch_idを数える（prefix一致で検索）
    response = match_table.scan(
        FilterExpression="begins_with(match_id, :prefix)",
        ExpressionAttributeValues={":prefix": today_str}
    )
    count = len(response.get('Items', [])) + 1
    match_id = f"{today_str}_{count:03d}"  # "20250623_001"
    return match_id


@bp_game.route('/rest', methods=['POST'])
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
    
    return redirect(url_for('game.enter_the_court'))

@bp_game.route('/resume', methods=['POST'])
@login_required
def resume():
    """復帰（アクティブに戻す）"""
    try:
        current_entry = get_user_current_entry(current_user.get_id())
        if current_entry:
            match_table.update_item(
                Key={'entry_id': current_entry['entry_id']},
                UpdateExpression='SET entry_status = :status, resumed_at = :time',
                ExpressionAttributeValues={
                    ':status': 'active',
                    ':time': datetime.now().isoformat()
                }
            )
            flash('復帰しました！試合をお待ちください', 'success')
        
    except Exception as e:
        current_app.logger.error(f'復帰エラー: {e}')
        flash('復帰に失敗しました', 'danger')
    
    return redirect(url_for('game.enter_the_court'))

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
                return redirect(url_for('game.enter_the_court'))
            
            # エントリーを削除
            match_table.delete_item(Key={'entry_id': current_entry['entry_id']})
            flash('コートから退出しました', 'info')
            return redirect(url_for('index'))
        
    except Exception as e:
        current_app.logger.error(f'退出エラー: {e}')
        flash('退出に失敗しました', 'danger')
    
    return redirect(url_for('game.enter_the_court'))

def get_user_current_entry(user_id):
    """ユーザーの現在のエントリーを取得"""
    try:
        response = match_table.scan(
            FilterExpression=Attr('user_id').eq(user_id)
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
        match_table = current_app.dynamodb.Table("bad-game-match_entries")  # ✅ 実際のテーブル名に修正
        try:
            # ✅ entry_id の形式に合わせて meta# を追加
            response = match_table.get_item(Key={"entry_id": f"meta#{latest_match_id}"})
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


@bp_game.route("/game/submit_score/<match_id>/court/<int:court_number>", methods=["POST"])
@login_required
def submit_score(match_id, court_number):
    try:
        # フォームデータの取得（例: 勝利チームを選ぶラジオボタン）
        winner = request.form.get("winner")  # "A" or "B"

        if winner not in {"A", "B"}:
            flash("勝利チームが正しく選択されていません", "danger")
            return redirect(url_for("game.enter_the_court"))

        # DynamoDBテーブル取得
        match_table = current_app.dynamodb.Table("matches")

        # 対象の試合データを取得
        response = match_table.get_item(Key={"match_id": match_id})
        match_item = response.get("Item")

        if not match_item:
            flash("対象の試合データが見つかりません", "danger")
            return redirect(url_for("game.enter_the_court"))

        # コート番号をキーに該当コートのスコア入力を記録
        score_key = f"court_{court_number}_score"

        update_expr = f"SET {score_key} = :score"
        expr_values = {":score": winner}

        # スコア更新
        match_table.update_item(
            Key={"match_id": match_id},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_values
        )

        flash(f"{court_number}番コートのスコアを記録しました", "success")

        # （オプション）すべてのスコアが入力済みかを判定して自動的に試合終了処理しても良い

        return redirect(url_for("game.enter_the_court"))

    except Exception as e:
        current_app.logger.error(f"[submit_score] スコア提出エラー: {e}")
        flash("スコアの提出中にエラーが発生しました", "danger")
        return redirect(url_for("game.enter_the_court"))
    
    
@bp_game.route("/game/set_score_format", methods=["POST"])
@login_required
def set_score_format():
    selected_format = request.form.get("score_format")
    if selected_format in {"15", "21"}:
        session["score_format"] = selected_format
    return redirect(url_for("game.enter_the_court"))







@bp_game.route('/create_test_data')
@login_required
def create_test_data():
    """開発用：テストデータを作成"""
    if not current_user.administrator:
        flash('管理者のみ実行可能です', 'danger')
        return redirect(url_for('index'))
    
    test_players = [
        {'display_name': '田中太郎'},
        {'display_name': '佐藤花子'},
        {'display_name': '鈴木一郎'},
        {'display_name': '高橋美咲'},
        {'display_name': '山田健太'},
        {'display_name': '渡辺さくら'},
        {'display_name': '松本大輔'},
        {'display_name': '中村実'},
    ]
    
    try:
        for i, player in enumerate(test_players):
            item = {
                'entry_id': str(uuid.uuid4()),
                'user_id': f'test_user_{i}',
                'display_name': player['display_name'],
                'joined_at': datetime.now().isoformat(),
                'match_id': "pending",
                'entry_status': "active",  # ← 追加
                'skill_score': 50  # ← 任意（必要なら）
            }
            match_table.put_item(Item=item)
        
        flash(f'{len(test_players)}人のテストデータを作成しました！', 'success')
        
    except Exception as e:
        flash(f'テストデータ作成に失敗: {e}', 'danger')
    
    return redirect(url_for('game.enter_the_court'))

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
        flash('管理者のみ実行可能です', 'danger')
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

        flash(f'{deleted_count}件のテストデータを削除しました', 'success')
    except Exception as e:
        flash(f'テストデータ削除に失敗: {e}', 'danger')

    return redirect(url_for('game.enter_the_court'))


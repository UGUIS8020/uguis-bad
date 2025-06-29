from flask import Blueprint, render_template, redirect, url_for, flash, request, current_app
from flask_login import login_required, current_user
import boto3
import uuid
from datetime import datetime, date
import random
from boto3.dynamodb.conditions import Key, Attr, And
from flask import jsonify

bp_game = Blueprint('game', __name__)

# DynamoDBリソース取得
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
match_table = dynamodb.Table('bad-game-match_entries')
user_table = dynamodb.Table("bad-users")

# @bp_game.route("/enter_the_court")
# @login_required
# def enter_the_court():
#     try:
#         auto_register_user()  # ✅ 自動エントリー
        
#         pending_players = get_pending_players()  # ✅ 待機中プレイヤー取得
#         resting_players = get_resting_players()
#         user_status = get_user_status(current_user.get_id())
        
#         # 試合回数は表示（シンプルに）
#         today = date.today().isoformat()
#         history_table = current_app.dynamodb.Table("bad-users-history")
#         history_response = history_table.scan(
#             FilterExpression=Attr('user_id').eq(current_user.get_id())
#         )
#         history_items = history_response.get('Items', [])
#         match_count = sum(1 for h in history_items if h.get('date') and h['date'] < today)

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
#         current_app.logger.error(f"コート入場エラー: {str(e)}")
#         flash(f"コートへの入場に失敗しました", "danger")
#         return redirect(url_for("index"))

@bp_game.route("/enter_the_court")
@login_required
def enter_the_court():
    try:
        current_app.logger.info("=== コート入場開始 ===")
        
        current_app.logger.info("auto_register_user 開始")
        auto_register_user()
        current_app.logger.info("auto_register_user 完了")
        
        current_app.logger.info("get_pending_players 開始")
        pending_players = get_pending_players()
        current_app.logger.info(f"pending_players: {len(pending_players)}人")
        
        current_app.logger.info("get_resting_players 開始")
        resting_players = get_resting_players()
        current_app.logger.info(f"resting_players: {len(resting_players)}人")
        
        current_app.logger.info("get_user_status 開始")
        user_status = get_user_status(current_user.get_id())
        current_app.logger.info(f"user_status: {user_status}")
        
        current_app.logger.info("履歴データ取得開始")
        today = date.today().isoformat()
        history_table = current_app.dynamodb.Table("bad-users-history")
        history_response = history_table.scan(
            FilterExpression=Attr('user_id').eq(current_user.get_id())
        )
        history_items = history_response.get('Items', [])
        match_count = sum(1 for h in history_items if h.get('date') and h['date'] < today)
        current_app.logger.info(f"match_count: {match_count}")
        
        current_app.logger.info("テンプレート表示開始")
        return render_template(
            'game/court.html',
            pending_players=pending_players,
            resting_players=resting_players,
            is_registered=user_status['is_registered'],
            is_resting=user_status['is_resting'],
            current_user_skill_score=user_status['skill_score'],
            current_user_match_count=match_count
        )
        
    except Exception as e:
        current_app.logger.error(f"コート入場エラー詳細: {str(e)}")
        import traceback
        current_app.logger.error(f"スタックトレース: {traceback.format_exc()}")
        return f"エラー: {e}"

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
    """自動参加登録（register関数のロジックを流用）"""
    # すでに "pending" 状態か確認
    pending_response = match_table.scan(
        FilterExpression=Attr('user_id').eq(current_user.get_id()) & Attr('match_id').eq('pending')
    )
    if pending_response.get('Items'):
        return  # すでに登録済み
    
    # "resting" 状態なら何もしない（手動で復帰してもらう）
    resting_response = match_table.scan(
        FilterExpression=Attr('user_id').eq(current_user.get_id()) & Attr('match_id').eq('resting')
    )
    if resting_response.get('Items'):
        return  # 休憩中は自動復帰しない
    
    # 新規エントリー（register関数と同じロジック）
    entry_id = str(uuid.uuid4())
    now = datetime.now().isoformat()
    
    user_response = user_table.get_item(Key={"user#user_id": current_user.get_id()})
    skill_score = user_response.get("Item", {}).get("skill_score", 50)

    match_table.put_item(Item={
        'entry_id': entry_id,
        'user_id': current_user.get_id(),
        'match_id': "pending",
        'entry_status': "active",
        'display_name': current_user.display_name,
        'skill_score': skill_score,
        'joined_at': now
    })

@bp_game.route('/create_pairings', methods=["GET", "POST"])
@login_required
def create_pairings():
    # ✅ エントリー中のプレイヤーを取得（match_id = 'pending' & entry_status = 'active'）
    response = match_table.scan(
        FilterExpression=Attr("match_id").eq("pending") & Attr("entry_status").eq("active")
    )
    entries = response.get("Items", [])

    # 🔍 取得したデータのログ出力
    current_app.logger.info(f"✅ 取得エントリー数: {len(entries)}")
    for e in entries:
        current_app.logger.info(f"🔎 entry_id: {e.get('entry_id')}, match_id: {e.get('match_id')}, entry_status: {e.get('entry_status')}")

    # 🛡 念のため明示的にフィルタ（文字列比較の安全性向上）
    entries = [
        e for e in entries
        if str(e.get("match_id", "")).strip() == "pending"
        and str(e.get("entry_status", "")).strip() == "active"
    ]
    current_app.logger.info(f"✅ フィルタ後エントリー数: {len(entries)}")

    if len(entries) < 4:
        flash("4人以上のエントリーが必要です。", "danger")
        current_app.logger.warning("⛔ エントリー人数不足。ペアリング中断。")
        return redirect(url_for("game.pairings"))

    # 2. 試合IDを生成（例: 20250624_001）
    match_id = generate_match_id()
    current_app.logger.info(f"🆕 生成された match_id: {match_id}")

    # 3. プレイヤーをシャッフル
    random.shuffle(entries)
    players = entries[:]
    rest = []

    if len(players) % 4 != 0:
        rest_count = len(players) % 4
        rest = players[-rest_count:]
        players = players[:-rest_count]

    current_app.logger.info(f"🧩 組み合わせ対象: {len(players)}人, 休憩者: {len(rest)}人")

    matches = []
    court_number = 1

    # 5. 4人ずつで試合を作成
    for i in range(0, len(players), 4):
        group = players[i:i + 4]
        if len(group) == 4:
            teamA = group[:2]
            teamB = group[2:]
            team_a_id = f"{match_id}_{court_number}A"
            team_b_id = f"{match_id}_{court_number}B"
            current_app.logger.info(f"🎾 コート{court_number}: {team_a_id} vs {team_b_id}")

            for p in teamA:
                current_app.logger.info(f"↪️ Aチーム: {p.get('display_name')} (entry_id: {p['entry_id']})")
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
                current_app.logger.info(f"↪️ Bチーム: {p.get('display_name')} (entry_id: {p['entry_id']})")
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

    # 6. 休憩者の処理
    for p in rest:
        current_app.logger.info(f"🪑 休憩: {p.get('display_name')} (entry_id: {p['entry_id']})")
        match_table.update_item(
            Key={"entry_id": p["entry_id"]},
            UpdateExpression="SET match_id = :m, entry_status = :s REMOVE court, team",
            ExpressionAttributeValues={
                ":m": match_id,
                ":s": "playing"
            }
        )

    flash(f"ペアリングが完了しました！{len(matches)}試合が開始されます", "success")

    # AJAX リクエストの場合のみ JSON を返す
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"success": True, "match_id": match_id})

    # 通常のフォーム送信の場合はコート画面にリダイレクト
    return redirect(url_for('game.enter_the_court'))

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
    
    return redirect(url_for('game.pairings'))

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

    return redirect(url_for('game.pairings'))


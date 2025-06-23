from flask import Blueprint, render_template, redirect, url_for, flash, request, current_app
from flask_login import login_required, current_user
import boto3
import uuid
from datetime import datetime
import random
from boto3.dynamodb.conditions import Key, Attr

bp_game = Blueprint('game', __name__)

# DynamoDBリソース取得
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
match_table = dynamodb.Table('bad-game-match_entries')
user_table = dynamodb.Table("bad-users")

@bp_game.route('/create_pairings')
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

    flash("ペアリングが完了しました", "success")
    return render_template("game/pairings.html", matches=matches, rest=rest, match_id=match_id)


@bp_game.route("/pairings")
@login_required
def pairings():
    """試合組み合わせ・参加者一覧ページ (統合版)"""
    # 1. 参加待ちプレイヤー（pendingステータス）を取得
    pending_response = match_table.scan(
        FilterExpression=Attr('match_id').eq('pending')
    )
    pending_players = pending_response.get("Items", [])
    
    # 2. 休憩中のプレイヤーを取得
    resting_response = match_table.scan(
        FilterExpression=Attr('match_id').eq('resting')
    )
    resting_players = resting_response.get("Items", [])
    
    # 3. 現在のユーザーの状態を確認
    is_registered = False
    is_resting = False
    
    for player in pending_players:
        if player.get('user_id') == current_user.get_id():
            is_registered = True
            break
    
    for player in resting_players:
        if player.get('user_id') == current_user.get_id():
            is_resting = True
            break
    
    # 4. 最新の試合IDを取得（pendingでない試合）
    matches_response = match_table.scan(
        FilterExpression=Attr('match_id').ne('pending') & Attr('match_id').ne('resting')
    )
    
    all_matches = matches_response.get('Items', [])
    
    # 試合IDでグループ化
    matches_by_id = {}
    match_ids = []
    
    for entry in all_matches:
        match_id = entry.get('match_id')
        if match_id not in matches_by_id:
            matches_by_id[match_id] = []
            match_ids.append(match_id)
        
        matches_by_id[match_id].append(entry)
    
    # 降順ソート（最新の試合を最初に）
    match_ids.sort(reverse=True)
    
    # 最新の試合ID
    latest_match_id = match_ids[0] if match_ids else None
    
    # 試合とrest（出場できなかった人）の情報を整理
    matches = []
    rest = []
    
    if latest_match_id:
        latest_entries = matches_by_id.get(latest_match_id, [])
        
        # コート番号でグループ化
        courts = {}
        
        for entry in latest_entries:
            court = entry.get('court')
            if court:
                if court not in courts:
                    courts[court] = []
                courts[court].append(entry)
            else:
                rest.append(entry)
        
        # コート番号順にソート
        for court_num in sorted(courts.keys()):
            matches.append(courts[court_num])
    
    return render_template(
        "game/pairings.html", 
        matches=matches, 
        rest=rest, 
        match_id=latest_match_id, 
        pending_players=pending_players,
        resting_players=resting_players,
        is_registered=is_registered,
        is_resting=is_resting
    )

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

@bp_game.route("/register")
@login_required
def register():
    """参加登録または休憩から復帰"""
    try:
        # すでに "pending" 状態か確認
        pending_response = match_table.scan(
            FilterExpression=Attr('user_id').eq(current_user.get_id()) & Attr('match_id').eq('pending')
        )
        if pending_response.get('Items'):
            flash("すでに参加登録されています", "info")
            return redirect(url_for("game.pairings"))
        
        # "resting" 状態なら pending に戻す
        resting_response = match_table.scan(
            FilterExpression=Attr('user_id').eq(current_user.get_id()) & Attr('match_id').eq('resting')
        )
        resting_items = resting_response.get('Items', [])
        if resting_items:
            for item in resting_items:
                match_table.update_item(
                    Key={'entry_id': item['entry_id']},
                    UpdateExpression="SET match_id = :pending, entry_status = :status",
                    ExpressionAttributeValues={
                        ':pending': 'pending',
                        ':status': 'active'
                    }
                )
            flash("休憩から復帰しました。引き続き参加します", "success")
            return redirect(url_for("game.pairings"))
        
        # 新規エントリー
        entry_id = str(uuid.uuid4())
        now = datetime.now().isoformat()
        
        # skill_score の取得
        user_response = user_table.get_item(Key={"user#user_id": current_user.get_id()})
        skill_score = user_response.get("Item", {}).get("skill_score", 50)

        # 登録（←ここで entry_status を追加）
        match_table.put_item(Item={
            'entry_id': entry_id,
            'user_id': current_user.get_id(),
            'match_id': "pending",
            'entry_status': "active",  # ← 追加！
            'display_name': current_user.display_name,
            'skill_score': skill_score,
            'joined_at': now
        })
        
        flash("参加登録が完了しました", "success")
    
    except Exception as e:
        current_app.logger.error(f"参加登録エラー: {str(e)}")
        flash(f"参加登録中にエラーが発生しました: {str(e)}", "danger")
    
    return redirect(url_for("game.pairings"))

@bp_game.route("/cancel")
@login_required
def cancel():
    """参加登録をキャンセル"""
    try:
        # 登録済みのエントリーを検索
        response = match_table.scan(
            FilterExpression=Attr('user_id').eq(current_user.get_id()) & Attr('match_id').eq('pending')
        )
        
        items = response.get('Items', [])
        
        if not items:
            flash("参加登録が見つかりません", "warning")
            return redirect(url_for("game.pairings"))
        
        # エントリーを削除
        for item in items:
            match_table.delete_item(Key={'entry_id': item['entry_id']})
        
        flash("参加登録をキャンセルしました", "success")
        
    except Exception as e:
        current_app.logger.error(f"参加キャンセルエラー: {str(e)}")
        flash(f"キャンセル中にエラーが発生しました", "danger")
    
    return redirect(url_for("game.pairings"))

@bp_game.route("/rest")
@login_required
def rest():
    """休憩する（一時的に試合から外れる）"""
    try:
        # 現在参加中かどうか確認
        response = match_table.scan(
            FilterExpression=Attr('user_id').eq(current_user.get_id()) & Attr('match_id').eq('pending')
        )
        
        items = response.get('Items', [])
        
        if not items:
            flash("参加登録していないため休憩できません", "warning")
            return redirect(url_for("game.pairings"))
        
        # 既存のエントリーを休憩状態に更新
        for item in items:
            match_table.update_item(
                Key={'entry_id': item['entry_id']},
                UpdateExpression="SET match_id = :rest_status",
                ExpressionAttributeValues={
                    ':rest_status': 'resting'
                }
            )
        
        flash("休憩モードに設定しました。再度参加する場合は「再開する」を押してください", "success")
        
    except Exception as e:
        current_app.logger.error(f"休憩設定エラー: {str(e)}")
        flash(f"休憩設定中にエラーが発生しました: {str(e)}", "danger")
    
    return redirect(url_for("game.pairings"))



@bp_game.route("/join_match", methods=["POST"])
@login_required
def join_match():   
    
    # ユーザーがすでに参加しているか確認
    response = match_table.scan(
        FilterExpression="user_id = :user_id AND match_id = :pending",
        ExpressionAttributeValues={
            ":user_id": current_user.id,
            ":pending": "pending"
        }
    )
    
    if response.get("Items"):
        flash("すでに参加登録されています", "warning")
        return redirect(url_for("game.pairings"))  # 統合ページへリダイレクト
    
    # 参加情報を登録
    item = {
        "entry_id": str(uuid.uuid4()),
        "user_id": current_user.id,
        "display_name": current_user.name,        
        "joined_at": datetime.now().isoformat(),
        "match_id": "pending"  # 試合組み前はpendingステータス
    }
    
    match_table.put_item(Item=item)
    flash("参加登録しました！", "success")
    
    return redirect(url_for("game.pairings"))  # 統合ページへリダイレクト

# @bp_game.route('/submit_score/<match_id>/<int:court_number>', methods=["POST"])
# @login_required
# def submit_score(match_id, court_number):
#     """スコア送信 → コートのプレイヤーを pending に戻す"""
#     if not current_user.administrator:
#         flash("スコア送信は管理者のみ可能です", "danger")
#         return redirect(url_for("game.pairings"))

#     try:
#         # 入力されたスコア（保存しても、ログ出力だけでもOK）
#         score_a = request.form.get("score_team_a")
#         score_b = request.form.get("score_team_b")
#         current_app.logger.info(f"✅ Court {court_number} のスコア: A={score_a}, B={score_b}")

#         # 対象のコートのプレイヤー取得
#         response = match_table.scan(
#             FilterExpression=Attr('match_id').eq(match_id) & Attr('court').eq(court_number)
#         )
#         players = response.get("Items", [])

#         for player in players:
#             match_table.update_item(
#                 Key={"entry_id": player["entry_id"]},
#                 UpdateExpression="SET match_id = :pending, entry_status = :active REMOVE court, team",
#                 ExpressionAttributeValues={
#                     ":pending": "pending",
#                     ":active": "active"
#                 }
#             )

#         flash(f"コート{court_number}のスコアを登録し、プレイヤーを待機状態に戻しました", "success")
#     except Exception as e:
#         current_app.logger.error(f"[スコア送信エラー] court={court_number}: {e}")
#         flash("スコア送信中にエラーが発生しました", "danger")

#     return redirect(url_for("game.pairings"))


@bp_game.route("/submit_score", methods=["POST"])
@login_required
def submit_score():
    try:
        match_id = request.form["match_id"]
        court_number = int(request.form["court_number"])
        score_a = int(request.form["score_team_a"])
        score_b = int(request.form["score_team_b"])

        # 該当するエントリを取得して status を更新
        response = match_table.scan(
            FilterExpression=Attr("match_id").eq(match_id) & Attr("court").eq(court_number)
        )
        entries = response.get("Items", [])

        for entry in entries:
            match_table.update_item(
                Key={"entry_id": entry["entry_id"]},
                UpdateExpression="SET match_id = :resting, entry_status = :status REMOVE court, team",
                ExpressionAttributeValues={
                    ":resting": "resting",
                    ":status": "active"
                }
            )

        return {"success": True}, 200
    except Exception as e:
        current_app.logger.error(f"スコア登録エラー: {str(e)}")
        return {"success": False, "message": str(e)}, 500

# @bp_game.route("/create_pairings")
# @login_required
# def create_pairings():
#     """管理者向け: ペアリングを実行する"""
#     if not current_user.administrator:
#         flash("管理者のみ実行できます", "danger")
#         return redirect(url_for("game.pairings"))
    
#     # pendingステータスのエントリーを取得（GSIを使用）
#     response = match_table.query(
#         IndexName='MatchIndex',
#         KeyConditionExpression=Key('match_id').eq('pending')
#     )
    
#     players = response.get("Items", [])
    
#     if len(players) < 4:
#         flash("ペアリングには最低4人の参加者が必要です", "warning")
#         return redirect(url_for("game.pairings"))
    
#     # 新しいmatch_idを生成
#     match_id = generate_match_id()
    
#     # 全ての参加者のmatch_idを更新
#     success_count = 0
#     error_count = 0
#     for player in players:
#         try:
#             # entry_idをキーとして更新
#             match_table.update_item(
#                 Key={'entry_id': player['entry_id']},
#                 UpdateExpression="SET match_id = :match_id",
#                 ExpressionAttributeValues={":match_id": match_id}
#             )
#             success_count += 1
#         except Exception as e:
#             # current_appを使用してロギング
#             current_app.logger.error(f"プレイヤー更新エラー: {str(e)}, プレイヤー: {player}")
#             error_count += 1
    
#     if error_count > 0:
#         flash(f"ペアリングを実行しました！({success_count}/{len(players)}人成功, {error_count}人失敗) マッチID: {match_id}", "warning")
#     else:
#         flash(f"ペアリングを実行しました！({success_count}/{len(players)}人) マッチID: {match_id}", "success")
    
#     return redirect(url_for("game.pairings"))




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


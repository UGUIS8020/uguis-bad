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
    response = match_table.scan(
        FilterExpression="match_id = :pending",
        ExpressionAttributeValues={":pending": "pending"}
    )
    entries = response.get("Items", [])
    if len(entries) < 4:
        flash("4人以上のエントリーが必要です。", "danger")
        return redirect(url_for("index"))

    match_id = f"match_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    random.shuffle(entries)
    players = entries[:]

    # 休憩人数を計算（4人ずつでマッチを作るため）
    rest_count = len(players) % 4
    if rest_count:
        rest = players[-rest_count:]
        players = players[:-rest_count]
    else:
        rest = []

    matches = []
    court_number = 1  # コート番号を初期化

    matches = []
    court_number = 1  # コート番号を初期化

    for i in range(0, len(players), 4):  # 4人ごとに2vs2
        group = players[i:i + 4]
        if len(group) == 4:
            # 全員にmatch_idとcourt番号を割り当てて更新
            for p in group:
                match_table.update_item(
                    Key={"entry_id": p["entry_id"]},
                    UpdateExpression="SET match_id = :m, court = :c",
                    ExpressionAttributeValues={
                        ":m": match_id,
                        ":c": court_number
                    }
                )
            # matchesにcourt番号とプレイヤーのリストを格納
            matches.append({
                "court": court_number,
                "players": group
            })
            court_number += 1

    # 休憩者はmatch_idをセットしcourtは空にする（または削除）
    for p in rest:
        match_table.update_item(
            Key={"entry_id": p["entry_id"]},
            UpdateExpression="SET match_id = :m REMOVE court",
            ExpressionAttributeValues={":m": match_id}
        )

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
        # 既に参加中かどうか確認
        pending_response = match_table.scan(
            FilterExpression=Attr('user_id').eq(current_user.get_id()) & Attr('match_id').eq('pending')
        )
        
        if pending_response.get('Items'):
            flash("すでに参加登録されています", "info")
            return redirect(url_for("game.pairings"))
        
        # 休憩中かどうか確認
        resting_response = match_table.scan(
            FilterExpression=Attr('user_id').eq(current_user.get_id()) & Attr('match_id').eq('resting')
        )
        
        resting_items = resting_response.get('Items', [])
        
        if resting_items:
            # 休憩中のエントリーを参加中に戻す
            for item in resting_items:
                match_table.update_item(
                    Key={'entry_id': item['entry_id']},
                    UpdateExpression="SET match_id = :pending_status",
                    ExpressionAttributeValues={
                        ':pending_status': 'pending'
                    }
                )
            
            flash("休憩から復帰しました。引き続き参加します", "success")
            return redirect(url_for("game.pairings"))
        
        # 新規エントリー作成
        entry_id = str(uuid.uuid4())
        now = datetime.now().isoformat()
        
        # ユーザー情報からバドミントン経験を取得
        badminton_experience = getattr(current_user, 'badminton_experience', '不明')
        user_response = user_table.get_item(Key={"user#user_id": current_user.get_id()})
        skill_score = user_response.get("Item", {}).get("skill_score", 50)  # 見つからない場合は 50

        # エントリー登録に追加
        match_table.put_item(Item={
            'entry_id': entry_id,
            'user_id': current_user.get_id(),
            'match_id': "pending",
            'display_name': current_user.display_name,
            'badminton_experience': badminton_experience,
            'skill_score': skill_score,  # ← 追加
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
    """試合に参加登録する"""
    badminton_experience = request.form.get("badminton_experience")
    
    # 入力検証
    if not badminton_experience:
        flash("バドミントン経験を選択してください", "danger")
        return redirect(url_for("index"))
    
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
        "badminton_experience": badminton_experience,
        "joined_at": datetime.now().isoformat(),
        "match_id": "pending"  # 試合組み前はpendingステータス
    }
    
    match_table.put_item(Item=item)
    flash("参加登録しました！", "success")
    
    return redirect(url_for("game.pairings"))  # 統合ページへリダイレクト

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
    
    # テスト用の参加者データ
    test_players = [
        {'display_name': '田中太郎', 'badminton_experience': '初心者'},
        {'display_name': '佐藤花子', 'badminton_experience': '1年未満'},
        {'display_name': '鈴木一郎', 'badminton_experience': '1-2年'},
        {'display_name': '高橋美咲', 'badminton_experience': '3年以上'},
        {'display_name': '山田健太', 'badminton_experience': '1年未満'},
        {'display_name': '渡辺さくら', 'badminton_experience': '初心者'},
        {'display_name': '松本大輔', 'badminton_experience': '3年以上'},
        {'display_name': '中村実', 'badminton_experience': '1-2年'},
    ]
    
    try:
        # pendingステータスでテストデータを作成
        for i, player in enumerate(test_players):
            item = {
                'entry_id': str(uuid.uuid4()),
                'user_id': f'test_user_{i}',
                'display_name': player['display_name'],
                'badminton_experience': player['badminton_experience'],
                'joined_at': datetime.now().isoformat(),
                'match_id': "pending"  # pendingステータス
            }
            match_table.put_item(Item=item)
        
        flash(f'{len(test_players)}人のテストデータを作成しました！', 'success')
        
    except Exception as e:
        flash(f'テストデータ作成に失敗: {e}', 'danger')
    
    return redirect(url_for('game.pairings'))  # 統合ページへリダイレクト

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


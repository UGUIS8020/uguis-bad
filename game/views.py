from flask import Blueprint, render_template, redirect, url_for, flash
from flask_login import login_required, current_user
import boto3
import uuid
from datetime import datetime
import random

bp_game = Blueprint('game', __name__)

# DynamoDBリソース取得
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
match_table = dynamodb.Table('match_entries')


@bp_game.route('/game/join_match')
@login_required
def join_match():
    item = {
        'entry_id': str(uuid.uuid4()),
        'user_id': current_user.id,
        'display_name': current_user.display_name,
        'badminton_experience': getattr(current_user, 'badminton_experience', '不明'),
        'joined_at': datetime.now().isoformat(),
        'match_id': 'pending'
    }
    try:
        match_table.put_item(Item=item)
        flash('試合にエントリーしました！', 'success')
    except Exception as e:
        flash(f'登録に失敗しました: {e}', 'danger')
    return redirect(url_for('index'))

@bp_game.route('/game/start_match')
@login_required
def start_match():
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
    display_names = [p['display_name'] for p in players]

    # 休憩人数を計算
    rest_count = len(players) % 6
    if rest_count:
        rest = players[-rest_count:]
        players = players[:-rest_count]
    else:
        rest = []

    matches = []
    for i in range(0, len(players), 4):  # 4人ごとに2vs2
        group = players[i:i + 4]
        if len(group) == 4:
            matches.append({
                "team1": [group[0]['display_name'], group[1]['display_name']],
                "team2": [group[2]['display_name'], group[3]['display_name']]
            })

    # 全員のmatch_idを更新
    for p in players + rest:
        match_table.update_item(
            Key={"entry_id": p["entry_id"]},
            UpdateExpression="SET match_id = :m",
            ExpressionAttributeValues={":m": match_id}
        )

    return render_template("game/pairings.html", matches=matches, rest=rest, match_id=match_id)

import random
from datetime import datetime

@bp_game.route("/game/pairings")
@login_required
def pairings():
    match_id = generate_match_id()
    now = datetime.now().isoformat()

    # エントリー済みユーザーを取得（必要に応じて match_id フィルタを追加）
    response = match_table.scan()
    entries = response.get("Items", [])
    users = [{"user_id": e["user_id"], "display_name": e["display_name"]} for e in entries]

    random.shuffle(users)
    pairs = [users[i:i + 2] for i in range(0, len(users), 2)]
    matches = []
    rest = None

    if len(users) % 2 == 1:
        rest = pairs.pop()

    for idx, pair in enumerate(pairs):
        team_id = f"Team{idx + 1}"
        if len(pair) == 2:
            matches.append({
                "team1": pair[0]["display_name"],
                "team2": pair[1]["display_name"]
            })
        for member in pair:
            item = {
                'match_id': match_id,
                'entry_id': str(uuid.uuid4()),
                'user_id': member["user_id"],
                'display_name': member["display_name"],
                'team_id': team_id,
                'joined_at': now,
            }
            match_table.put_item(Item=item)

    return render_template("game/pairings.html", matches=matches, rest=rest)

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
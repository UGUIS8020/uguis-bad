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


@bp_game.route('/game/join')
@login_required
def join_match():
    item = {
        'entry_id': str(uuid.uuid4()),
        'user_id': current_user.id,
        'display_name': current_user.display_name,
        'badminton_experience': getattr(current_user, 'badminton_experience', '不明'),
        'joined_at': datetime.now().isoformat(),
    }
    try:
        match_table.put_item(Item=item)
        flash('試合に参加しました！', 'success')
    except Exception as e:
        flash(f'登録に失敗しました: {e}', 'danger')
    return redirect(url_for('game.show_pairings'))

@bp_game.route("/game/pairings")
@login_required
def pairings():
    response = match_table.scan()
    users = [item["display_name"] for item in response.get("Items", [])]

    random.shuffle(users)
    pairs = [users[i:i + 2] for i in range(0, len(users), 2)]

    matches = []
    rest = None

    if len(pairs) % 2 == 1:
        rest = pairs.pop()

    for i in range(0, len(pairs), 2):
        if i + 1 < len(pairs):
            matches.append({
                "team1": pairs[i],
                "team2": pairs[i + 1]
            })

    return render_template("game/pairings.html", matches=matches, rest=rest)
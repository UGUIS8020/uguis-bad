from flask import request, current_app, flash, redirect, url_for
from datetime import datetime
import uuid
import pytz



#勝敗を評価し点数を加える
def update_individual_win_loss(winning_team: list, losing_team: list):
    user_table = current_app.dynamodb.Table("bad-users")

    for player in winning_team:
        user_id = player["user_id"]
        user_table.update_item(
            Key={"user#user_id": user_id},
            UpdateExpression="ADD wins :inc",
            ExpressionAttributeValues={":inc": 1}
        )

    for player in losing_team:
        user_id = player["user_id"]
        user_table.update_item(
            Key={"user#user_id": user_id},
            UpdateExpression="ADD losses :inc",
            ExpressionAttributeValues={":inc": 1}
        )
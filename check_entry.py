# 一時的なデバッグスクリプト check_entry.py
import boto3

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')

user_id = "確認したいuser_id"

# match_entries を確認
table = dynamodb.Table('bad-game-match_entries')
res = table.scan(
    FilterExpression="user_id = :uid",
    ExpressionAttributeValues={":uid": user_id}
)
print("match_entries:", res['Items'])

# participants テーブルがあれば確認
table2 = dynamodb.Table('bad-game-participants')  # テーブル名を合わせてください
res2 = table2.scan(
    FilterExpression="user_id = :uid",
    ExpressionAttributeValues={":uid": user_id}
)
print("participants:", res2['Items'])
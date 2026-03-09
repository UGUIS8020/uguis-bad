import boto3

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
table = dynamodb.Table('bad-game-match_entries')

table.delete_item(Key={"entry_id": "ff83da5b-825e-47df-ae35-6dd77e76031f"})
print("削除完了")
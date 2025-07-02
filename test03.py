import boto3
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
match_table = dynamodb.Table('bad-game-match_entries')

user_id = '4c7f822d-ff39-4797-9b7b-8ebc205490f5'

response = match_table.scan(
    FilterExpression=Attr('user_id').eq(user_id) & Attr('entry_status').eq('active')
)

for item in response['Items']:
    entry_id = item['entry_id']
    print(f"🗑️ 削除中: entry_id={entry_id}, joined_at={item['joined_at']}")
    match_table.delete_item(Key={'entry_id': entry_id})
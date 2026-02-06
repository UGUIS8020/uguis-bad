import boto3
from boto3.dynamodb.conditions import Attr

# DynamoDB接続
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
history_table = dynamodb.Table('bad-users-history')

# action='tara_join' で status が None または 'registered' のレコードを修正
response = history_table.scan(
    FilterExpression=Attr('action').eq('tara_join')
)

items = response.get('Items', [])
fixed_count = 0

for item in items:
    status = item.get('status')
    
    # status が None, 空文字, または 'registered' の場合
    if not status or status == 'registered':
        user_id = item['user_id']
        joined_at = item['joined_at']
        
        print(f"修正: user_id={user_id}, date={item.get('date')}, joined_at={joined_at}")
        
        # status を 'tentative' に更新
        history_table.update_item(
            Key={'user_id': user_id, 'joined_at': joined_at},
            UpdateExpression='SET #st = :tentative',
            ExpressionAttributeNames={'#st': 'status'},
            ExpressionAttributeValues={':tentative': 'tentative'}
        )
        fixed_count += 1

print(f"\n修正完了: {fixed_count}件")
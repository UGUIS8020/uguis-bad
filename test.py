import boto3
from collections import Counter

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
table = dynamodb.Table('bad-game-history')

items = []
resp = table.scan()
items.extend(resp.get('Items', []))
while 'LastEvaluatedKey' in resp:
    resp = table.scan(ExclusiveStartKey=resp['LastEvaluatedKey'])
    items.extend(resp.get('Items', []))

items.sort(key=lambda x: x.get('match_id', ''))

dates = Counter(i.get('match_id', '')[:8] for i in items)

print(f"総レコード数: {len(items)}")
print(f"最古: {items[0].get('match_id') if items else 'なし'}")
print(f"最新: {items[-1].get('match_id') if items else 'なし'}")
print()
print("日付別件数:")
for d, cnt in sorted(dates.items()):
    print(f"  {d}: {cnt}件")
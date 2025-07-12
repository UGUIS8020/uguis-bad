import boto3

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
table = dynamodb.Table('bad-users')

# スキャン件数を増やす
response = table.scan(Limit=100)

items = response.get('Items', [])
attribute_names = set()

for item in items:
    attribute_names.update(item.keys())

print("✅ bad-users テーブルに含まれる属性（項目）一覧:")
for attr in sorted(attribute_names):
    print(f"- {attr}")
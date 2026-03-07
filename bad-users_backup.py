import boto3
import json
from decimal import Decimal

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
table = dynamodb.Table('bad-users')

def backup_to_json():
    items = []
    response = table.scan()
    items.extend(response.get('Items', []))
    
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items', []))

    # Decimal を float に変換
    def decimal_default(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError

    with open('bad-users-backup.json', 'w', encoding='utf-8') as f:
        json.dump(items, f, ensure_ascii=False, indent=2, default=decimal_default)

    print(f"バックアップ完了: {len(items)}件 → bad-users-backup.json")

backup_to_json()
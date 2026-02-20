import boto3

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
table = dynamodb.Table('bad-users')

def delete_old_items():
    response = table.scan()
    items = response.get('Items', [])
    
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items', []))

    deleted = 0
    for item in items:
        pk = item.get("user#user_id", "")
        if not str(pk).startswith("user#"):
            table.delete_item(Key={"user#user_id": pk})
            print(f"🗑️  削除: {pk}")
            deleted += 1

    print(f"\n削除完了: {deleted}件")

delete_old_items()
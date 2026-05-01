import boto3

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
table = dynamodb.Table('bad-game-results')

TEST_KEYWORDS = ['テスト', '悟空', 'キャメロン', 'ロバート', 'ノーマン']

def is_test_record(item):
    for k in ['team_a', 'team_b']:
        for p in item.get(k, []):
            name = p.get('display_name', '')
            if any(kw in name for kw in TEST_KEYWORDS):
                return True
    return False

# 全件取得
items = []
resp = table.scan()
items.extend(resp.get('Items', []))
while 'LastEvaluatedKey' in resp:
    resp = table.scan(ExclusiveStartKey=resp['LastEvaluatedKey'])
    items.extend(resp.get('Items', []))

test_items = [i for i in items if is_test_record(i)]
print(f"削除対象: {len(test_items)}件")

# 25件ずつバッチ削除
deleted = 0
for i in range(0, len(test_items), 25):
    batch = test_items[i:i+25]
    with table.batch_writer() as bw:
        for item in batch:
            bw.delete_item(Key={'result_id': item['result_id']})
    deleted += len(batch)
    print(f"  削除済み: {deleted}/{len(test_items)}")

print(f"削除完了: {deleted}件")
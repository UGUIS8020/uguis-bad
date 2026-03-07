import boto3
from decimal import Decimal

# DynamoDBリソースの初期化
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
table = dynamodb.Table('bad-users')

print("既存ユーザーにskill_sigmaを追加します...")

# 全ユーザーをスキャン
response = table.scan()
items = response.get('Items', [])

print(f"対象ユーザー数: {len(items)}")

updated_count = 0
for item in items:
    user_id = item.get('user#user_id')
    
    # 既にskill_sigmaがある場合はスキップ
    if 'skill_sigma' in item:
        print(f"スキップ: {user_id} (既にskill_sigmaあり)")
        continue
    
    try:
        # TrueSkillのデフォルト: sigma = 25.0 / 3.0 = 8.333
        table.update_item(
            Key={'user#user_id': user_id},
            UpdateExpression='SET skill_sigma = :sigma',
            ExpressionAttributeValues={
                ':sigma': Decimal('8.333')
            }
        )
        updated_count += 1
        print(f"更新: {user_id} - skill_sigma=8.333")
    except Exception as e:
        print(f"エラー: {user_id} - {str(e)}")

print(f"\n完了: {updated_count}件のユーザーを更新しました")

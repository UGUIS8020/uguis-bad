import boto3
from decimal import Decimal

# 本番と同じ認証設定に合わせてください
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
table = dynamodb.Table('bad-users')

def migrate():
    # 全アイテムをスキャン
    response = table.scan()
    items = response.get('Items', [])
    
    # ページネーション対応
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items', []))

    print(f"総アイテム数: {len(items)}")

    old_items = []  # abc123 形式
    new_items = []  # user#abc123 形式

    for item in items:
        pk = item.get("user#user_id", "")
        if str(pk).startswith("user#"):
            new_items.append(item)
        else:
            old_items.append(item)

    print(f"旧形式（abc123）: {len(old_items)}件")
    print(f"新形式（user#abc123）: {len(new_items)}件")

    merged = 0
    skipped = 0

    for old in old_items:
        old_pk = old.get("user#user_id")
        new_pk = f"user#{old_pk}"

        # 新形式のアイテムを探す
        new = next((n for n in new_items if n.get("user#user_id") == new_pk), None)

        if new:
            # 新アイテムに旧アイテムのフィールドをマージ（新側を優先）
            merged_item = {**old, **new}
            merged_item["user#user_id"] = new_pk  # PKは新形式に統一

            table.put_item(Item=merged_item)
            print(f"✅ マージ完了: {old_pk} → {new_pk} / skill_score={merged_item.get('skill_score')}")
            merged += 1
        else:
            # 新形式がない場合は旧アイテムをuser#付きでコピー
            migrated_item = dict(old)
            migrated_item["user#user_id"] = new_pk

            table.put_item(Item=migrated_item)
            print(f"📋 コピー完了: {old_pk} → {new_pk}")
            merged += 1

    print(f"\n完了: {merged}件移行, {skipped}件スキップ")
    print("⚠️  旧アイテム（abc123形式）はまだ残っています。動作確認後に手動削除してください。")

if __name__ == "__main__":
    migrate()
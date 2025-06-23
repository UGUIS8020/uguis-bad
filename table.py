import boto3
from boto3.dynamodb.conditions import Key, Attr

# テーブル名
TABLE_NAME = "bad-game-match_entries"
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
table = dynamodb.Table(TABLE_NAME)

# 全件スキャンして entry_status を追加し、badminton_experience を削除
def update_entries():
    print(f"Updating entries in table: {TABLE_NAME}")
    
    # 1. 全件取得
    response = table.scan()
    items = response.get('Items', [])

    print(f"Found {len(items)} items to update.")

    # 2. 1件ずつ処理
    for item in items:
        entry_id = item.get('entry_id')
        if not entry_id:
            continue

        update_expr = []
        expr_attr_values = {}
        expr_attr_names = {}

        # entry_status を追加（存在しなければ）
        if 'entry_status' not in item:
            update_expr.append("SET #status = :status")
            expr_attr_values[":status"] = "active"
            expr_attr_names["#status"] = "entry_status"

        # badminton_experience を削除
        if 'badminton_experience' in item:
            update_expr.append("REMOVE #experience")
            expr_attr_names["#experience"] = "badminton_experience"

        if update_expr:
            update_expression = " ".join(update_expr)
            print(f"Updating {entry_id} with: {update_expression}")
            table.update_item(
                Key={"entry_id": entry_id},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expr_attr_values or None,
                ExpressionAttributeNames=expr_attr_names
            )

if __name__ == "__main__":
    update_entries()
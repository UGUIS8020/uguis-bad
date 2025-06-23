import boto3
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")
match_entries_table = dynamodb.Table("bad-game-match_entries")

def update_entries():
    last_evaluated_key = None
    while True:
        if last_evaluated_key:
            response = match_entries_table.scan(ExclusiveStartKey=last_evaluated_key)
        else:
            response = match_entries_table.scan()
        
        items = response.get("Items", [])
        
        for item in items:
            entry_id = item.get("entry_id")
            if not entry_id:
                print("entry_idがありません。スキップします。", item)
                continue

            update_expr = "SET court = :court REMOVE badminton_experience"
            expr_attr_values = {":court": ""}
            
            try:
                match_entries_table.update_item(
                    Key={"entry_id": entry_id},
                    UpdateExpression=update_expr,
                    ExpressionAttributeValues=expr_attr_values
                )
                print(f"✅ 更新成功: {entry_id}")
            except Exception as e:
                print(f"❌ 更新失敗: {entry_id}, エラー: {e}")
        
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break

if __name__ == "__main__":
    update_entries()
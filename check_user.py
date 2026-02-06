# check_table_schema.py
import sys
sys.path.insert(0, '.')
from uguu.dynamo import DynamoDB

db = DynamoDB()

# テーブルのキー構造を確認
table_desc = db.part_history.meta.client.describe_table(
    TableName='bad-users-history'
)

print("=== bad-users-history テーブル構造 ===")
print("\nキースキーマ:")
for key in table_desc['Table']['KeySchema']:
    print(f"  {key['AttributeName']}: {key['KeyType']}")

print("\n属性定義:")
for attr in table_desc['Table']['AttributeDefinitions']:
    print(f"  {attr['AttributeName']}: {attr['AttributeType']}")
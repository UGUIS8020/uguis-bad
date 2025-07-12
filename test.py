import boto3
from boto3.dynamodb.conditions import Key
from pprint import pprint



def inspect_dynamodb_tables():
    # 明示的にリージョンを指定
    dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
    client = boto3.client('dynamodb', region_name='ap-northeast-1')

    # 1. テーブル一覧取得
    response = client.list_tables()
    table_names = response['TableNames']
    print(f"\n🔍 テーブル一覧: {table_names}\n")

    for name in table_names:
        print(f"📘 テーブル: {name}")
        table = dynamodb.Table(name)

        # 2. テーブルのキー構成を取得
        description = client.describe_table(TableName=name)
        key_schema = description['Table']['KeySchema']
        attribute_definitions = description['Table']['AttributeDefinitions']

        print("🔑 主キー構成:")
        for key in key_schema:
            attr_type = next(
                (attr['AttributeType'] for attr in attribute_definitions if attr['AttributeName'] == key['AttributeName']),
                'N/A'
            )
            print(f"  - {key['KeyType']}: {key['AttributeName']} ({attr_type})")

        # 3. 最初の数件のデータを表示
        try:
            sample = table.scan(Limit=3)
            print("\n🧾 サンプルデータ（最大3件）:")
            for item in sample.get('Items', []):
                pprint(item)
        except Exception as e:
            print(f"⚠️ データ取得エラー: {e}")

        print("-" * 60)

# 実行
if __name__ == "__main__":
    inspect_dynamodb_tables()
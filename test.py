import boto3
from boto3.dynamodb.conditions import Key, Attr

def check_permissions():
    # DynamoDBリソースを初期化
    dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
    
    try:
        # テーブルの情報を取得
        table = dynamodb.Table('match_entries')
        table_info = table.meta.client.describe_table(TableName='match_entries')
        print("テーブル情報: 取得成功")
        
        # テーブルへの直接アクセスをテスト
        scan_result = table.scan(Limit=1)
        print("Scanテスト: 成功")
        
        # インデックスへのアクセスをテスト
        try:
            query_result = table.query(
                IndexName='UserMatchIndex',
                KeyConditionExpression=Key('user_id').eq('test_user_1')
            )
            print("UserMatchIndexテスト: 成功")
        except Exception as e:
            print("UserMatchIndexテスト: 失敗")
            print(f"エラー: {str(e)}")
        
        try:
            query_result = table.query(
                IndexName='MatchIndex',
                KeyConditionExpression=Key('match_id').eq('pending')
            )
            print("MatchIndexテスト: 成功")
        except Exception as e:
            print("MatchIndexテスト: 失敗")
            print(f"エラー: {str(e)}")
            
    except Exception as e:
        print(f"テーブルアクセステスト失敗: {str(e)}")

if __name__ == "__main__":
    check_permissions()
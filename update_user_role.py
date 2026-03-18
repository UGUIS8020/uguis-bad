import boto3
from werkzeug.security import generate_password_hash

# --- 設定 ---
REGION = "ap-northeast-1"
TABLE_NAME = "bad-users"
# テスト用アカウントID
TARGET_USER_ID = "da8582cb-26d6-4776-a8e7-aa6fe38ac8a4"

# パスワードをハッシュ化
raw_password = "00000000"
hashed_password = generate_password_hash(raw_password, method='pbkdf2:sha256:1000000')

dynamodb = boto3.resource('dynamodb', region_name=REGION)
table = dynamodb.Table(TABLE_NAME)

def setup_test_admin_with_hash(user_id):
    print(f"ID: {user_id} をハッシュ化パスワードで更新中...")
    
    try:
        response = table.update_item(
            Key={
                'user#user_id': user_id
            },
            UpdateExpression="""
                SET display_name = :dname,
                    email = :email,
                    password = :pw,
                    organization = :org, 
                    team_id = :tid, 
                    #r = :role_val, 
                    administrator = :admin_val
            """,
            ExpressionAttributeNames={
                "#r": "role"
            },
            ExpressionAttributeValues={
                ":dname": "てすと999まさひこ",
                ":email": "test999@test.com",
                ":pw": hashed_password,     # ハッシュ化した値をセット
                ":org": "かわせみバドミントン倶楽部",
                ":tid": "t999",
                ":role_val": "admin",
                ":admin_val": False
            },
            ReturnValues="UPDATED_NEW"
        )
        print("✅ ハッシュ化パスワードでの更新が完了しました！")
        print(f"設定パスワード: {raw_password}")
        print("更新された属性:", response.get('Attributes'))
        
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")

if __name__ == "__main__":
    setup_test_admin_with_hash(TARGET_USER_ID)
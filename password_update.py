import os
from dotenv import load_dotenv
import boto3
from werkzeug.security import generate_password_hash

load_dotenv()

AWS_REGION = "ap-northeast-1"   # ← ここを固定
TABLE_NAME = "bad-users"        # ← コンソールと同じ名前に固定

# 認証情報は .env から
session = boto3.session.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=AWS_REGION,
)

dynamodb = session.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)

user_id = "4c7f822d-ff39-4797-9b7b-8ebc205490f5"

# 更新前を確認
before = table.get_item(Key={"user#user_id": user_id})
print("=== BEFORE ===")
print(before)

# 新パスワードをハッシュ化（00000000）
new_password = "00000000"
hashed_password = generate_password_hash(new_password, method="pbkdf2:sha256")
print("=== NEW HASH ===")
print(hashed_password)

# 更新
resp = table.update_item(
    Key={"user#user_id": user_id},
    UpdateExpression="SET password = :p",
    ExpressionAttributeValues={":p": hashed_password},
    ReturnValues="ALL_NEW",
)

print("=== AFTER ===")
print(resp)
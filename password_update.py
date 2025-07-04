import boto3
from werkzeug.security import generate_password_hash

# DynamoDBクライアント作成
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
table = dynamodb.Table('bad-users')

# 新しいパスワード
new_password = "12345678"
hashed_password = generate_password_hash(new_password, method='pbkdf2:sha256')

# 直接user_idを指定
user_id = "913bad6e-b79f-444d-b414-12341ce05a25"  # 使いたいuser_idをここに

# パスワード更新
table.update_item(
    Key={
        'user#user_id': user_id
    },
    UpdateExpression='SET password = :password',
    ExpressionAttributeValues={
        ':password': hashed_password
    }
)
print(f"パスワードを更新しました: {user_id}")
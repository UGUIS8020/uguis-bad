from app import db, app
from app import User  # Userモデルをインポート
from datetime import datetime, date
from werkzeug.security import generate_password_hash
import boto3
import os
import uuid

# DynamoDBクライアントの設定
dynamodb = boto3.client(
    'dynamodb',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

# テーブル名を環境変数から取得
table_name = os.getenv("TABLE_NAME_USER")

with app.app_context():
    # すべてのテーブルをドロップ
    db.drop_all()
    print("Dropped all existing tables.")
    
    # すべてのテーブルを再作成
    db.create_all()
    print("Created all tables.")

    # テストユーザーの作成
    test_user = User(
        display_name="UGUIS",
        user_name="渋谷　正彦",
        furigana="シブヤ　マサヒコ",
        email="shibuyamasahiko@gmail.com",
        password=generate_password_hash("giko8020@Z"),  # パスワードをハッシュ化
        gender="male",
        date_of_birth=date(1971, 11, 20),  # 生年月日を設定
        post_code="3430845",
        address="埼玉県越谷市南越谷4-9-6　新越谷プラザビル201",
        phone="07066330363",
        administrator=True  # 管理者権限を付与
    )

    # SQLデータベースに追加
    db.session.add(test_user)
    
    # DynamoDBに追加
    try:
        db.session.commit()
        
        response = dynamodb.put_item(
            TableName=table_name,
            Item={
                "user_id": {"S": str(uuid.uuid4())},  # ユニークID
                "display_name": {"S": test_user.display_name},
                "user_name": {"S": test_user.user_name},
                "furigana": {"S": test_user.furigana},
                "email": {"S": test_user.email},
                "password": {"S": test_user.password},  # ハッシュ化されたパスワード
                "gender": {"S": test_user.gender},
                "date_of_birth": {"S": test_user.date_of_birth.strftime('%Y-%m-%d')},  # 生年月日を文字列に変換
                "post_code": {"S": test_user.post_code},
                "address": {"S": test_user.address},
                "phone": {"S": test_user.phone},
                "administrator": {"BOOL": test_user.administrator},  # 管理者フラグ
                "created_at": {"S": datetime.now().isoformat()},
                "updated_at": {"S": datetime.now().isoformat()}
            }
        )
        
        print("Test user created successfully in SQL database and DynamoDB!")
        print("\nTest User Details:")
        print(f"Email: {test_user.email}")
        print(f"Display Name: {test_user.display_name}")
        print(f"User Name: {test_user.user_name}")
        print(f"Administrator: {test_user.administrator}")
        
    except Exception as e:
        db.session.rollback()
        print(f"Error creating test user: {str(e)}")

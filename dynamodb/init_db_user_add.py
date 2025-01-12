from app import db, app
from app import User  # Userモデルをインポート
from datetime import datetime, date
from werkzeug.security import generate_password_hash

with app.app_context():
    # すべてのテーブルをドロップ
    db.drop_all()
    print("Dropped all existing tables.")
    
    # すべてのテーブルを再作成
    db.create_all()
    print("Created all tables.")

    # テストユーザーの作成
    test_user = User(
        user_id=item
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

    # データベースに追加
    db.session.add(test_user)
    try:
        db.session.commit()
        print("Test user created successfully!")
        print("\nTest User Details:")
        print(f"Email: test@example.com")
        print(f"Password: Test1234")
        print(f"Display Name: {test_user.display_name}")
        print(f"User Name: {test_user.user_name}")
        print(f"Administrator: {test_user.administrator}")
    except Exception as e:
        db.session.rollback()
        print(f"Error creating test user: {str(e)}")
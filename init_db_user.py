from app import db, app

with app.app_context():
    # すべてのテーブルをドロップ
    db.drop_all()
    
    # すべてのテーブルを再作成
    db.create_all()

    print("Database tables have been dropped and recreated successfully.")
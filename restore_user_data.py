import boto3
import os
from dotenv import load_dotenv
from datetime import datetime
from werkzeug.security import generate_password_hash

load_dotenv()

# AWS認証
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION", "ap-northeast-1")
)

users_table = dynamodb.Table('bad-users')

def restore_specific_user():
    """特定ユーザーのデータを復元"""
    
    user_id = '4c7f822d-ff39-4797-9b7b-8ebc205490f5'
    
    print(f"ユーザーID: {user_id} のデータを復元します")
    print("=" * 60)
    
    # 復元するデータ
    restored_data = {
        'user#user_id': user_id",
        'display_name': 'UGUIS渋谷',
        'user_name': '渋谷まさひこ',
        'email': 'shibuyamasahiko@gmail.com',
        'date_of_birth': '1971-11-20',  # YYYY-MM-DD形式に変換
        'password_hash': generate_password_hash('00000000'),
        'administrator': True,
        'bio': '',
        'skill_score': 0,
        'created_at': '2025-01-01T00:00:00',  # 適当な作成日
        'updated_at': datetime.now().isoformat(),
    }
    
    print("\n復元するデータ:")
    print("=" * 60)
    for key, value in restored_data.items():
        if key == 'password_hash':
            print(f"  {key}: [ハッシュ化済み]")
        else:
            print(f"  {key}: {value}")
    
    confirm = input("\nこのデータで復元しますか？ (yes/no): ").strip().lower()
    
    if confirm == 'yes':
        try:
            users_table.put_item(Item=restored_data)
            print("\nユーザーデータの復元が完了しました！")
            print("\nログイン情報:")
            print(f"  メールアドレス: {restored_data['email']}")
            print(f"  パスワード: 00000000")
        except Exception as e:
            print(f"\n❌ エラーが発生しました: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("\n復元をキャンセルしました。")

if __name__ == "__main__":
    restore_specific_user()
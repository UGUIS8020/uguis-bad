from app import app
import uuid
from werkzeug.security import generate_password_hash
from datetime import date
import logging

logger = logging.getLogger(__name__)

# ユーザーIDを生成
user_id = str(uuid.uuid4())

# 現在の日時
now = date.today().isoformat()

# ユーザー情報
user_item = {
    'user#user_id': user_id,  # プライマリキー
    'display_name': "UGUIS.渋谷",
    'user_name': "渋谷　正彦",
    'furigana': "シブヤ　マサヒコ",
    'email': "shibuyamasahiko@gmail.com",
    'password': generate_password_hash("giko8020@Z", method='scrypt'),
    'gender': "male",
    'date_of_birth': "1971-11-20",
    'post_code': "3430845",
    'address': "埼玉県越谷市南越谷4-9-6　新越谷プラザビル201",
    'phone': "07066330363",
    'organization': "Boot_Camp15",
    'badminton_experience': "3年以上",
    'guardian_name': "渋谷　俊春",
    'emergency_phone': "07066330000",
    'administrator': True,
    'created_at': now,
    'updated_at': now
}

try:
    app.table.put_item(Item=user_item)
except Exception as e:
    # エラー時のみログ（本番ではWARNING以上が推奨）
    logger.warning(f"管理者ユーザー作成エラー: {str(e)}")
from werkzeug.security import generate_password_hash

# 新しいパスワード（自由に変更可）
new_password = "12345678"

# ハッシュ生成
hashed_password = generate_password_hash(new_password, method='pbkdf2:sha256')
print(hashed_password)
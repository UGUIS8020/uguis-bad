import boto3
from decimal import Decimal

# --- 設定 ---
REGION = "ap-northeast-1"
TABLE_NAME = "bad-users"

# DynamoDB接続
dynamodb = boto3.resource('dynamodb', region_name=REGION)
table = dynamodb.Table(TABLE_NAME)

def search_user_details(target_name):
    print(f"\n🔎 名前 '{target_name}' を詳細検索中...")
    
    # 全件スキャンして名前でフィルタリング
    # (本番環境で巨大なテーブルには向きませんが、数千件程度ならこれで確実です)
    response = table.scan()
    items = response.get('Items', [])
    
    matches = [item for item in items if target_name in item.get('display_name', '')]
    
    if not matches:
        print(f"❌ '{target_name}' に一致するデータは見つかりませんでした。")
        return

    print(f"✅ {len(matches)} 件のデータが見つかりました：\n")
    print("-" * 70)
    
    for i, item in enumerate(matches):
        print(f"【検索結果 {i+1}】")
        
        # IDの取得と型の判定
        uid = item.get('user_id') or item.get('User_id')
        uid_type = type(uid).__name__
        
        # 重要な属性をピックアップ
        display_name = item.get('display_name', '不明')
        skill_score = item.get('skill_score', 'なし')
        email = item.get('email', 'なし')
        role = item.get('role', 'なし')
        created_at = item.get('created_at', 'なし')

        print(f"  表示名    : {display_name}")
        print(f"  ID (Key)  : {uid} (型: {uid_type})")
        print(f"  戦闘力    : {skill_score}")
        print(f"  権限      : {role}")
        print(f"  作成日    : {created_at}")
        print(f"  登録メール: {email}")
        
        # その他すべての属性（デバッグ用）
        print(f"  全データ  : {item}")
        print("-" * 70)

def main():
    while True:
        name = input("\n検索したい名前を入力してください（終了は q）: ")
        if name.lower() == 'q':
            break
        search_user_details(name)

if __name__ == "__main__":
    main()
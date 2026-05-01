import boto3
from decimal import Decimal

# --- 設定 ---
REGION = "ap-northeast-1"
TABLE_NAME = "bad-users"

# DynamoDB接続
dynamodb = boto3.resource('dynamodb', region_name=REGION)
table = dynamodb.Table(TABLE_NAME)

def get_all_items():
    """
    1MBの制限を超えて、テーブルの全データを取得する関数
    """
    items = []
    response = table.scan()
    items.extend(response.get('Items', []))

    # LastEvaluatedKeyがある限り、繰り返し取得を続ける
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items', []))
    
    return items

def search_anywhere(target_name):
    print(f"\n🔎 キーワード '{target_name}' を全項目から検索中...")
    
    # 1. まず全データを取得
    all_items = get_all_items()
    
    # 2. 検索キーワードを小文字化（大文字小文字を区別しないため）
    search_term = target_name.lower()
    
    matches = []
    for item in all_items:
        # item.values() で全カラムの値を取得
        # str(val) で数値やリストも文字列に変換し、どこかに含まれているか判定
        found = False
        for val in item.values():
            if search_term in str(val).lower():
                found = True
                break
        
        if found:
            matches.append(item)

    # 3. 結果表示
    if not matches:
        print(f"❌ '{target_name}' を含むデータは、どの項目（名前、メール、ID、権限等）にも見つかりませんでした。")
        return

    print(f"✅ {len(matches)} 件のデータが見つかりました：\n")
    print("-" * 75)
    
    for i, item in enumerate(matches):
        print(f"【検索結果 {i+1}】")
        
        # 重要な情報を優先表示
        display_name = item.get('display_name', '---')
        email = item.get('email', '---')
        role = item.get('role', '---')
        uid = item.get('user_id') or item.get('User_id') or '---'

        print(f"  表示名    : {display_name}")
        print(f"  ユーザーID: {uid}")
        print(f"  メール    : {email}")
        print(f"  現在の権限: {role}")
        
        # どの項目にヒットしたか分かりやすくするため、全データも出力
        print(f"  全属性データ: {item}")
        print("-" * 75)

def main():
    print(f"🚀 DynamoDB ({TABLE_NAME}) ディープスキャン・ツール")
    while True:
        name = input("\n検索したいキーワードを入力（終了は q）: ").strip()
        if not name:
            continue
        if name.lower() == 'q':
            break
        search_anywhere(name)

if __name__ == "__main__":
    main()
import boto3
from boto3.dynamodb.conditions import Attr

# --- 設定 ---
REGION = "ap-northeast-1"
TRASH_KEYWORDS = ["テスト", "test", "悟空", "ロバート", "ノーマン", "キャメロン", "ささき"]

# DynamoDB接続
dynamodb = boto3.resource('dynamodb', region_name=REGION)
user_table = dynamodb.Table("bad-users")
match_entry_table = dynamodb.Table("bad-game-match_entries")
result_table = dynamodb.Table("bad-game-results")

def search_targets():
    print("🔍 厳選した削除候補を検索中...")
    users = user_table.scan().get("Items", [])
    targets = []
    
    # 明らかなダミーリスト
    dummy_names = ["悟空", "ロバート", "ノーマン", "キャメロン", "ささき", "名前なし"]
    
    for u in users:
        name = u.get("display_name", "")
        email = u.get("email", "")
        
        # 条件1: 名前が「テスト」を含む
        if any(kw in name for kw in ["テスト", "test"]):
            targets.append(u)
            continue
            
        # 条件2: 特定のダミー名かつ、メールが不自然な場合
        if name in dummy_names:
            # メールの形式や有無で判断（本物ならちゃんとしたアドレスがあるはず）
            if not email or "@example.com" in email or "test" in email:
                targets.append(u)
                
    return targets

def main():
    targets = search_targets()
    
    if not targets:
        print("✅ 削除候補のテストユーザーは見つかりませんでした。")
        return

    print(f"\n📋 以下の {len(targets)} 名が見つかりました:")
    print("-" * 60)
    for i, u in enumerate(targets):
        name = u.get('display_name', '名前なし')
        uid = u.get('user_id')
        uid_display = f"{uid[:8]}..." if uid else "ID欠損(ゴミ)"
        print(f"[{i:3}] 名前: {name:15} | ID: {uid_display}")
    print("-" * 60)

    # 選択
    choice = input("\n削除したい番号をカンマ区切りで入力、'all' で全員選択、または 'q' で終了: ")
    
    if choice.lower() == 'q':
        return
        
    to_delete = []
    if choice.lower() == 'all':
        to_delete = targets
    else:
        try:
            indices = [int(x.strip()) for x in choice.split(',')]
            to_delete = [targets[i] for i in indices]
        except Exception:
            print("❌ 入力が正しくありません。")
            return

    confirm = input(f"\n⚠️  {len(to_delete)} 名のデータと関連する試合結果を削除します。よろしいですか？ (yes/no): ")
    if confirm.lower() != 'yes':
        print("中止しました。")
        return

    print("\n🧹 クリーンアップ開始...")
    
    # 高速化のため、全Resultsを一度だけ取得
    print("📦 試合結果データを読み込み中（時間がかかる場合があります）...")
    all_results = result_table.scan().get("Items", [])

    for u in to_delete:
        uid = u.get('user_id')
        name = u.get('display_name', '名前なし')
        
        if not uid:
            # IDがないゴミデータは、他に紐付くデータがないため、スキャンして見つけた属性で消す必要がある場合がありますが、
            # 通常はKey（user_id）がないとdelete_itemできません。ここではスキップするか、特殊な処理が必要です。
            print(f"⏩ ID欠損データのためスキップしました: {name}")
            continue

        # 1. ユーザー削除
        user_table.delete_item(Key={'user_id': uid})
        
        # 2. 進行中エントリー削除
        entries = match_entry_table.scan(FilterExpression=Attr("user_id").eq(uid)).get("Items", [])
        for e in entries:
            match_entry_table.delete_item(Key={'entry_id': e['entry_id']})
            
        # 3. 試合結果(Results)削除
        res_count = 0
        for r in all_results:
            uids_in_a = [p.get('user_id') for p in r.get('team_a', [])]
            uids_in_b = [p.get('user_id') for p in r.get('team_b', [])]
            
            if uid in uids_in_a or uid in uids_in_b:
                result_table.delete_item(Key={'result_id': r['result_id']})
                res_count += 1
        
        print(f"✅ 削除完了: {name} (関連Results: {res_count}件)")

    print("\n✨ すべてのクリーンアップが完了しました！")

if __name__ == "__main__":
    main()
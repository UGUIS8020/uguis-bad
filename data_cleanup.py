"""
重複レコード検出・クリーンアップスクリプト

使い方:
  python check_duplicates.py              # 重複を検出（読み取り専用）
  python check_duplicates.py --cleanup    # 重複を検出して自動削除
  python check_duplicates.py --export     # CSV出力
"""

import sys
import os
from collections import defaultdict
from datetime import datetime

# プロジェクトのルートディレクトリをパスに追加
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from uguu.dynamo import DynamoDB


def find_duplicates(db):
    """重複レコードを検出"""
    print("[INFO] 重複レコードの検索開始...")
    print("=" * 80)
    
    # 全レコードを取得
    response = db.part_history.scan()
    all_records = response.get('Items', [])
    
    while 'LastEvaluatedKey' in response:
        response = db.part_history.scan(
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        all_records.extend(response.get('Items', []))
    
    print(f"[INFO] 総レコード数: {len(all_records)}件")
    
    # ↓↓↓ ここを修正 ↓↓↓
    # user_id + date でグループ化（Noneを除外）
    grouped = defaultdict(list)
    invalid_records = []
    
    for record in all_records:
        user_id = record.get('user_id')
        date = record.get('date')
        
        # user_id または date が None の場合はスキップ
        if not user_id or not date:
            invalid_records.append(record)
            continue
        
        key = (user_id, date)
        grouped[key].append(record)
    
    # 不正なレコードを報告
    if invalid_records:
        print(f"\n[警告] user_id または date が欠落しているレコード: {len(invalid_records)}件")
        for i, record in enumerate(invalid_records[:5], 1):  # 最初の5件だけ表示
            print(f"  {i}. user_id={record.get('user_id')}, date={record.get('date')}, joined_at={record.get('joined_at')}")
        if len(invalid_records) > 5:
            print(f"  ... 他 {len(invalid_records) - 5}件")
        print()
    # ↑↑↑ ここまで修正 ↑↑↑
    
    # 重複を検出
    duplicates = []
    total_extra_records = 0
    
    for (user_id, date), records in sorted(grouped.items()):
        if len(records) > 1:
            # キャンセル済みを除外した有効なレコード数
            active_records = [r for r in records if r.get('status') != 'cancelled']
            cancelled_records = [r for r in records if r.get('status') == 'cancelled']
            
            if len(active_records) > 1:
                # 有効なレコードが複数ある場合は重複
                duplicates.append({
                    'user_id': user_id,
                    'date': date,
                    'total_count': len(records),
                    'active_count': len(active_records),
                    'cancelled_count': len(cancelled_records),
                    'active_records': active_records,
                    'cancelled_records': cancelled_records,
                    'severity': 'high'
                })
                total_extra_records += len(active_records) - 1
            elif len(cancelled_records) > 1:
                # キャンセル済みが複数ある場合
                duplicates.append({
                    'user_id': user_id,
                    'date': date,
                    'total_count': len(records),
                    'active_count': len(active_records),
                    'cancelled_count': len(cancelled_records),
                    'active_records': active_records,
                    'cancelled_records': cancelled_records,
                    'severity': 'low'
                })
                total_extra_records += len(cancelled_records) - 1
    
    # 結果表示
    print(f"\n{'='*80}")
    print(f"重複検出結果")
    print(f"{'='*80}")
    print(f"重複グループ数: {len(duplicates)}件")
    print(f"余分なレコード数: {total_extra_records}件")
    print(f"{'='*80}\n")
    
    if duplicates:
        print("【重複の詳細】\n")
        
        # 重大度順にソート
        duplicates.sort(key=lambda x: (x['severity'] == 'low', x['date']), reverse=True)
        
        for i, dup in enumerate(duplicates, 1):
            severity_label = "🔴 重大" if dup['severity'] == 'high' else "🟡 軽微"
            print(f"{i}. {severity_label}")
            print(f"   User ID: {dup['user_id']}")
            print(f"   Date: {dup['date']}")
            print(f"   総レコード数: {dup['total_count']}件")
            print(f"   ├─ 有効: {dup['active_count']}件")
            print(f"   └─ キャンセル済み: {dup['cancelled_count']}件")
            
            # 有効なレコード
            if dup['active_records']:
                print(f"   有効なレコード:")
                for j, record in enumerate(dup['active_records'], 1):
                    joined_at = record.get('joined_at', 'N/A')
                    print(f"     [{j}] {joined_at}")
            
            # キャンセル済みレコード
            if dup['cancelled_records']:
                print(f"   キャンセル済みレコード:")
                for j, record in enumerate(dup['cancelled_records'], 1):
                    joined_at = record.get('joined_at', 'N/A')
                    print(f"     [{j}] {joined_at}")
            
            print()
    else:
        print("重複レコードは見つかりませんでした")
    
    return duplicates, total_extra_records


def cleanup_duplicates(db, duplicates, batch_size=50):
    """
    重複レコードを安全に削除（正しいキー構造を使用）
    """
    import time
    
    print("\n" + "=" * 80)
    print(f"重複レコードのクリーンアップ開始（{batch_size}件ずつ処理）")
    print("=" * 80)
    
    deleted_count = 0
    error_count = 0
    error_details = []
    
    total_groups = len(duplicates)
    
    for i, dup in enumerate(duplicates, 1):
        user_id = dup['user_id']
        date = dup['date']
        
        if i % 10 == 0 or i == 1:
            print(f"[{i}/{total_groups}] 処理中...", end='\r')
        
        try:
            if dup['active_count'] > 1:
                # joined_at で最新のものを選択
                active_records = sorted(
                    dup['active_records'], 
                    key=lambda x: x.get('joined_at', ''), 
                    reverse=True
                )
                keep_record = active_records[0]
                delete_records = active_records[1:]
                
                # 古いレコードを削除（正しいキーを使用）
                for record in delete_records:
                    try:
                        # user_id + joined_at で削除
                        db.part_history.delete_item(
                            Key={
                                'user_id': user_id,
                                'joined_at': record.get('joined_at')
                            }
                        )
                        deleted_count += 1
                    except Exception as e:
                        error_msg = f"User: {user_id[:8]}..., Date: {date}, JoinedAt: {record.get('joined_at')} - {str(e)[:80]}"
                        error_details.append(error_msg)
                        error_count += 1
                        if error_count <= 5:
                            print(f"\n✗ エラー [{error_count}]: {error_msg}")
                
        except Exception as e:
            error_count += 1
            error_msg = f"User: {user_id[:8]}..., Date: {date} - {str(e)[:80]}"
            error_details.append(error_msg)
            print(f"\n✗ エラー [{error_count}]: {error_msg}")
        
        # バッチサイズごとに確認
        if i % batch_size == 0 and i < total_groups:
            print(f"\n\n{'='*80}")
            print(f"進捗レポート ({i}/{total_groups}件処理完了)")
            print(f"{'='*80}")
            print(f"削除完了: {deleted_count}レコード")
            print(f"エラー: {error_count}件")
            
            if error_count > 0:
                print(f"\n直近のエラー:")
                for err in error_details[-5:]:
                    print(f"  • {err}")
            
            print(f"\n次の{min(batch_size, total_groups - i)}件を処理します。")
            response = input("続行しますか？ (yes/no/skip): ")
            
            if response.lower() == 'no':
                print("\n中断しました")
                break
            elif response.lower() == 'skip':
                print(f"\n残り{total_groups - i}件をスキップします")
                break
            
            print(f"\n処理を再開します...\n")
            time.sleep(0.5)
    
    # 最終レポート
    print(f"\n\n{'='*80}")
    print(f"クリーンアップ完了")
    print(f"{'='*80}")
    print(f"処理グループ数: {i}件")
    print(f"削除レコード数: {deleted_count}件")
    print(f"エラー件数: {error_count}件")
    print("=" * 80)
    
    if error_count > 0:
        print(f"\n⚠️  エラー詳細:")
        for err in error_details[:10]:  # 最初の10件
            print(f"  • {err}")
        if len(error_details) > 10:
            print(f"  ... 他 {len(error_details) - 10}件")


def export_to_csv(duplicates, filename='duplicates_report.csv'):
    """重複レコードをCSVに出力"""
    import csv
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'User ID', 'Date', 'Total Count', 'Active Count', 
            'Cancelled Count', 'Severity', 'Active Records', 'Cancelled Records'
        ])
        
        for dup in duplicates:
            active_times = ', '.join([r.get('joined_at', 'N/A') for r in dup['active_records']])
            cancelled_times = ', '.join([r.get('joined_at', 'N/A') for r in dup['cancelled_records']])
            
            writer.writerow([
                dup['user_id'],
                dup['date'],
                dup['total_count'],
                dup['active_count'],
                dup['cancelled_count'],
                dup['severity'],
                active_times,
                cancelled_times
            ])
    
    print(f"\nレポートを {filename} に出力しました")


def main():
    """メイン処理"""
    print("\n" + "=" * 80)
    print("重複レコード検出スクリプト")
    print("=" * 80 + "\n")
    
    # 引数チェック
    do_cleanup = '--cleanup' in sys.argv
    do_export = '--export' in sys.argv
    
    # バッチサイズの指定（デフォルト: 50）
    batch_size = 50
    if '--batch' in sys.argv:
        try:
            batch_idx = sys.argv.index('--batch')
            batch_size = int(sys.argv[batch_idx + 1])
            print(f"バッチサイズ: {batch_size}件\n")
        except:
            print("⚠ --batch の値が不正です。デフォルト（50）を使用します\n")
    
    if do_cleanup:
        print(f"⚠️  クリーンアップモード")
        print(f"• {batch_size}件ごとに確認")
        print(f"• 重複レコードを自動削除")
        print(f"• エラーは即座に表示")
        confirm = input("\n続行しますか？ (yes/no): ")
        if confirm.lower() != 'yes':
            print("キャンセルしました")
            return
    
    # DynamoDB接続
    try:
        db = DynamoDB()
        print("\nDynamoDBに接続しました\n")
    except Exception as e:
        print(f"DynamoDB接続エラー: {e}")
        return
    
    # 重複検出
    duplicates, total_extra = find_duplicates(db)
    
    # CSV出力
    if do_export and duplicates:
        export_to_csv(duplicates)
    
    # クリーンアップ
    if do_cleanup and duplicates:
        cleanup_duplicates(db, duplicates, batch_size=batch_size)
        
        print("\n\n再度重複を確認しますか？ (yes/no): ", end='')
        if input().lower() == 'yes':
            print("\n再検索中...")
            find_duplicates(db)
    
    print("\n" + "=" * 80)
    print("処理完了")
    print("=" * 80)


if __name__ == '__main__':
    main()
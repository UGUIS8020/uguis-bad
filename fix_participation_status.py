"""
参加履歴のstatus修正スクリプト

実際に参加していないのにstatusが未設定/registeredになっているレコードを
"cancelled"に更新します。

使い方:
  python fix_participation_status.py              # 検出のみ（読み取り専用）
  python fix_participation_status.py --fix        # 実際に修正
  python fix_participation_status.py --export     # CSV出力
"""

import sys
import os
from datetime import datetime
from collections import defaultdict

# プロジェクトのルートディレクトリをパスに追加
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from uguu.dynamo import DynamoDB


def get_all_schedules(db):
    """全スケジュールを取得してdate -> participantsのマップを作成"""
    print("[INFO] スケジュール情報を取得中...")
    
    # ↓↓↓ この部分を修正 ↓↓↓
    import boto3
    
    # 既存のDynamoDBリソースから設定を取得
    dynamodb = boto3.resource(
        'dynamodb',
        region_name=os.environ.get('AWS_REGION', 'ap-northeast-1'),
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
    )
    
    table_name = os.environ.get('SCHEDULE_TABLE_NAME', 'bad_schedules')
    schedule_table = dynamodb.Table(table_name)
    # ↑↑↑ ここまで修正 ↑↑↑
    
    response = schedule_table.scan()
    schedules = response.get('Items', [])
    
    while 'LastEvaluatedKey' in response:
        response = schedule_table.scan(
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        schedules.extend(response.get('Items', []))
    
    print(f"[INFO] スケジュール総数: {len(schedules)}件")
    
    # date -> participants のマップを作成
    date_participants_map = {}
    for schedule in schedules:
        date = schedule.get('date')
        participants = schedule.get('participants', [])
        if date:
            date_participants_map[date] = set(participants)
    
    return date_participants_map


def find_invalid_status_records(db, date_participants_map):
    """statusが不正なレコードを検出"""
    print("\n[INFO] 参加履歴の検証開始...")
    print("=" * 80)
    
    # 全参加履歴を取得
    response = db.part_history.scan()
    all_records = response.get('Items', [])
    
    while 'LastEvaluatedKey' in response:
        response = db.part_history.scan(
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        all_records.extend(response.get('Items', []))
    
    print(f"[INFO] 参加履歴総数: {len(all_records)}件")
    
    # 問題のあるレコードを分類
    invalid_records = []
    stats = {
        'total': len(all_records),
        'valid': 0,
        'already_cancelled': 0,
        'should_be_cancelled': 0,
        'future_events': 0,
        'missing_date': 0,
        'missing_user_id': 0
    }
    
    today = datetime.now().date()
    
    for record in all_records:
        user_id = record.get('user_id')
        date = record.get('date')
        status = record.get('status', '未設定')
        joined_at = record.get('joined_at', 'N/A')
        
        # 必須フィールドのチェック
        if not user_id:
            stats['missing_user_id'] += 1
            continue
        if not date:
            stats['missing_date'] += 1
            continue
        
        # 未来のイベントはスキップ
        try:
            event_date = datetime.strptime(date, '%Y-%m-%d').date()
            if event_date > today:
                stats['future_events'] += 1
                continue
        except:
            continue
        
        # 既にキャンセル済み
        if status == 'cancelled':
            stats['already_cancelled'] += 1
            continue
        
        # 実際に参加しているかチェック
        actual_participants = date_participants_map.get(date, set())
        is_actually_participating = user_id in actual_participants
        
        if is_actually_participating:
            # 正常な参加レコード
            stats['valid'] += 1
        else:
            # 参加していないのにstatusが未設定またはregistered
            stats['should_be_cancelled'] += 1
            invalid_records.append({
                'user_id': user_id,
                'date': date,
                'joined_at': joined_at,
                'current_status': status,
                'schedule_exists': date in date_participants_map
            })
    
    # 結果表示
    print(f"\n{'='*80}")
    print(f"検証結果")
    print(f"{'='*80}")
    print(f"総レコード数: {stats['total']}件")
    print(f"  ├─ 正常な参加: {stats['valid']}件")
    print(f"  ├─ 既にキャンセル済み: {stats['already_cancelled']}件")
    print(f"  ├─ 🔴 修正が必要: {stats['should_be_cancelled']}件")
    print(f"  ├─ 未来のイベント: {stats['future_events']}件")
    print(f"  ├─ user_id欠落: {stats['missing_user_id']}件")
    print(f"  └─ date欠落: {stats['missing_date']}件")
    print(f"{'='*80}\n")
    
    if invalid_records:
        print("【修正が必要なレコードの詳細】\n")
        
        # 日付でソート
        invalid_records.sort(key=lambda x: x['date'])
        
        # ユーザーごとにグループ化
        by_user = defaultdict(list)
        for record in invalid_records:
            by_user[record['user_id']].append(record)
        
        for i, (user_id, records) in enumerate(sorted(by_user.items()), 1):
            print(f"{i}. User ID: {user_id[:8]}... ({len(records)}件)")
            for j, record in enumerate(records[:5], 1):  # 最初の5件のみ表示
                schedule_note = "スケジュール存在" if record['schedule_exists'] else "⚠️ スケジュール削除済み"
                print(f"   [{j}] {record['date']} - {record['joined_at'][:19]} ({schedule_note})")
            if len(records) > 5:
                print(f"   ... 他 {len(records) - 5}件")
            print()
    else:
        print("修正が必要なレコードは見つかりませんでした")
    
    return invalid_records, stats


def fix_status(db, invalid_records, batch_size=50):
    """statusを"cancelled"に修正"""
    print("\n" + "=" * 80)
    print(f"statusの修正開始（{batch_size}件ずつ処理）")
    print("=" * 80)
    
    updated_count = 0
    error_count = 0
    error_details = []
    
    total_records = len(invalid_records)
    
    for i, record in enumerate(invalid_records, 1):
        user_id = record['user_id']
        date = record['date']
        joined_at = record['joined_at']
        
        if i % 10 == 0 or i == 1:
            print(f"[{i}/{total_records}] 処理中...", end='\r')
        
        try:
            # statusを"cancelled"に更新
            db.part_history.update_item(
                Key={
                    'user_id': user_id,
                    'joined_at': joined_at
                },
                UpdateExpression='SET #status = :cancelled',
                ExpressionAttributeNames={
                    '#status': 'status'
                },
                ExpressionAttributeValues={
                    ':cancelled': 'cancelled'
                }
            )
            updated_count += 1
            
        except Exception as e:
            error_count += 1
            error_msg = f"User: {user_id[:8]}..., Date: {date}, Error: {str(e)[:80]}"
            error_details.append(error_msg)
            if error_count <= 5:
                print(f"\n✗ エラー [{error_count}]: {error_msg}")
        
        # バッチサイズごとに確認
        if i % batch_size == 0 and i < total_records:
            print(f"\n\n{'='*80}")
            print(f"📊 進捗レポート ({i}/{total_records}件処理完了)")
            print(f"{'='*80}")
            print(f"更新完了: {updated_count}レコード")
            print(f"エラー: {error_count}件")
            
            if error_count > 0:
                print(f"\n直近のエラー:")
                for err in error_details[-5:]:
                    print(f"  • {err}")
            
            print(f"\n次の{min(batch_size, total_records - i)}件を処理します。")
            response = input("続行しますか？ (yes/no/skip): ")
            
            if response.lower() == 'no':
                print("\n中断しました")
                break
            elif response.lower() == 'skip':
                print(f"\n残り{total_records - i}件をスキップします")
                break
            
            print(f"\n処理を再開します...\n")
    
    # 最終レポート
    print(f"\n\n{'='*80}")
    print(f"🏁 修正完了")
    print(f"{'='*80}")
    print(f"処理レコード数: {i}件")
    print(f"更新成功: {updated_count}件")
    print(f"エラー: {error_count}件")
    print("=" * 80)
    
    if error_count > 0:
        print(f"\n⚠️  エラー詳細:")
        for err in error_details[:10]:
            print(f"  • {err}")
        if len(error_details) > 10:
            print(f"  ... 他 {len(error_details) - 10}件")


def export_to_csv(invalid_records, filename='invalid_status_report.csv'):
    """問題のあるレコードをCSVに出力"""
    import csv
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'User ID', 'Date', 'Joined At', 'Current Status', 
            'Schedule Exists', 'Action Required'
        ])
        
        for record in invalid_records:
            writer.writerow([
                record['user_id'],
                record['date'],
                record['joined_at'],
                record['current_status'],
                'Yes' if record['schedule_exists'] else 'No',
                'Set to cancelled'
            ])
    
    print(f"\n✅ レポートを {filename} に出力しました")


def main():
    """メイン処理"""
    print("\n" + "=" * 80)
    print("参加履歴status修正スクリプト")
    print("=" * 80 + "\n")
    
    # 引数チェック
    do_fix = '--fix' in sys.argv
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
    
    if do_fix:
        print(f"⚠️  修正モード")
        print(f"• {batch_size}件ごとに確認")
        print(f"• statusを'cancelled'に更新")
        print(f"• エラーは即座に表示")
        confirm = input("\n続行しますか？ (yes/no): ")
        if confirm.lower() != 'yes':
            print("キャンセルしました")
            return
    
    # DynamoDB接続
    try:
        db = DynamoDB()
        print("\n✅ DynamoDBに接続しました\n")
    except Exception as e:
        print(f"❌ DynamoDB接続エラー: {e}")
        return
    
    # スケジュール情報を取得
    date_participants_map = get_all_schedules(db)
    
    # 不正なstatusのレコードを検出
    invalid_records, stats = find_invalid_status_records(db, date_participants_map)
    
    # CSV出力
    if do_export and invalid_records:
        export_to_csv(invalid_records)
    
    # 修正実行
    if do_fix and invalid_records:
        fix_status(db, invalid_records, batch_size=batch_size)
        
        print("\n\n再度検証しますか？ (yes/no): ", end='')
        if input().lower() == 'yes':
            print("\n再検証中...")
            date_participants_map = get_all_schedules(db)
            find_invalid_status_records(db, date_participants_map)
    
    print("\n" + "=" * 80)
    print("処理完了")
    print("=" * 80)


if __name__ == '__main__':
    main()
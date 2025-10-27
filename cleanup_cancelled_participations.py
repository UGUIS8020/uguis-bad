#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
DynamoDBの参加履歴で、キャンセルされた参加に'cancelled'ステータスを設定するスクリプト

使用方法:
python cleanup_cancelled_participations.py --table bad-users-history
python cleanup_cancelled_participations.py --table bad-users-history --dry-run
"""

import argparse
import sys
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

# .envファイルから環境変数を読み込む
load_dotenv()

def get_boto3_session(profile=None):
    """認証情報を使ってセッションを作成"""
    if profile:
        return boto3.Session(profile_name=profile)
    return boto3.Session()

def cleanup_cancelled_participations(table_name, dry_run=False):
    """キャンセルされた参加レコードを修正"""
    try:
        session = get_boto3_session()
        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table(table_name)
        
        # 利用可能なテーブルをリスト
        tables = list(dynamodb.tables.all())
        print(f"[INFO] 利用可能なテーブル: {[t.name for t in tables]}")
        
        if table_name not in [t.name for t in tables]:
            print(f"[ERROR] テーブル '{table_name}' が見つかりません。")
            return False
        
        print(f"[INFO] テーブル '{table_name}' に接続しました。")
        
        # キャンセルが必要なレコードを特定する方法
        # 方法1: 既知のキャンセルレコードリスト（CSV等から読み込む）
        cancelled_records = get_known_cancelled_records()
        
        # 方法2: 重複参加を検出（同じユーザーが同じ日に複数回参加している場合）
        duplicate_records = find_duplicate_participations(table)
        
        all_records = cancelled_records + duplicate_records
        print(f"[INFO] 処理対象レコード数: {len(all_records)}")
        
        # レコードを更新
        updated_count = 0
        for record in all_records:
            user_id = record['user_id']
            date = record['date']
            
            if dry_run:
                print(f"[DRY-RUN] 更新予定: user_id={user_id}, date={date}, status='cancelled'")
                updated_count += 1
                continue
            
            try:
                table.update_item(
                    Key={
                        'user_id': user_id,
                        'date': date
                    },
                    UpdateExpression='SET #s = :s',
                    ExpressionAttributeNames={
                        '#s': 'status'
                    },
                    ExpressionAttributeValues={
                        ':s': 'cancelled'
                    }
                )
                updated_count += 1
                print(f"[INFO] 更新成功: user_id={user_id}, date={date}")
            except Exception as e:
                print(f"[ERROR] 更新失敗: user_id={user_id}, date={date}, エラー: {str(e)}")
        
        print(f"[INFO] 処理完了: 合計{updated_count}件のレコードを{'確認' if dry_run else '更新'}しました。")
        return True
        
    except Exception as e:
        print(f"[ERROR] データクリーンアップエラー: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def get_known_cancelled_records():
    """既知のキャンセル記録リストを取得"""
    # ここでは例として空のリストを返す
    # 実際には、CSVファイルやログからデータを読み込む処理を実装
    # 例: cancelled_records.csv からの読み込み
    records = []
    try:
        import csv
        if os.path.exists('cancelled_records.csv'):
            with open('cancelled_records.csv', 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    records.append({
                        'user_id': row['user_id'],
                        'date': row['date']
                    })
                print(f"[INFO] CSVから{len(records)}件のキャンセル記録を読み込みました")
    except Exception as e:
        print(f"[WARN] CSVファイル読み込みエラー: {str(e)}")
    
    return records

def find_duplicate_participations(table):
    """同じユーザーが同じ日に複数回参加している場合を検出"""
    duplicates = []
    
    # スキャンで全レコード取得（注意: 大規模データの場合は非効率）
    print("[INFO] 重複参加を検索中...")
    
    try:
        response = table.scan()
        items = response.get('Items', [])
        
        # 同一ユーザーの参加日をカウント
        user_dates = {}
        for item in items:
            user_id = item.get('user_id')
            date = item.get('date')
            
            if not user_id or not date:
                continue
                
            key = f"{user_id}_{date}"
            if key not in user_dates:
                user_dates[key] = []
            
            user_dates[key].append(item)
        
        # 重複を検出（2回以上の参加）
        for key, entries in user_dates.items():
            if len(entries) > 1:
                print(f"[INFO] 重複検出: {key}, {len(entries)}件")
                # 最新の1件以外をキャンセル対象とする
                # TODO: キャンセル判断ロジックはビジネス要件に合わせて調整
                entries.sort(key=lambda x: x.get('created_at', ''))
                for entry in entries[:-1]:  # 最後の1件以外をキャンセル対象に
                    duplicates.append({
                        'user_id': entry['user_id'],
                        'date': entry['date']
                    })
        
        print(f"[INFO] 合計{len(duplicates)}件の重複参加を検出しました")
        
    except Exception as e:
        print(f"[ERROR] 重複検索エラー: {str(e)}")
    
    return duplicates

def main():
    parser = argparse.ArgumentParser(description='キャンセルされた参加レコードのクリーンアップ')
    parser.add_argument('--table', required=True, help='DynamoDBテーブル名')
    parser.add_argument('--dry-run', action='store_true', help='実行せずに処理内容を表示')
    parser.add_argument('--profile', help='AWS認証プロファイル名')
    args = parser.parse_args()
    
    if args.dry_run:
        print("[INFO] ドライラン実行中 - 実際の更新は行われません")
    
    success = cleanup_cancelled_participations(args.table, args.dry_run)
    
    if success:
        print("[SUCCESS] クリーンアップ処理が完了しました")
        sys.exit(0)
    else:
        print("[FAILED] クリーンアップ処理に失敗しました")
        sys.exit(1)

if __name__ == "__main__":
    main()
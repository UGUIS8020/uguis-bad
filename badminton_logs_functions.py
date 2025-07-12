import boto3
from datetime import datetime
from botocore.exceptions import ClientError
from decimal import Decimal

# DynamoDBの設定
def get_dynamodb_table():
    """
    DynamoDBテーブルオブジェクトを取得（読み取り専用）
    """
    try:
        dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
        table = dynamodb.Table('badminton_chat_logs')
        return table
    except Exception as e:
        print(f"[DYNAMODB] テーブル取得エラー: {str(e)}")
        return None

def decimal_to_float(obj):
    """
    DynamoDBのDecimal型をfloatに変換（JSON化のため）
    """
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [decimal_to_float(v) for v in obj]
    return obj

def get_badminton_chat_logs(cache_filter=None, limit=100):
    """
    バドミントンチャットログを取得する関数
    
    Args:
        cache_filter (str): 'true', 'false', or None
        limit (int): 取得件数制限
        
    Returns:
        dict: {'success': bool, 'data': list, 'count': int, 'error': str}
    """
    try:
        table = get_dynamodb_table()
        if table is None:
            return {'success': False, 'error': 'DynamoDBテーブルにアクセスできません'}
        
        print(f"[API] チャットログ取得開始: cache={cache_filter}, limit={limit}")
        
        # フィルター条件を構築
        filter_expression = None
        
        if cache_filter:
            is_cached = cache_filter.lower() == 'true'
            filter_expression = boto3.dynamodb.conditions.Attr('is_cached_response').eq(is_cached)
        
        # DynamoDBからデータ取得
        scan_kwargs = {'Limit': limit}
        if filter_expression:
            scan_kwargs['FilterExpression'] = filter_expression
            
        response = table.scan(**scan_kwargs)
        items = response.get('Items', [])
        
        # Decimal型をfloatに変換
        items = [decimal_to_float(item) for item in items]
        
        # タイムスタンプでソート（最新順）
        items.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        
        print(f"[API] 取得完了: {len(items)}件")
        
        return {
            'success': True,
            'data': items,
            'count': len(items)
        }
        
    except Exception as e:
        print(f"[API] エラー: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }

def get_badminton_chat_stats():
    """
    バドミントンチャット統計を取得する関数
    
    Returns:
        dict: {'success': bool, 'stats': dict, 'error': str}
    """
    try:
        table = get_dynamodb_table()
        if table is None:
            return {'success': False, 'error': 'DynamoDBテーブルにアクセスできません'}
        
        print("[API] 統計データ取得開始")
        
        # 全データを取得（統計用）
        response = table.scan()
        all_items = response.get('Items', [])
        
        # 今日のデータをフィルター
        today = datetime.now().strftime('%Y-%m-%d')
        today_items = [item for item in all_items if item.get('date') == today]
        
        # 統計計算
        total_count = len(all_items)
        today_count = len(today_items)
        today_cached = sum(1 for item in today_items if item.get('is_cached_response', False))
        today_new = today_count - today_cached
        
        # キャッシュヒット率
        cache_hit_rate = (today_cached / today_count * 100) if today_count > 0 else 0
        
        # 平均処理時間（新規回答のみ）
        new_response_times = [
            float(item.get('processing_time_seconds', 0)) 
            for item in all_items 
            if not item.get('is_cached_response', False) and float(item.get('processing_time_seconds', 0)) > 0
        ]
        avg_processing_time = sum(new_response_times) / len(new_response_times) if new_response_times else 0
        
        stats = {
            'total_chats': total_count,
            'today_chats': today_count,
            'today_cached': today_cached,
            'today_new': today_new,
            'cache_hit_rate': round(cache_hit_rate, 1),
            'avg_processing_time': round(avg_processing_time, 2)
        }
        
        print(f"[API] 統計取得完了: {stats}")
        
        return {
            'success': True,
            'stats': stats
        }
        
    except Exception as e:
        print(f"[API] 統計エラー: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }

def check_badminton_health():
    """
    バドミントンチャットログのヘルスチェック関数
    
    Returns:
        dict: {'status': str, 'message': str, 'timestamp': str, 'table': str}
    """
    try:
        table = get_dynamodb_table()
        if table is None:
            return {
                'status': 'error', 
                'message': 'DynamoDB接続失敗',
                'timestamp': datetime.now().isoformat()
            }
            
        # テーブル存在確認
        table.load()
        
        return {
            'status': 'ok',
            'timestamp': datetime.now().isoformat(),
            'table': 'badminton_chat_logs',
            'message': 'テーブル接続正常'
        }
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            return {
                'status': 'error',
                'message': 'テーブル badminton_chat_logs が存在しません',
                'timestamp': datetime.now().isoformat()
            }
        else:
            return {
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }
    

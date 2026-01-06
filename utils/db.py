import boto3
import logging
import os
from dateutil import parser
from boto3.dynamodb.types import TypeDeserializer
logger = logging.getLogger(__name__)

from uguu.dynamo import DynamoDB   
_db = DynamoDB() 

def cancel_participation(user_id: str, date_str: str, schedule_id: str = None):
    """bad-users-history の該当レコードを更新する"""
    return _db.cancel_participation(user_id, date_str, schedule_id)

def get_schedule_table():
    """スケジュールテーブルを取得する関数"""
    """最新12件を取得"""
    region = os.getenv('AWS_REGION', 'ap-northeast-1')
    table_name = os.getenv('DYNAMODB_TABLE_NAME', 'bad_schedules')

    # logger.debug(f"Region: {region}")
    # logger.debug(f"Table name: {table_name}")

    try:
        dynamodb = boto3.resource('dynamodb', region_name=region)
        table = dynamodb.Table(table_name)
        return table
    except Exception as e:
        logger.error(f"Error getting schedule table: {e}")
        raise    

def get_schedules_with_formatting():
    """スケジュール一覧を取得してフォーマットする（最適化版）"""
    logger.info("Cache: Attempting to get formatted schedules")
    
    try:
        schedule_table = get_schedule_table()
        response = schedule_table.scan()
        
        # アクティブなスケジュールのみをフィルタリングしてからソート
        active_schedules = [
            schedule for schedule in response.get('Items', [])
            if schedule.get('status', 'active') == 'active'
        ]
        
        # dateで昇順ソート
        schedules = sorted(
            active_schedules,
            key=lambda x: x.get('date', ''),
            reverse=False
        )[:10]
        
        formatted_schedules = []
        for schedule in schedules:
            try:
                date_obj = parser.parse(schedule['date'])
                formatted_date = f"{date_obj.month:02d}/{date_obj.day:02d}({schedule['day_of_week']})"
                
                # 軽量化: 参加者の詳細情報は取得せず、カウントのみ
                participants = schedule.get('participants', [])
                
                formatted_schedule = {
                    'schedule_id': schedule.get('schedule_id'),
                    'title': schedule.get('title'),
                    'date': schedule.get('date'),
                    'day_of_week': schedule.get('day_of_week'),
                    'formatted_date': formatted_date,
                    'start_time': schedule.get('start_time', ''),  
                    'end_time': schedule.get('end_time', ''),      
                    'venue': schedule.get('venue', ''),            
                    'court': schedule.get('court', ''),          
                    'max_participants': int(schedule.get('max_participants', 10)),
                    'participants_count': len(participants),
                    'participants': participants,  # IDのみ保持
                    'status': schedule.get('status', 'active'),
                    'description': schedule.get('description', ''),
                    'comment': schedule.get('comment', ''),
                    'tara_participants': schedule.get('tara_participants', []),
                    'tara_count': len(schedule.get('tara_participants', []))
                }
                
                formatted_schedules.append(formatted_schedule)
                
            except Exception as e:
                logger.error(f"Error processing schedule: {e}")
                continue
        
        logger.info(f"Cache: Successfully processed {len(formatted_schedules)} schedules")
        return formatted_schedules
        
    except Exception as e:
        logger.error(f"Error in get_schedules_with_formatting: {str(e)}")
        return []
    
def get_schedules_with_formatting_all():
    """すべてのスケジュールを取得（制限なし）"""
    try:
        schedule_table = get_schedule_table()
        response = schedule_table.scan()

        active_schedules = [
            schedule for schedule in response.get('Items', [])
            if schedule.get('status', 'active') == 'active'
        ]

        # dateで昇順ソート
        schedules = sorted(active_schedules, key=lambda x: x.get('date', ''))

        formatted_schedules = []
        for schedule in schedules:
            date_obj = parser.parse(schedule['date'])
            formatted_date = f"{date_obj.month:02d}/{date_obj.day:02d}({schedule['day_of_week']})"

            formatted_schedules.append({
                'schedule_id': schedule.get('schedule_id'),
                'title': schedule.get('title'),
                'date': schedule.get('date'),
                'day_of_week': schedule.get('day_of_week'),
                'formatted_date': formatted_date,
                'start_time': schedule.get('start_time', ''),  
                'end_time': schedule.get('end_time', ''),      
                'venue': schedule.get('venue', ''),            
                'court': schedule.get('court', ''),          
                'max_participants': int(schedule.get('max_participants', 10)),
                'participants_count': len(schedule.get('participants', [])),
                'participants': schedule.get('participants', []),
                'status': schedule.get('status', 'active'),
                'description': schedule.get('description', '')
            })

        return formatted_schedules

    except Exception as e:
        logger.error(f"[get_all_schedules] スケジュール取得失敗: {e}")
        return []

def get_users_batch(user_ids):
    """ユーザー情報を一括取得する関数（正しくデシリアライズ）"""
    try:
        dynamodb = boto3.client('dynamodb', region_name=os.getenv('AWS_REGION', 'ap-northeast-1'))
        keys = [{'user#user_id': {'S': user_id}} for user_id in user_ids]

        response = dynamodb.batch_get_item(
            RequestItems={
                'bad-users': {
                    'Keys': keys
                }
            }
        )

        deserializer = TypeDeserializer()
        users = {}

        if 'Responses' in response:
            for user in response['Responses']['bad-users']:
                # 🔽 ここでネストされた DynamoDB形式 → 通常の dict に変換
                deserialized_user = {k: deserializer.deserialize(v) for k, v in user.items()}
                user_id = deserialized_user['user#user_id']
                users[user_id] = deserialized_user

        return users

    except Exception as e:
        logger.error(f"Error batch getting users: {e}")
        return {}
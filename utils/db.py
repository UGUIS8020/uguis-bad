import boto3
import logging
import os
from dateutil import parser
from flask import current_app
logger = logging.getLogger(__name__)


def get_schedule_table():
    """スケジュールテーブルを取得する関数"""
    region = os.getenv('AWS_REGION', 'ap-northeast-1')
    table_name = os.getenv('DYNAMODB_TABLE_NAME', 'bad_schedules')

    logger.debug(f"Region: {region}")
    logger.debug(f"Table name: {table_name}")

    try:
        dynamodb = boto3.resource('dynamodb', region_name=region)
        table = dynamodb.Table(table_name)
        return table
    except Exception as e:
        logger.error(f"Error getting schedule table: {e}")
        raise


def get_schedules_with_formatting():
    """スケジュール一覧を取得してフォーマットする"""
    logger.info("Cache: Attempting to get formatted schedules")
    
    try:
        schedule_table = get_schedule_table()
        response = schedule_table.scan()
        
        # アクティブなスケジュールのみをフィルタリングしてからソート
        active_schedules = [
            schedule for schedule in response.get('Items', [])
            if schedule.get('status', 'active') == 'active'  # statusが設定されていない場合はactiveとみなす
        ]
        
        # dateで昇順ソート
        schedules = sorted(
            active_schedules,
            key=lambda x: x.get('date', ''),
            reverse=False
        )[:12]  # 最新10件を取得
        
        # 以下は既存の処理をそのまま維持
        unique_user_ids = set()
        for schedule in schedules:
            if 'participants' in schedule:
                unique_user_ids.update(schedule['participants'])
        
        logger.info(f"Found {len(unique_user_ids)} unique users to fetch")
        
        users = get_users_batch(list(unique_user_ids))
        
        logger.info(f"Retrieved {len(users)} user records")
        
        formatted_schedules = []
        for schedule in schedules:
            try:
                date_obj = parser.parse(schedule['date'])
                formatted_date = f"{date_obj.month:02d}/{date_obj.day:02d}({schedule['day_of_week']})"
                schedule['formatted_date'] = formatted_date
                
                participants_info = []
                if 'participants' in schedule:
                    for participant_id in schedule['participants']:
                        user = users.get(participant_id, {})
                        participants_info.append({
                            'user_id': participant_id,
                            'display_name': user.get('display_name', '未登録'),
                            'badminton_experience': user.get('badminton_experience', '')
                        })

                 # max_participantsとparticipants_countの処理を追加
                schedule['max_participants'] = int(schedule.get('max_participants', 10))  
                schedule['participants_count'] = len(schedule.get('participants', []))
                
                schedule['participants_info'] = participants_info
                formatted_schedules.append(schedule)
                
            except Exception as e:
                logger.error(f"Error processing schedule: {e}")
                continue
        
        logger.info(f"Cache: Successfully processed {len(formatted_schedules)} schedules")
        return formatted_schedules
        
    except Exception as e:
        logger.error(f"Error in get_schedules_with_formatting: {str(e)}")
        return []
    
def get_users_batch(user_ids):
    """ユーザー情報を一括取得する関数"""
    try:
        user_table = current_app.dynamodb.Table(os.getenv('TABLE_NAME_USER', 'bad-users'))
        
        # ユーザーIDのリストをバッチ処理用に変換
        keys = [{'user#user_id': user_id} for user_id in user_ids]
        
        # バッチでユーザー情報を取得
        response = current_app.dynamodb.batch_get_item(
            RequestItems={
                os.getenv('TABLE_NAME_USER', 'bad-users'): {
                    'Keys': keys
                }
            }
        )
        
        # 結果を辞書形式に整理
        users = {}
        if 'Responses' in response:
            for user in response['Responses'][os.getenv('TABLE_NAME_USER', 'bad-users')]:
                user_id = user['user#user_id']

                # デバッグ用：各ユーザーの情報を確認
                logger.info(f"User data: {user}")
                users[user_id] = user
                
        return users
        
    except Exception as e:
        logger.error(f"Error batch getting users: {e}")
        return {}
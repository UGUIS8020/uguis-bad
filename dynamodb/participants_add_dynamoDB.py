import boto3
from datetime import datetime
import uuid
import os
from dotenv import load_dotenv

# .envファイルから環境変数を読み込む
load_dotenv()

aws_credentials = {
    'aws_access_key_id': os.getenv("AWS_ACCESS_KEY_ID"),
    'aws_secret_access_key': os.getenv("AWS_SECRET_ACCESS_KEY"),
    'region_name': os.getenv("AWS_REGION")
}

class ScheduleManager:
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', **aws_credentials)
        self.table = self.dynamodb.Table('bad_schedules')

    def add_participants_count_to_all_items(self):
        """既存の全スケジュールにparticipants_countを追加"""
        try:
            # 全ての項目を取得
            response = self.table.scan()
            items = response.get('Items', [])
            
            for item in items:
                self.table.update_item(
                    Key={
                        'schedule_id': item['schedule_id'],
                        'date': item['date']  # ソートキーも必要
                    },
                    UpdateExpression='SET participants_count = :count',
                    ExpressionAttributeValues={':count': 0}
                )
            
            print("すべての項目にparticipants_countを追加しました")
            return True
        except Exception as e:
            print(f"更新中にエラーが発生しました: {str(e)}")
            return False

def main():
    # ScheduleManagerのインスタンスを作成
    manager = ScheduleManager()
    # 全項目を更新
    manager.add_participants_count_to_all_items()

if __name__ == '__main__':
    main()
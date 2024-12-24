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
        self.table = self.dynamodb.Table('bad-schedule')  # テーブル名を修正

    def add_schedule(self, venue, date, start_time, end_time, day_of_week):
        """スケジュールを追加する"""
        try:            
            schedule_id = str(uuid.uuid4())  # ユニークなID

            item = {
                'schedule_id': schedule_id,   # パーテーションキー
                'venue': venue,               # 会場
                'date': date,                 # 日付
                'day_of_week': day_of_week,   # 曜日
                'start_time': start_time,     # 開始時間
                'end_time': end_time,         # 終了時間
                'created_at': datetime.now().isoformat(),  # 作成日時
                'updated_at': datetime.now().isoformat()   # 更新日時
            }

            # DynamoDBにデータを追加
            self.table.put_item(Item=item)
            print(f"スケジュールが追加されました: {venue} {date}")
            return True

        except Exception as e:
            print(f"スケジュール追加中にエラーが発生しました: {str(e)}")
            return False

    def get_schedules(self):
        """全てのスケジュールを取得する"""
        try:
            response = self.table.scan()
            items = response.get('Items', [])
            
            # 見やすく整形して表示
            if items:
                print("\n登録されているスケジュール:")
                for item in items:
                    print(f"\n会場: {item['venue']}")
                    print(f"日付: {item['date']}")
                    print(f"曜日: {item['day_of_week']}")
                    print(f"時間: {item['start_time']} - {item['end_time']}")
                    print("-" * 40)
            else:
                print("\nスケジュールが登録されていません")
            
            return items

        except Exception as e:
            print(f"スケジュール取得中にエラーが発生しました: {str(e)}")
            return []

def main():
    # ScheduleManagerのインスタンスを作成
    manager = ScheduleManager()

    # テストデータの追加
    test_schedules = [
        {
            'venue': '北越谷A面',
            'date': '2024-11-23',
            'day_of_week': '土',
            'start_time': '13:00',
            'end_time': '15:00'
        },
        {
            'venue': '北越谷B面',
            'date': '2024-11-24',
            'day_of_week': '日',
            'start_time': '10:00',
            'end_time': '12:00'
        },
        {
            'venue': 'ウィングハット',
            'date': '2024-11-25',
            'day_of_week': '月',
            'start_time': '19:00',
            'end_time': '21:00'
        }
    ]

    # テストデータを追加
    for schedule in test_schedules:
        manager.add_schedule(
            venue=schedule['venue'],
            date=schedule['date'],
            day_of_week=schedule['day_of_week'],
            start_time=schedule['start_time'],
            end_time=schedule['end_time']
        )

    # 登録されたスケジュールを確認
    manager.get_schedules()

if __name__ == '__main__':
    main()
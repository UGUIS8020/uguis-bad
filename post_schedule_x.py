#!/usr/bin/env python3
"""
post_schedule_x.py
鶯バドミントンサークル 練習予定 X自動投稿スクリプト

使い方:
  python3 post_schedule_x.py --mode 3days   # 3日後の練習を投稿
  python3 post_schedule_x.py --mode today   # 当日の練習を投稿
  python3 post_schedule_x.py --mode test    # テスト（実際には投稿しない）

cron設定例:
  0 9 * * * cd /var/www/uguis_bad && python3 post_schedule_x.py --mode 3days
  0 7 * * * cd /var/www/uguis_bad && python3 post_schedule_x.py --mode today
"""

import boto3
import os
import sys
import argparse
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import tweepy
from dotenv import load_dotenv

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/www/uguis_bad/post_schedule_x.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 環境変数読み込み
load_dotenv('/var/www/uguis_bad/.env')

# DynamoDB設定
AWS_ACCESS_KEY_ID     = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION            = os.getenv('AWS_REGION', 'ap-northeast-1')
TABLE_NAME_SCHEDULE   = os.getenv('TABLE_NAME_SCHEDULE', 'bad_schedules')

# X API設定
X_API_KEY              = os.getenv('X_API_KEY')
X_API_SECRET           = os.getenv('X_API_SECRET')
X_ACCESS_TOKEN         = os.getenv('X_ACCESS_TOKEN')
X_ACCESS_TOKEN_SECRET  = os.getenv('X_ACCESS_TOKEN_SECRET')

SITE_URL = 'https://uguis-bad.shibuya8020.com'

# 会場名の短縮表示
VENUE_SHORT = {
    '越谷市立地域スポーツセンター': '北越谷体育館',
    '越谷市立総合体育館': '越谷総合体育館',
}


def get_dynamodb_table():
    dynamodb = boto3.resource(
        'dynamodb',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    return dynamodb.Table(TABLE_NAME_SCHEDULE)


def get_schedules_for_date(target_date: str):
    """指定日付のactiveなスケジュールを取得"""
    table = get_dynamodb_table()
    result = table.scan(
        FilterExpression='#d = :date AND #s = :status',
        ExpressionAttributeNames={'#d': 'date', '#s': 'status'},
        ExpressionAttributeValues={':date': target_date, ':status': 'active'}
    )
    return result.get('Items', [])


def build_tweet(schedule: dict, mode: str) -> str:
    """ツイート文を生成"""
    date_str    = schedule.get('date', '')          # 例: '2026-05-11'
    dow         = schedule.get('day_of_week', '')   # 例: '月'
    start       = schedule.get('start_time', '')    # 例: '19:00'
    end         = schedule.get('end_time', '')      # 例: '21:00'
    venue_raw   = schedule.get('venue', '')
    court       = schedule.get('court', '')
    max_p       = int(schedule.get('max_participants', 0))
    count_p     = int(schedule.get('participants_count', 0))
    remaining   = max_p - count_p

    # 日付フォーマット: 05/11(月)
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        date_disp = dt.strftime('%-m/%-d') + f'({dow})'
    except Exception:
        date_disp = date_str

    venue_disp = VENUE_SHORT.get(venue_raw, venue_raw)

    # 残枠表示
    if remaining <= 0:
        slots = '満員御礼🈵'
    elif remaining <= 3:
        slots = f'残り{remaining}枠⚡️'
    else:
        slots = f'残{remaining}枠 参加募集中！'

    if mode == '3days':
        header = '【3日後の練習🏸】'
    else:
        header = '【本日の練習🏸】'

    tweet = (
        f'{header}\n'
        f'📅 {date_disp} {start}〜{end}\n'
        f'📍 {venue_disp} {court}\n'
        f'👥 {slots}\n'
        f'👉 {SITE_URL}'
    )
    return tweet


def post_to_x(text: str, dry_run: bool = False) -> bool:
    """Xに投稿"""
    if dry_run:
        logger.info(f'[DRY RUN] 投稿内容:\n{text}')
        return True

    if not all([X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET]):
        logger.error('X APIキーが設定されていません。.envを確認してください。')
        return False

    try:
        client = tweepy.Client(
            consumer_key=X_API_KEY,
            consumer_secret=X_API_SECRET,
            access_token=X_ACCESS_TOKEN,
            access_token_secret=X_ACCESS_TOKEN_SECRET
        )
        response = client.create_tweet(text=text)
        tweet_id = response.data['id']
        logger.info(f'投稿成功！ tweet_id={tweet_id}')
        logger.info(f'URL: https://x.com/rbn17pjAfz41575/status/{tweet_id}')
        return True
    except tweepy.TweepyException as e:
        logger.error(f'X投稿エラー: {e}')
        return False


def main():
    parser = argparse.ArgumentParser(description='鶯バドミントン X自動投稿')
    parser.add_argument('--mode', choices=['3days', 'today', 'test'], default='test',
                        help='3days: 3日後の練習, today: 当日の練習, test: テスト')
    args = parser.parse_args()

    dry_run = (args.mode == 'test')
    jst = timezone(timedelta(hours=9))
    now = datetime.now(jst)

    if args.mode == '3days':
        target = now + timedelta(days=3)
    elif args.mode == 'today':
        target = now
    else:
        # testモード: 直近のactiveな予定を1件取得
        target = now

    target_date = target.strftime('%Y-%m-%d')
    logger.info(f'モード: {args.mode}, 対象日: {target_date}')

    schedules = get_schedules_for_date(target_date)

    if not schedules:
        logger.info(f'{target_date} の練習予定はありません。')
        return

    for schedule in schedules:
        tweet = build_tweet(schedule, args.mode)
        logger.info(f'生成ツイート:\n{tweet}')
        success = post_to_x(tweet, dry_run=dry_run)
        if not success:
            logger.error('投稿に失敗しました。')
            sys.exit(1)


if __name__ == '__main__':
    main()
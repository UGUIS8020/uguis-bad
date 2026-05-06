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
import re
import sys
import argparse
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4
import requests
import tweepy
from dotenv import load_dotenv

# スクリプトのディレクトリを基準にパスを解決
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, 'post_schedule_x.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 環境変数読み込み（本番パス → なければスクリプトと同じディレクトリの.env）
load_dotenv('/var/www/uguis_bad/.env')
load_dotenv(os.path.join(BASE_DIR, '.env'))

# DynamoDB設定
AWS_ACCESS_KEY_ID     = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION            = os.getenv('AWS_REGION', 'ap-northeast-1')
TABLE_NAME_SCHEDULE   = os.getenv('TABLE_NAME_SCHEDULE', 'bad_schedules')
TABLE_NAME_USER       = os.getenv('TABLE_NAME_USER', 'bad-users')

# X API設定
X_API_KEY              = os.getenv('X_API_KEY')
X_API_SECRET           = os.getenv('X_API_SECRET')
X_ACCESS_TOKEN         = os.getenv('X_ACCESS_TOKEN')
X_ACCESS_TOKEN_SECRET  = os.getenv('X_ACCESS_TOKEN_SECRET')

SITE_URL = 'https://uguis-bad.shibuya8020.com'

# Threads API設定
THREADS_ACCESS_TOKEN = os.getenv('THREADS_ACCESS_TOKEN')

# うぐすたぐらむ設定
UGUU_BOT_USER_ID = os.getenv('UGUU_BOT_USER_ID')
UGUU_POST_TABLE  = 'uguu_post'

# Instagram API設定
INSTAGRAM_ACCESS_TOKEN = os.getenv('INSTAGRAM_ACCESS_TOKEN')
INSTAGRAM_USER_ID      = os.getenv('INSTAGRAM_USER_ID')
INSTAGRAM_IMAGES_DIR   = os.path.join(BASE_DIR, 'static', 'sns_images')


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


def get_participant_details(schedule: dict) -> dict:
    """参加者の合計数と初参加者の性別内訳を返す"""
    raw = schedule.get('participants') or []
    ids = []
    seen = set()
    for x in raw:
        uid = (x.get('user_id') or x.get('user#user_id')) if isinstance(x, dict) else x
        if uid and str(uid) not in seen:
            seen.add(str(uid))
            ids.append(str(uid))

    total = int(schedule.get('participants_count') or len(ids))
    first_timers = {'male': 0, 'female': 0, 'other': 0}

    if not ids:
        return {'total': total, 'first_timers': first_timers}

    client = boto3.client(
        'dynamodb',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    # 100件ずつバッチ取得
    for i in range(0, len(ids), 100):
        chunk = ids[i:i+100]
        resp = client.batch_get_item(RequestItems={
            TABLE_NAME_USER: {
                'Keys': [{'user#user_id': {'S': uid}} for uid in chunk],
                'ProjectionExpression': '#uid, practice_count, gender',
                'ExpressionAttributeNames': {'#uid': 'user#user_id'}
            }
        })
        for user in resp.get('Responses', {}).get(TABLE_NAME_USER, []):
            pc = user.get('practice_count', {}).get('N')
            if pc is not None and int(pc) == 1:
                g = user.get('gender', {}).get('S', '')
                if g == 'male':
                    first_timers['male'] += 1
                elif g == 'female':
                    first_timers['female'] += 1
                else:
                    first_timers['other'] += 1

    return {'total': total, 'first_timers': first_timers}


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

    venue_disp = venue_raw

    # "A面(3面)"→"A面"、"第一体育室(6面)"→"第一体育室" のように括弧部分を除去
    court_disp = re.sub(r'\(.*?\)$', '', court).strip()

    # 残枠表示
    if remaining <= 0:
        slots = '満員御礼'
    elif remaining <= 3:
        slots = f'残り{remaining}枠'
    else:
        slots = f'残{remaining}枠 参加募集中！'

    schedule_id = schedule.get('schedule_id', '')
    detail_url = f'{SITE_URL}/schedule/{schedule_id}/{date_str}' if schedule_id else SITE_URL

    # 参加者詳細
    details = get_participant_details(schedule)
    total = details['total']
    ft = details['first_timers']
    ft_parts = []
    if ft['female'] > 0:
        ft_parts.append(f'女性{ft["female"]}名')
    if ft['male'] > 0:
        ft_parts.append(f'男性{ft["male"]}名')
    if ft['other'] > 0:
        ft_parts.append(f'その他{ft["other"]}名')
    first_timer_line = f'初参加者：{" ".join(ft_parts)}' if ft_parts else ''

    if mode == 'today':
        # 当日引用投稿用：シンプルな内容で最新情報を表示
        lines = [
            '今日はバドミントンです',
            '参加者募集！',
            f'{date_disp} {start}〜{end}',
            f'{venue_disp} {court_disp}',
            slots,
            f'現在{total}名参加',
        ]
        if first_timer_line:
            lines.append(first_timer_line)
        lines.append(detail_url)
    else:
        # 3日前投稿用：詳細情報を含む
        lines = [
            '鶯バドミントン',
            '参加者募集！',
            f'{date_disp} {start}〜{end}',
            f'{venue_disp} {court_disp}',
            '基礎打ちができて、ルールがわかればどなたでも参加できます。',
            '初級者～上級者レベルが違っても楽しくゲームできる方',
            slots,
            f'現在{total}名参加',
        ]
        if first_timer_line:
            lines.append(first_timer_line)
        lines.append(detail_url)

    return '\n'.join(lines)


INSTAGRAM_HASHTAGS = '#バドミントン #越谷 #バドミントンサークル #メンバー募集 #越谷市 #埼玉 #スポーツサークル #社会人サークル #badminton'


def build_instagram_caption(schedule: dict, mode: str) -> str:
    """Instagram用キャプションを生成（URLなし・ハッシュタグあり）"""
    date_str  = schedule.get('date', '')
    dow       = schedule.get('day_of_week', '')
    start     = schedule.get('start_time', '')
    end       = schedule.get('end_time', '')
    venue_raw = schedule.get('venue', '')
    court     = schedule.get('court', '')
    max_p     = int(schedule.get('max_participants', 0))
    count_p   = int(schedule.get('participants_count', 0))
    remaining = max_p - count_p

    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        date_disp = dt.strftime('%-m/%-d') + f'({dow})'
    except Exception:
        date_disp = date_str

    court_disp = re.sub(r'\(.*?\)$', '', court).strip()

    if remaining <= 0:
        slots = '満員御礼'
    elif remaining <= 3:
        slots = f'残り{remaining}枠'
    else:
        slots = f'残{remaining}枠 参加募集中！'

    details = get_participant_details(schedule)
    total = details['total']
    ft = details['first_timers']
    ft_parts = []
    if ft['female'] > 0:
        ft_parts.append(f'女性{ft["female"]}名')
    if ft['male'] > 0:
        ft_parts.append(f'男性{ft["male"]}名')
    if ft['other'] > 0:
        ft_parts.append(f'その他{ft["other"]}名')
    first_timer_line = f'初参加者：{" ".join(ft_parts)}' if ft_parts else ''

    if mode == 'today':
        lines = [
            '今日はバドミントンです',
            '参加者募集！',
            f'{date_disp} {start}〜{end}',
            f'{venue_raw} {court_disp}',
            slots,
            f'現在{total}名参加',
        ]
    else:
        lines = [
            '鶯バドミントン',
            '参加者募集！',
            f'{date_disp} {start}〜{end}',
            f'{venue_raw} {court_disp}',
            '基礎打ちができて、ルールがわかればどなたでも参加できます。',
            '初級者～上級者レベルが違っても楽しくゲームできる方',
            slots,
            f'現在{total}名参加',
        ]

    if first_timer_line:
        lines.append(first_timer_line)
    lines.append('詳細・参加登録はプロフィールのリンクから')
    lines.append('')
    lines.append(INSTAGRAM_HASHTAGS)

    return '\n'.join(lines)


def get_x_client():
    return tweepy.Client(
        consumer_key=X_API_KEY,
        consumer_secret=X_API_SECRET,
        access_token=X_ACCESS_TOKEN,
        access_token_secret=X_ACCESS_TOKEN_SECRET
    )


def post_to_x(text: str, dry_run: bool = False):
    """Xに投稿。成功時はtweet_idを返す、失敗時はNone"""
    if dry_run:
        logger.info(f'[DRY RUN] 投稿内容:\n{text}')
        return 'DRY_RUN_ID'

    if not all([X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET]):
        logger.error('X APIキーが設定されていません。.envを確認してください。')
        return None

    try:
        response = get_x_client().create_tweet(text=text)
        tweet_id = response.data['id']
        logger.info(f'X投稿成功！ tweet_id={tweet_id}')
        logger.info(f'URL: https://x.com/rbn17pjAfz41575/status/{tweet_id}')
        return tweet_id
    except tweepy.TweepyException as e:
        logger.error(f'X投稿エラー: {e}')
        return None


def save_post_ids(schedule: dict, tweet_id=None, threads_post_id=None, uguu_post_id=None):
    """投稿IDをDynamoDBのスケジュールに保存"""
    updates = []
    values = {}
    if tweet_id:
        updates.append('x_tweet_id = :xid')
        values[':xid'] = tweet_id
    if threads_post_id:
        updates.append('threads_post_id = :tid')
        values[':tid'] = threads_post_id
    if uguu_post_id:
        updates.append('uguu_post_id = :uid')
        values[':uid'] = uguu_post_id
    if not updates:
        return
    table = get_dynamodb_table()
    table.update_item(
        Key={'schedule_id': schedule['schedule_id'], 'date': schedule['date']},
        UpdateExpression='SET ' + ', '.join(updates),
        ExpressionAttributeValues=values
    )
    logger.info(f'投稿ID保存: x={tweet_id}, threads={threads_post_id}, uguu={uguu_post_id}')


def quote_post_to_x(text: str, quote_tweet_id: str, dry_run: bool = False) -> bool:
    """保存したtweet_idを引用して新規投稿"""
    if dry_run:
        logger.info(f'[DRY RUN] 引用投稿 quote_id={quote_tweet_id}\n{text}')
        return True

    if not all([X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET]):
        logger.error('X APIキーが設定されていません。')
        return False

    try:
        response = get_x_client().create_tweet(text=text, quote_tweet_id=quote_tweet_id)
        tweet_id = response.data['id']
        logger.info(f'X引用投稿成功！ tweet_id={tweet_id}')
        return True
    except tweepy.TweepyException as e:
        logger.error(f'X引用投稿エラー: {e}')
        return False


def post_to_threads(text: str, dry_run: bool = False):
    """Threadsに投稿。成功時はpost_idを返す、失敗時はNone"""
    if dry_run:
        logger.info(f'[DRY RUN Threads] 投稿内容:\n{text}')
        return 'DRY_RUN_ID'

    if not THREADS_ACCESS_TOKEN:
        logger.error('Threads APIキーが設定されていません。.envを確認してください。')
        return None

    try:
        r1 = requests.post(
            'https://graph.threads.net/v1.0/me/threads',
            params={
                'media_type': 'TEXT',
                'text': text,
                'access_token': THREADS_ACCESS_TOKEN,
            }
        )
        if not r1.ok:
            logger.error(f'Threadsコンテナ作成失敗: {r1.status_code} {r1.text}')
            return None
        container_id = r1.json()['id']
        logger.info(f'Threadsコンテナ作成: {container_id}')

        r2 = requests.post(
            'https://graph.threads.net/v1.0/me/threads_publish',
            params={
                'creation_id': container_id,
                'access_token': THREADS_ACCESS_TOKEN,
            }
        )
        if not r2.ok:
            logger.error(f'Threads公開失敗: {r2.status_code} {r2.text}')
            return None
        post_id = r2.json()['id']
        logger.info(f'Threads投稿成功！ post_id={post_id}')
        return post_id
    except Exception as e:
        logger.error(f'Threads投稿エラー: {e}')
        return None


def quote_post_to_threads(text: str, quote_post_id: str, dry_run: bool = False) -> bool:
    """保存したpost_idを引用して新規投稿（テキスト必須）"""
    if dry_run:
        logger.info(f'[DRY RUN Threads] 引用投稿 quote_id={quote_post_id}\n{text}')
        return True

    if not THREADS_ACCESS_TOKEN:
        logger.error('Threads APIキーが設定されていません。')
        return False

    try:
        r1 = requests.post(
            'https://graph.threads.net/v1.0/me/threads',
            params={
                'media_type': 'TEXT',
                'text': text,
                'quote_post_id': quote_post_id,
                'access_token': THREADS_ACCESS_TOKEN,
            }
        )
        if not r1.ok:
            logger.error(f'Threads引用コンテナ作成失敗: {r1.status_code} {r1.text}')
            return False
        container_id = r1.json()['id']

        r2 = requests.post(
            'https://graph.threads.net/v1.0/me/threads_publish',
            params={
                'creation_id': container_id,
                'access_token': THREADS_ACCESS_TOKEN,
            }
        )
        if not r2.ok:
            logger.error(f'Threads引用投稿公開失敗: {r2.status_code} {r2.text}')
            return False
        logger.info(f'Threads引用投稿成功！ post_id={r2.json()["id"]}')
        return True
    except Exception as e:
        logger.error(f'Threads引用投稿エラー: {e}')
        return False


def post_to_uguu(content: str, dry_run: bool = False):
    """うぐすたぐらむに投稿。成功時はpost_idを返す、失敗時はNone"""
    if dry_run:
        logger.info(f'[DRY RUN うぐすたぐらむ] 投稿内容:\n{content}')
        return 'DRY_RUN_ID'

    if not UGUU_BOT_USER_ID:
        logger.error('UGUU_BOT_USER_IDが設定されていません。.envを確認してください。')
        return None

    try:
        dynamodb = boto3.resource(
            'dynamodb',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        table = dynamodb.Table(UGUU_POST_TABLE)
        post_id = str(uuid4())
        now = datetime.now(timezone.utc).isoformat()
        item = {
            'PK': f'POST#{post_id}',
            'SK': f'METADATA#{post_id}',
            'post_id': post_id,
            'user_id': UGUU_BOT_USER_ID,
            'content': content,
            'created_at': now,
            'updated_at': now,
            'feed_pk': 'FEED',
            'feed_sk': f'TS#{now}#POST#{post_id}',
            'likes_count': 0,
        }
        table.put_item(Item=item)
        logger.info(f'うぐすたぐらむ投稿成功！ post_id={post_id}')
        return post_id
    except Exception as e:
        logger.error(f'うぐすたぐらむ投稿エラー: {e}')
        return None


def post_to_instagram(caption: str, dry_run: bool = False):
    """Instagramに画像付きで投稿。成功時はpost_idを返す、失敗時はNone"""
    import random, glob as globmod
    images = globmod.glob(os.path.join(INSTAGRAM_IMAGES_DIR, '*.jpg')) + \
             globmod.glob(os.path.join(INSTAGRAM_IMAGES_DIR, '*.png'))
    if not images:
        logger.error('sns_imagesフォルダに画像がありません。')
        return None

    image_file = random.choice(images)
    image_name = os.path.basename(image_file)
    image_url  = f'{SITE_URL}/static/sns_images/{image_name}'

    if dry_run:
        logger.info(f'[DRY RUN Instagram] 画像: {image_name}\nキャプション:\n{caption}')
        return 'DRY_RUN_ID'

    if not INSTAGRAM_ACCESS_TOKEN or not INSTAGRAM_USER_ID:
        logger.error('Instagram APIキーが設定されていません。')
        return None

    try:
        r1 = requests.post(
            f'https://graph.instagram.com/v21.0/{INSTAGRAM_USER_ID}/media',
            params={
                'image_url': image_url,
                'caption': caption,
                'access_token': INSTAGRAM_ACCESS_TOKEN,
            }
        )
        if not r1.ok:
            logger.error(f'Instagramコンテナ作成失敗: {r1.status_code} {r1.text}')
            return None
        container_id = r1.json()['id']

        r2 = requests.post(
            f'https://graph.instagram.com/v21.0/{INSTAGRAM_USER_ID}/media_publish',
            params={
                'creation_id': container_id,
                'access_token': INSTAGRAM_ACCESS_TOKEN,
            }
        )
        if not r2.ok:
            logger.error(f'Instagram公開失敗: {r2.status_code} {r2.text}')
            return None
        post_id = r2.json()['id']
        logger.info(f'Instagram投稿成功！ post_id={post_id} 画像: {image_name}')
        return post_id
    except Exception as e:
        logger.error(f'Instagram投稿エラー: {e}')
        return None


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
        tweet   = build_tweet(schedule, args.mode)
        caption = build_instagram_caption(schedule, args.mode)
        logger.info(f'生成ツイート:\n{tweet}')

        if args.mode == 'today':
            # 3日前に保存したIDを引用して新規投稿
            x_id = schedule.get('x_tweet_id')
            if x_id:
                ok_x = quote_post_to_x(tweet, x_id, dry_run=dry_run)
                if not ok_x:
                    logger.error('X引用投稿に失敗しました。')
            else:
                logger.warning('x_tweet_idが見つかりません。新規投稿します。')
                ok_x = post_to_x(tweet, dry_run=dry_run) is not None

            th_id = schedule.get('threads_post_id')
            if th_id:
                ok_threads = quote_post_to_threads(tweet, th_id, dry_run=dry_run)
                if not ok_threads:
                    logger.error('Threads引用投稿に失敗しました。')
            else:
                logger.warning('threads_post_idが見つかりません。新規投稿します。')
                ok_threads = post_to_threads(tweet, dry_run=dry_run) is not None

            ok_ig   = post_to_instagram(caption, dry_run=dry_run) is not None
            uguu_id = post_to_uguu(tweet, dry_run=dry_run)
            ok_uguu = uguu_id is not None
            if not ok_ig:
                logger.error('Instagram投稿に失敗しました。')
            if not ok_uguu:
                logger.error('うぐすたぐらむ投稿に失敗しました。')
        else:
            # 3daysモード: 新規投稿してIDを保存
            x_id      = post_to_x(tweet, dry_run=dry_run)
            th_id     = post_to_threads(tweet, dry_run=dry_run)
            ok_x      = x_id is not None
            ok_threads = th_id is not None
            ok_ig     = post_to_instagram(caption, dry_run=dry_run) is not None
            uguu_id   = post_to_uguu(tweet, dry_run=dry_run)
            ok_uguu   = uguu_id is not None
            if not dry_run:
                save_post_ids(schedule, tweet_id=x_id, threads_post_id=th_id, uguu_post_id=uguu_id)
            if not ok_ig:
                logger.error('Instagram投稿に失敗しました。')
            if not ok_uguu:
                logger.error('うぐすたぐらむ投稿に失敗しました。')

        if not ok_x:
            logger.error('X投稿/リポストに失敗しました。')
        if not ok_threads:
            logger.error('Threads投稿/リポストに失敗しました。')
        if not ok_x and not ok_threads and not ok_ig and not ok_uguu:
            sys.exit(1)


if __name__ == '__main__':
    main()
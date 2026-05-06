#!/usr/bin/env python3
"""
post_badnews.py
直近7日間のバドミントンニュースをAIで要約してX・うぐすたぐらむに投稿するスクリプト

使い方:
  python3 post_badnews.py --mode test    # テスト（実際には投稿しない）
  python3 post_badnews.py --mode post    # 実際に投稿

cron設定例（毎週月曜 午前8時）:
  0 8 * * 1 cd /var/www/uguis_bad && /var/www/uguis_bad/venv/bin/python post_badnews.py --mode post >> /var/www/uguis_bad/post_badnews.log 2>&1
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import boto3
import tweepy
import openai
import requests
from boto3.dynamodb.conditions import Key
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, 'post_badnews.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv('/var/www/uguis_bad/.env')
load_dotenv(os.path.join(BASE_DIR, '.env'))

AWS_REGION            = os.getenv('AWS_REGION', 'ap-northeast-1')
AWS_ACCESS_KEY_ID     = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BAD_TABLE_NAME        = os.getenv('BAD_TABLE_NAME', 'bad_items')

X_API_KEY             = os.getenv('X_API_KEY')
X_API_SECRET          = os.getenv('X_API_SECRET')
X_ACCESS_TOKEN        = os.getenv('X_ACCESS_TOKEN')
X_ACCESS_TOKEN_SECRET = os.getenv('X_ACCESS_TOKEN_SECRET')

THREADS_ACCESS_TOKEN  = os.getenv('THREADS_ACCESS_TOKEN')

UGUU_BOT_USER_ID      = os.getenv('UGUU_BOT_USER_ID')
UGUU_POST_TABLE       = 'uguu_post'

OPENAI_API_KEY        = os.getenv('OPENAI_API_KEY')

SITE_NEWS_URL         = 'https://uguis-bad.shibuya8020.com/bad_news'


# ---- DynamoDB ----

def get_recent_news(days: int = 7, limit: int = 30) -> list[dict]:
    """直近N日間の日本語ニュース記事を取得"""
    dynamodb = boto3.resource(
        'dynamodb',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    table = dynamodb.Table(BAD_TABLE_NAME)

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%S')

    resp = table.query(
        IndexName='gsi1',
        KeyConditionExpression=Key('gsi1pk').eq('KIND#news#LANG#ja') & Key('gsi1sk').gte(cutoff),
        ScanIndexForward=False,
        Limit=limit,
    )
    items = resp.get('Items', [])
    logger.info(f'直近{days}日間のニュース: {len(items)}件取得')
    return items


# ---- AI要約 ----

def generate_posts(articles: list[dict]) -> dict | None:
    """OpenAIでXツイートとうぐすたぐらむ投稿文を生成"""
    if not OPENAI_API_KEY:
        logger.error('OPENAI_API_KEYが設定されていません。')
        return None

    if not articles:
        logger.warning('記事が0件のため投稿をスキップします。')
        return None

    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    # 記事リストをテキスト化
    article_text = ''
    for i, a in enumerate(articles, 1):
        title   = a.get('title', '')
        summary = a.get('summary', '') or ''
        url     = a.get('url', '')
        pub     = a.get('published_at', '')[:10] if a.get('published_at') else ''
        article_text += f'{i}. [{pub}] {title}\n   概要: {summary[:100]}\n   URL: {url}\n\n'

    prompt = f"""以下は直近1週間の日本のバドミントンニュース一覧です。

{article_text}

これらの記事から日本国内で最も注目度が高そうな記事を1本選び、以下の2つを作成してください。

【X(Twitter)投稿文のルール】
- 全角140文字以内（URLは含めない）
- 「【バドミントンニュース】」から始める
- 記事タイトルをそのままコピーせず、読んだ人が「へえ！」と思えるような自分の言葉でひと言添える
- 驚き・共感・応援など感情が伝わる表現を入れる
- 最後に「詳細→」は書かない（URLは別途付加します）

【うぐすたぐらむ投稿文のルール】
- 「【今週のバドミントンニュース】」から始め、直後に空行を入れる
- 導入文は1文だけ。今週を一言で表す
- 導入文の後に空行を入れる
- 主なトピックを3〜5件紹介する。各トピックは**1文**で簡潔に。感想は短く一言添える程度でよい
- 各トピックの間には空行を入れる
- 締めの言葉は不要
- 最後に空行なしで「詳細はサイトのニュースページへ」と入れる
- 余計な文章を足さず、シンプルにまとめること

以下のJSON形式で返してください：
{{
  "tweet": "Xに投稿するテキスト",
  "uguu": "うぐすたぐらむに投稿するテキスト",
  "top_article_url": "注目記事のURL"
}}"""

    try:
        response = client.chat.completions.create(
            model='gpt-4o-mini',
            messages=[
                {'role': 'system', 'content': 'あなたはバドミントンが大好きな社会人サークルのSNS担当です。親しみやすく、読んでいて楽しくなるような自然な日本語で投稿文を書きます。ニュースサイトの見出しをそのまま並べるのではなく、自分の感想や言葉を交えて書いてください。'},
                {'role': 'user', 'content': prompt}
            ],
            response_format={'type': 'json_object'},
            temperature=0.9,
        )
        import json, re
        result = json.loads(response.choices[0].message.content)
        # 連続する空行を1行に統一
        if 'uguu' in result:
            result['uguu'] = re.sub(r'\n{3,}', '\n\n', result['uguu']).strip()
        logger.info(f'AI生成完了: tweet={len(result.get("tweet",""))}文字')
        return result
    except Exception as e:
        logger.error(f'OpenAI APIエラー: {e}')
        return None


# ---- 投稿関数 ----

def post_to_x(tweet_text: str, article_url: str, dry_run: bool = False):
    """Xに投稿（ツイート本文 + 記事URL）"""
    full_text = f'{tweet_text}\n{article_url}'

    if dry_run:
        logger.info(f'[DRY RUN X]\n{full_text}')
        return 'DRY_RUN_ID'

    if not all([X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET]):
        logger.error('X APIキーが設定されていません。')
        return None

    try:
        client = tweepy.Client(
            consumer_key=X_API_KEY,
            consumer_secret=X_API_SECRET,
            access_token=X_ACCESS_TOKEN,
            access_token_secret=X_ACCESS_TOKEN_SECRET
        )
        resp = client.create_tweet(text=full_text)
        tweet_id = resp.data['id']
        logger.info(f'X投稿成功！ tweet_id={tweet_id}')
        return tweet_id
    except tweepy.TweepyException as e:
        logger.error(f'X投稿エラー: {e}')
        return None


def post_to_threads(text: str, dry_run: bool = False):
    """Threadsに投稿"""
    if dry_run:
        logger.info(f'[DRY RUN Threads]\n{text}')
        return 'DRY_RUN_ID'

    if not THREADS_ACCESS_TOKEN:
        logger.error('THREADS_ACCESS_TOKENが設定されていません。')
        return None

    try:
        r1 = requests.post(
            'https://graph.threads.net/v1.0/me/threads',
            params={'media_type': 'TEXT', 'text': text, 'access_token': THREADS_ACCESS_TOKEN}
        )
        if not r1.ok:
            logger.error(f'Threadsコンテナ作成失敗: {r1.status_code} {r1.text}')
            return None
        container_id = r1.json()['id']

        r2 = requests.post(
            'https://graph.threads.net/v1.0/me/threads_publish',
            params={'creation_id': container_id, 'access_token': THREADS_ACCESS_TOKEN}
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


def post_to_uguu(content: str, dry_run: bool = False):
    """うぐすたぐらむに投稿"""
    if dry_run:
        logger.info(f'[DRY RUN うぐすたぐらむ]\n{content}')
        return 'DRY_RUN_ID'

    if not UGUU_BOT_USER_ID:
        logger.error('UGUU_BOT_USER_IDが設定されていません。')
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
        table.put_item(Item={
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
        })
        logger.info(f'うぐすたぐらむ投稿成功！ post_id={post_id}')
        return post_id
    except Exception as e:
        logger.error(f'うぐすたぐらむ投稿エラー: {e}')
        return None


# ---- メイン ----

def main():
    parser = argparse.ArgumentParser(description='バドミントンニュース自動投稿')
    parser.add_argument('--mode', choices=['test', 'post'], default='test',
                        help='test: テスト（投稿なし）, post: 実際に投稿')
    args = parser.parse_args()
    dry_run = (args.mode == 'test')

    logger.info(f'=== post_badnews 開始 mode={args.mode} ===')

    # 1. ニュース取得
    articles = get_recent_news(days=7, limit=30)
    if not articles:
        logger.info('直近7日間のニュースがありません。終了します。')
        return

    # 2. AI生成
    result = generate_posts(articles)
    if not result:
        logger.error('投稿文の生成に失敗しました。')
        sys.exit(1)

    tweet_text      = result.get('tweet', '')
    uguu_text       = result.get('uguu', '')
    top_article_url = result.get('top_article_url', SITE_NEWS_URL)

    # うぐすたぐらむにはニュースページURLも追記
    uguu_full = f'{uguu_text}\n{SITE_NEWS_URL}'

    logger.info(f'--- X投稿文 ---\n{tweet_text}\n{top_article_url}')
    logger.info(f'--- うぐすたぐらむ投稿文 ---\n{uguu_full}')

    # 3. 各SNSに投稿
    x_id      = post_to_x(tweet_text, top_article_url, dry_run=dry_run)
    uguu_id   = post_to_uguu(uguu_full, dry_run=dry_run)

    if not x_id:
        logger.error('X投稿に失敗しました。')
    if not uguu_id:
        logger.error('うぐすたぐらむ投稿に失敗しました。')

    logger.info('=== post_badnews 完了 ===')


if __name__ == '__main__':
    main()

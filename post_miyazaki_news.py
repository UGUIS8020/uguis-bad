#!/usr/bin/env python3
"""
post_miyazaki_news.py
宮崎友花選手の最新ニュースをAIで要約してX・Threads・うぐすたぐらむに投稿するスクリプト

使い方:
  python3 post_miyazaki_news.py --mode test    # テスト（実際には投稿しない）
  python3 post_miyazaki_news.py --mode post    # 実際に投稿

cron設定例（毎週日曜 夜20時）:
  0 20 * * 0 /bin/bash -lc 'cd /var/www/uguis_bad && /var/www/uguis_bad/venv/bin/python post_miyazaki_news.py --mode post >> /var/www/uguis_bad/post_miyazaki_news.log 2>&1'
"""

import os
import sys
import argparse
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus
from uuid import uuid4

import boto3
import tweepy
import openai
import requests
import feedparser
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, 'post_miyazaki_news.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv('/var/www/uguis_bad/.env')
load_dotenv(os.path.join(BASE_DIR, '.env'))

X_API_KEY             = os.getenv('X_API_KEY')
X_API_SECRET          = os.getenv('X_API_SECRET')
X_ACCESS_TOKEN        = os.getenv('X_ACCESS_TOKEN')
X_ACCESS_TOKEN_SECRET = os.getenv('X_ACCESS_TOKEN_SECRET')

THREADS_ACCESS_TOKEN  = os.getenv('THREADS_ACCESS_TOKEN')

UGUU_BOT_USER_ID      = os.getenv('UGUU_BOT_USER_ID')
UGUU_POST_TABLE       = 'uguu_post'

AWS_REGION            = os.getenv('AWS_REGION', 'ap-northeast-1')
AWS_ACCESS_KEY_ID     = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

OPENAI_API_KEY        = os.getenv('OPENAI_API_KEY')

SITE_NEWS_URL         = 'https://uguis-bad.shibuya8020.com/bad_news'
SEARCH_QUERY          = '宮崎友花 バドミントン'


# ---- Google Newsからニュース取得 ----

def fetch_miyazaki_news(days: int = 7) -> list[dict]:
    """Google Newsから宮崎友花の最新ニュースを取得"""
    url = f"https://news.google.com/rss/search?q={quote_plus(SEARCH_QUERY)}&hl=ja&gl=JP&ceid=JP:ja"
    feed = feedparser.parse(url)

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    articles = []

    for e in feed.entries:
        pub_str = getattr(e, 'published', None)
        pub_dt = None
        if pub_str:
            try:
                from email.utils import parsedate_to_datetime
                pub_dt = parsedate_to_datetime(pub_str)
                if pub_dt.tzinfo is None:
                    pub_dt = pub_dt.replace(tzinfo=timezone.utc)
            except Exception:
                pass

        if pub_dt and pub_dt < cutoff:
            continue

        articles.append({
            'title':        (getattr(e, 'title', '') or '').strip(),
            'url':          getattr(e, 'link', ''),
            'published_at': pub_dt.isoformat() if pub_dt else '',
            'summary':      (getattr(e, 'summary', '') or '').strip(),
            'author':       getattr(e, 'source', {}).get('title', '') if hasattr(e, 'source') else '',
        })

    logger.info(f'宮崎友花ニュース: {len(articles)}件取得（直近{days}日間）')
    return articles


# ---- AI要約 ----

def generate_posts(articles: list[dict]) -> dict | None:
    """OpenAIで宮崎友花ニュースの投稿文を生成"""
    if not OPENAI_API_KEY:
        logger.error('OPENAI_API_KEYが設定されていません。')
        return None

    if not articles:
        logger.warning('記事が0件のため投稿をスキップします。')
        return None

    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    article_text = ''
    for i, a in enumerate(articles, 1):
        pub = a.get('published_at', '')[:10] if a.get('published_at') else ''
        article_text += f'{i}. [{pub}] {a["title"]}\n   概要: {a["summary"][:120]}\n   URL: {a["url"]}\n\n'

    prompt = f"""以下は直近1週間の宮崎友花選手（バドミントン）に関するニュース一覧です。

{article_text}

これらの記事から最も注目度が高そうな記事を1本選び、以下の2つを作成してください。

【X(Twitter)投稿文のルール】
- 全角140文字以内（URLは含めない）
- 「【宮崎友花】」から始める
- 選手への応援・感動・驚きが伝わる自分の言葉で書く
- 絵文字を1〜2個使ってもよい
- 最後に「詳細→」は書かない（URLは別途付加します）

【うぐすたぐらむ投稿文のルール】
- 「【宮崎友花ニュース】」から始め、直後に空行を入れる
- 今週の宮崎選手の動向を2〜4件、各1文で紹介する
- 各トピックの間には空行を入れる
- 応援・感動のコメントを自然に添える
- 締めの言葉は不要
- 最後に空行なしで「詳細はサイトのニュースページへ」と入れる

記事がない場合や宮崎友花に直接関係しない記事しかない場合は、nullを返してください。

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
                {'role': 'system', 'content': 'あなたは宮崎友花選手のファンで、バドミントン社会人サークルのSNS担当です。選手への愛情と応援が伝わる自然な日本語で投稿文を書きます。'},
                {'role': 'user', 'content': prompt}
            ],
            response_format={'type': 'json_object'},
            temperature=0.9,
        )
        result = json.loads(response.choices[0].message.content)
        if not result.get('tweet'):
            logger.info('AIが投稿なしと判断しました。')
            return None
        if 'uguu' in result:
            result['uguu'] = re.sub(r'\n{3,}', '\n\n', result['uguu']).strip()
        logger.info(f'AI生成完了: tweet={len(result.get("tweet",""))}文字')
        return result
    except Exception as e:
        logger.error(f'OpenAI APIエラー: {e}')
        return None


# ---- 各SNSへの投稿 ----

def post_to_x(tweet_text: str, article_url: str, dry_run: bool = False):
    full_text = f'{tweet_text}\n{article_url}'
    if dry_run:
        logger.info(f'[DRY RUN X]\n{full_text}')
        return 'DRY_RUN_ID'
    try:
        client = tweepy.Client(
            consumer_key=X_API_KEY, consumer_secret=X_API_SECRET,
            access_token=X_ACCESS_TOKEN, access_token_secret=X_ACCESS_TOKEN_SECRET
        )
        resp = client.create_tweet(text=full_text)
        tweet_id = resp.data['id']
        logger.info(f'X投稿成功！ tweet_id={tweet_id}')
        return tweet_id
    except Exception as e:
        logger.error(f'X投稿エラー: {e}')
        return None


def post_to_threads(text: str, dry_run: bool = False):
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
        time.sleep(5)
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
    if dry_run:
        logger.info(f'[DRY RUN うぐすたぐらむ]\n{content}')
        return 'DRY_RUN_ID'
    if not UGUU_BOT_USER_ID:
        logger.error('UGUU_BOT_USER_IDが設定されていません。')
        return None
    try:
        dynamodb = boto3.resource(
            'dynamodb', region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        table = dynamodb.Table(UGUU_POST_TABLE)
        now = datetime.now(timezone.utc).isoformat()
        post_id = str(uuid4())
        table.put_item(Item={
            'PK': f'POST#{post_id}',
            'SK': f'METADATA#{post_id}',
            'post_id': post_id,
            'user_id': UGUU_BOT_USER_ID,
            'content': content,
            'created_at': now,
            'updated_at': now,
            'likes_count': 0,
            'reposts_count': 0,
            'replies_count': 0,
        })
        logger.info(f'うぐすたぐらむ投稿成功！ post_id={post_id}')
        return post_id
    except Exception as e:
        logger.error(f'うぐすたぐらむ投稿エラー: {e}')
        return None


# ---- メイン ----

def main():
    parser = argparse.ArgumentParser(description='宮崎友花ニュース自動投稿')
    parser.add_argument('--mode', choices=['test', 'post'], default='test',
                        help='test: テスト（投稿なし）, post: 実際に投稿')
    parser.add_argument('--days', type=int, default=7, help='何日前までのニュースを対象にするか')
    args = parser.parse_args()
    dry_run = (args.mode == 'test')

    logger.info(f'=== post_miyazaki_news 開始 mode={args.mode} ===')

    articles = fetch_miyazaki_news(days=args.days)
    if not articles:
        logger.info(f'直近{args.days}日間の宮崎友花ニュースがありません。終了します。')
        return

    result = generate_posts(articles)
    if not result:
        logger.info('投稿対象のニュースがないため終了します。')
        return

    tweet_text      = result.get('tweet', '')
    uguu_text       = result.get('uguu', '')
    top_article_url = result.get('top_article_url', SITE_NEWS_URL)
    uguu_full       = uguu_text + '\n' + SITE_NEWS_URL

    logger.info(f'--- X投稿文 ---\n{tweet_text}\n{top_article_url}')
    logger.info(f'--- Threads投稿文 ---\n{tweet_text}\n{top_article_url}')
    logger.info(f'--- うぐすたぐらむ投稿文 ---\n{uguu_full}')

    threads_text = tweet_text + '\n' + top_article_url
    x_id       = post_to_x(tweet_text, top_article_url, dry_run=dry_run)
    threads_id = post_to_threads(threads_text, dry_run=dry_run)
    uguu_id    = post_to_uguu(uguu_full, dry_run=dry_run)

    if not x_id:
        logger.error('X投稿に失敗しました。')
    if not threads_id:
        logger.error('Threads投稿に失敗しました。')
    if not uguu_id:
        logger.error('うぐすたぐらむ投稿に失敗しました。')

    logger.info('=== post_miyazaki_news 完了 ===')


if __name__ == '__main__':
    main()

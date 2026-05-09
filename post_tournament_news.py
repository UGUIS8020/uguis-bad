#!/usr/bin/env python3
"""
post_tournament_news.py
国内・国際バドミントン大会結果をAIで要約してX・Threads・うぐすたぐらむに投稿するスクリプト

使い方:
  python3 post_tournament_news.py --mode test    # テスト（実際には投稿しない）
  python3 post_tournament_news.py --mode post    # 実際に投稿

cron設定例（8時間ごと）:
  0 0,8,16 * * * /bin/bash -lc 'cd /var/www/uguis_bad && /var/www/uguis_bad/venv/bin/python post_tournament_news.py --mode post >> /var/www/uguis_bad/post_tournament_news.log 2>&1'
"""

import os
import sys
import json
import argparse
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus
from uuid import uuid4
from email.utils import parsedate_to_datetime

import boto3
import tweepy
import openai
import requests
import feedparser
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
POSTED_URLS_FILE = os.path.join(BASE_DIR, 'posted_urls.json')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, 'post_tournament_news.log'), encoding='utf-8'),
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

# 国内・国際大会の検索クエリ
TOURNAMENT_QUERIES = [
    ('バドミントン 大会 結果',            'ja'),
    ('全日本バドミントン 結果',            'ja'),
    ('バドミントン ワールドツアー',         'ja'),
    ('BWF World Tour results badminton', 'en'),
    ('badminton championship results',   'en'),
]


# ---- 重複防止 ----

def load_posted_urls() -> dict:
    if not os.path.exists(POSTED_URLS_FILE):
        return {}
    try:
        with open(POSTED_URLS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}

def save_posted_urls(data: dict):
    # 30日以上前のエントリを削除
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    data = {url: ts for url, ts in data.items() if ts >= cutoff}
    with open(POSTED_URLS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def mark_as_posted(urls: list[str]):
    data = load_posted_urls()
    now = datetime.now(timezone.utc).isoformat()
    for url in urls:
        data[url] = now
    save_posted_urls(data)


# ---- ニュース取得 ----

def fetch_tournament_news(hours: int = 24) -> list[dict]:
    """複数クエリで大会ニュースを取得し、未投稿の新着のみ返す"""
    posted = load_posted_urls()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    seen_urls = set()
    articles = []

    for query, lang in TOURNAMENT_QUERIES:
        hl = 'ja' if lang == 'ja' else 'en'
        gl = 'JP' if lang == 'ja' else 'US'
        ceid = f'JP:{hl}' if lang == 'ja' else f'US:{hl}'
        url = f'https://news.google.com/rss/search?q={quote_plus(query)}&hl={hl}&gl={gl}&ceid={ceid}'
        feed = feedparser.parse(url)

        for e in feed.entries:
            link = getattr(e, 'link', '')
            if not link or link in seen_urls or link in posted:
                continue

            pub_dt = None
            pub_str = getattr(e, 'published', None)
            if pub_str:
                try:
                    pub_dt = parsedate_to_datetime(pub_str)
                    if pub_dt.tzinfo is None:
                        pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                except Exception:
                    pass

            if pub_dt and pub_dt < cutoff:
                continue

            seen_urls.add(link)
            articles.append({
                'title':        (getattr(e, 'title', '') or '').strip(),
                'url':          link,
                'published_at': pub_dt.isoformat() if pub_dt else '',
                'summary':      (getattr(e, 'summary', '') or '').strip(),
                'lang':         lang,
            })

    logger.info(f'大会ニュース新着: {len(articles)}件（直近{hours}時間・未投稿のみ）')
    return articles


# ---- AI要約 ----

def generate_posts(articles: list[dict]) -> dict | None:
    if not OPENAI_API_KEY:
        logger.error('OPENAI_API_KEYが設定されていません。')
        return None
    if not articles:
        return None

    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    article_text = ''
    for i, a in enumerate(articles, 1):
        pub = a.get('published_at', '')[:10]
        lang_label = '🇯🇵' if a.get('lang') == 'ja' else '🌏'
        article_text += f'{i}. {lang_label}[{pub}] {a["title"]}\n   概要: {a["summary"][:120]}\n   URL: {a["url"]}\n\n'

    prompt = f"""以下は直近24時間の国内・国際バドミントン大会に関するニュース一覧です。

{article_text}

最も速報性・注目度が高い記事を1本選んで以下を作成してください。

【X(Twitter)投稿文のルール】
- 全角140文字以内（URLは含めない）
- 「【大会結果】」から始める
- 記事に書かれている情報だけを使う。スコアや選手名が不明な場合は書かない（○○や〇〇などのプレースホルダー禁止）
- 興奮・応援・感動が伝わる表現を使う
- 絵文字を1〜2個使ってもよい

【うぐすたぐらむ投稿文のルール】
- 「【バドミントン大会速報】」から始め、空行を入れる
- 記事に書かれている情報だけを使って2〜4件紹介。不明な情報は書かない（○○や〇〇などのプレースホルダー禁止）
- 各トピックの間に空行
- 応援コメントを自然に添える
- 最後（空行なし）に「詳細はサイトのニュースページへ」

大会結果と無関係な記事しかない場合、または具体的な情報が何も得られない場合はnullを返してください。

JSON形式で返してください：
{{
  "tweet": "Xに投稿するテキスト",
  "uguu": "うぐすたぐらむに投稿するテキスト",
  "top_article_url": "注目記事のURL"
}}"""

    try:
        response = client.chat.completions.create(
            model='gpt-4o-mini',
            messages=[
                {'role': 'system', 'content': 'あなたはバドミントンが大好きな社会人サークルのSNS担当です。大会結果をリアルタイムで伝える速報担当として、興奮が伝わる自然な日本語で書きます。'},
                {'role': 'user', 'content': prompt}
            ],
            response_format={'type': 'json_object'},
            temperature=0.8,
        )
        result = json.loads(response.choices[0].message.content)
        if not result.get('tweet'):
            logger.info('AIが投稿対象なしと判断しました。')
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
    parser = argparse.ArgumentParser(description='バドミントン大会結果自動投稿')
    parser.add_argument('--mode', choices=['test', 'post'], default='test')
    parser.add_argument('--hours', type=int, default=24, help='何時間前までの記事を対象にするか')
    args = parser.parse_args()
    dry_run = (args.mode == 'test')

    logger.info(f'=== post_tournament_news 開始 mode={args.mode} ===')

    articles = fetch_tournament_news(hours=args.hours)
    if not articles:
        logger.info('新着の大会ニュースがありません。終了します。')
        return

    result = generate_posts(articles)
    if not result:
        logger.info('投稿対象のニュースがないため終了します。')
        return

    tweet_text      = result.get('tweet', '')
    uguu_text       = result.get('uguu', '')
    top_article_url = result.get('top_article_url', SITE_NEWS_URL)
    uguu_full       = uguu_text + '\n' + SITE_NEWS_URL
    threads_text    = tweet_text + '\n' + top_article_url

    x_id       = post_to_x(tweet_text, top_article_url, dry_run=dry_run)
    threads_id = post_to_threads(threads_text, dry_run=dry_run)
    uguu_id    = post_to_uguu(uguu_full, dry_run=dry_run)

    if not x_id:
        logger.error('X投稿に失敗しました。')
    if not threads_id:
        logger.error('Threads投稿に失敗しました。')
    if not uguu_id:
        logger.error('うぐすたぐらむ投稿に失敗しました。')

    # 投稿済みURLを記録（dry_runでは記録しない）
    if not dry_run and (x_id or threads_id or uguu_id):
        mark_as_posted([a['url'] for a in articles])

    logger.info('=== post_tournament_news 完了 ===')


if __name__ == '__main__':
    main()

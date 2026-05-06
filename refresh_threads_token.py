#!/usr/bin/env python3
"""
refresh_threads_token.py
Threads長期アクセストークンを自動更新するスクリプト

cron設定例（毎月1日 午前3時に実行）:
  0 3 1 * * /bin/bash -lc 'cd /var/www/uguis_bad && /var/www/uguis_bad/venv/bin/python refresh_threads_token.py >> /var/www/uguis_bad/refresh_threads_token.log 2>&1'
"""

import os
import requests
import logging
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.getenv('ENV_PATH', '/var/www/uguis_bad/.env')
if not os.path.exists(ENV_PATH):
    ENV_PATH = os.path.join(BASE_DIR, '.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, 'refresh_threads_token.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def refresh_token(current_token: str) -> str | None:
    """Threadsトークンを更新して新しいトークンを返す"""
    try:
        r = requests.get(
            'https://graph.threads.net/refresh_access_token',
            params={
                'grant_type': 'th_refresh_token',
                'access_token': current_token,
            }
        )
        if not r.ok:
            logger.error(f'トークン更新失敗: {r.status_code} {r.text}')
            return None
        data = r.json()
        new_token = data.get('access_token')
        expires_in = data.get('expires_in', 0)
        days = expires_in // 86400
        logger.info(f'トークン更新成功（有効期限: 約{days}日）')
        return new_token
    except Exception as e:
        logger.error(f'トークン更新エラー: {e}')
        return None


def update_env_file(env_path: str, new_token: str) -> bool:
    """.envファイルのTHREADS_ACCESS_TOKENを書き換える"""
    try:
        with open(env_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        new_lines = []
        updated = False
        for line in lines:
            if line.startswith('THREADS_ACCESS_TOKEN='):
                new_lines.append(f'THREADS_ACCESS_TOKEN={new_token}\n')
                updated = True
            else:
                new_lines.append(line)

        if not updated:
            new_lines.append(f'THREADS_ACCESS_TOKEN={new_token}\n')

        with open(env_path, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)

        logger.info(f'.envファイルを更新しました: {env_path}')
        return True
    except Exception as e:
        logger.error(f'.env更新エラー: {e}')
        return False


def main():
    load_dotenv(ENV_PATH)
    current_token = os.getenv('THREADS_ACCESS_TOKEN')

    if not current_token:
        logger.error('THREADS_ACCESS_TOKENが設定されていません')
        return

    logger.info('Threadsトークン更新を開始します')
    new_token = refresh_token(current_token)

    if not new_token:
        logger.error('トークン更新に失敗しました')
        return

    if new_token == current_token:
        logger.info('トークンは変更なし（既に最新）')
        return

    if not update_env_file(ENV_PATH, new_token):
        logger.error('.envの書き換えに失敗しました')
        return

    logger.info('トークン自動更新が完了しました')


if __name__ == '__main__':
    main()

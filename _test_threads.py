import os, requests
from dotenv import load_dotenv
load_dotenv('/var/www/uguis_bad/.env')
token = os.getenv('THREADS_ACCESS_TOKEN')
r1 = requests.post('https://graph.threads.net/v1.0/me/threads',
    params={'media_type': 'TEXT', 'text': 'テスト投稿（自動投稿確認）', 'access_token': token})
print('コンテナ:', r1.status_code, r1.text[:200])
if r1.ok:
    cid = r1.json()['id']
    r2 = requests.post('https://graph.threads.net/v1.0/me/threads_publish',
        params={'creation_id': cid, 'access_token': token})
    print('公開:', r2.status_code, r2.text[:200])

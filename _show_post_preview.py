import os, re
from dotenv import load_dotenv
load_dotenv()
from datetime import datetime

SITE_URL = 'https://uguis-bad.shibuya8020.com'

schedule = {
    'schedule_id': 'abc123',
    'date': '2026-06-15',
    'day_of_week': '月',
    'start_time': '19:00',
    'end_time': '21:00',
    'venue': '越谷市立地域スポーツセンター',
    'court': 'A面(3面)',
    'max_participants': 20,
    'participants_count': 8,
}

date_str  = schedule['date']
dow       = schedule['day_of_week']
start     = schedule['start_time']
end       = schedule['end_time']
venue_raw = schedule['venue']
court     = schedule['court']
max_p     = int(schedule['max_participants'])
count_p   = int(schedule['participants_count'])
remaining = max_p - count_p

dt = datetime.strptime(date_str, '%Y-%m-%d')
date_disp  = dt.strftime('%m/%d').lstrip('0').replace('/0', '/') + f'({dow})'
court_disp = re.sub(r'\(.*?\)$', '', court).strip()

if remaining <= 0:
    slots = '満員御礼'
elif remaining <= 3:
    slots = f'残り{remaining}枠'
else:
    slots = f'残{remaining}枠 参加募集中！'

sid        = schedule['schedule_id']
detail_url = f'{SITE_URL}/schedule/{sid}/{date_str}'

print('=== 3日前投稿（X / Threads / うぐすたぐらむ 共通）===')
lines = [
    '鶯バドミントン',
    '参加者募集！',
    f'{date_disp} {start}〜{end}',
    f'{venue_raw} {court_disp}',
    '基礎打ちができて、ルールがわかればどなたでも参加できます。',
    '初級者～上級者レベルが違っても楽しくゲームできる方',
    slots,
    f'現在{count_p}名参加',
    detail_url,
]
print('\n'.join(lines))
print()
print('=== 当日投稿（X / Threads / うぐすたぐらむ 共通）===')
lines2 = [
    '今日はバドミントンです',
    '参加者募集！',
    f'{date_disp} {start}〜{end}',
    f'{venue_raw} {court_disp}',
    slots,
    f'現在{count_p}名参加',
    detail_url,
]
print('\n'.join(lines2))

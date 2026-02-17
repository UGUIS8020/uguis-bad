# view_game_results.py
import boto3
from boto3.dynamodb.conditions import Attr
from collections import defaultdict

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
table = dynamodb.Table('bad-game-results')

# 全件取得（ページネーション対応）
items = []
response = table.scan()
items.extend(response.get('Items', []))
while 'LastEvaluatedKey' in response:
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    items.extend(response.get('Items', []))

print(f"総試合結果数: {len(items)}件\n")

# match_id でグループ化
matches = defaultdict(list)
for item in items:
    matches[item.get('match_id', '???')].append(item)

# 日付順にソート
for match_id in sorted(matches.keys()):
    courts = sorted(matches[match_id], key=lambda x: x.get('court_number', 0))
    print(f"{'='*50}")
    print(f"試合ID: {match_id}  ({len(courts)}コート)")
    for c in courts:
        t1 = c.get('team1_score', '-')
        t2 = c.get('team2_score', '-')
        winner = c.get('winner', '?')
        team_a = ', '.join(p.get('display_name', '?') for p in c.get('team_a', []))
        team_b = ', '.join(p.get('display_name', '?') for p in c.get('team_b', []))
        print(f"  コート{c.get('court_number', '?')}: [{team_a}] vs [{team_b}]  {t1}-{t2}  勝者:{winner}")

print(f"\n{'='*50}")
print(f"試合数: {len(matches)}試合")

# 日付別サマリー
dates = defaultdict(int)
for match_id in matches:
    date = match_id[:8] if len(match_id) >= 8 else '不明'
    dates[date] += 1

print("\n日付別サマリー:")
for date in sorted(dates.keys()):
    print(f"  {date[:4]}-{date[4:6]}-{date[6:8]}: {dates[date]}試合")
import boto3
from collections import Counter, defaultdict

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
table = dynamodb.Table('bad-game-results')

TARGET = 'UGUIS渋谷'

items = []
resp = table.scan()
items.extend(resp.get('Items', []))
while 'LastEvaluatedKey' in resp:
    resp = table.scan(ExclusiveStartKey=resp['LastEvaluatedKey'])
    items.extend(resp.get('Items', []))

items.sort(key=lambda x: x.get('match_id', ''))

# TARGETが含まれるレコードを抽出
partner_count  = Counter()  # ペアパートナー
opponent_count = Counter()  # 対戦相手
match_log      = []         # 試合ログ

for item in items:
    team_a = item.get('team_a', [])
    team_b = item.get('team_b', [])
    names_a = [p.get('display_name', '') for p in team_a]
    names_b = [p.get('display_name', '') for p in team_b]

    if TARGET in names_a:
        my_team   = names_a
        opp_team  = names_b
    elif TARGET in names_b:
        my_team   = names_b
        opp_team  = names_a
    else:
        continue

    partner = [n for n in my_team if n != TARGET]
    for p in partner:
        partner_count[p] += 1
    for o in opp_team:
        opponent_count[o] += 1

    match_log.append({
        'match_id': item.get('match_id'),
        'court': item.get('court_number'),
        'partner': partner,
        'opponent': opp_team,
        'winner': item.get('winner'),
        'my_team': 'A' if TARGET in names_a else 'B',
        'score_a': item.get('team1_score'),
        'score_b': item.get('team2_score'),
    })

total = len(match_log)
wins  = sum(1 for m in match_log if m['winner'] == m['my_team'])

print("=" * 60)
print(f"  {TARGET} のペアリング分析")
print("=" * 60)
print(f"  総試合数: {total}  勝: {wins}  負: {total-wins}  勝率: {wins/total*100:.1f}%")
print()

print("【ペアパートナー（多い順）】")
for name, cnt in partner_count.most_common():
    rate = cnt / total * 100
    bar = '█' * cnt
    print(f"  {name:<20} {cnt:3d}回 ({rate:4.1f}%)  {bar}")

print()
print("【対戦相手（多い順）TOP15】")
for name, cnt in opponent_count.most_common(15):
    rate = cnt / total * 100
    bar = '█' * cnt
    print(f"  {name:<20} {cnt:3d}回 ({rate:4.1f}%)  {bar}")

print()
print("【直近10試合ログ】")
print(f"  {'match_id':<20} {'パートナー':<15} {'対戦相手':<25} {'結果'}")
print("  " + "─" * 70)
for m in match_log[-30:]:
    partner_str  = ', '.join(m['partner'])
    opponent_str = ', '.join(m['opponent'])
    result = '勝' if m['winner'] == m['my_team'] else '負'
    score  = f"{m['score_a']}-{m['score_b']}"
    print(f"  {m['match_id']:<20} {partner_str:<15} {opponent_str:<25} {result} ({score})")
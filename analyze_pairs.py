"""
bad-game-results からペアの重複率を分析し、理論値と比較する。
bad-game-results はコートごとに1レコードの構造。
使い方: python analyze_pairs.py
"""

import boto3
from collections import defaultdict

# ===== 設定 =====
EXCLUDE_PLAYERS = ['UGUIS渋谷']  # 管理者アカウントを除外
RECENT_N        = 3              # 「直近N試合以内の重複」を重複とみなす
DATE_FROM = '20260101'     # この日付以降のデータを対象（空文字で全件）
# ================

def count_partitions(n):
    result = 1
    for i in range(n - 1, 0, -2):
        result *= i
    return result

def pair_repeat_probability(n_active):
    if n_active < 2:
        return 0
    return count_partitions(n_active - 2) / count_partitions(n_active)

def fetch_results():
    dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
    table = dynamodb.Table('bad-game-results')
    items = []
    resp = table.scan()
    items.extend(resp.get('Items', []))
    while 'LastEvaluatedKey' in resp:
        resp = table.scan(ExclusiveStartKey=resp['LastEvaluatedKey'])
        items.extend(resp.get('Items', []))
    return items

def main():
    print("bad-game-results を取得中...")
    items = fetch_results()
    print(f"総レコード数: {len(items)}")

    # DATE_FROM でフィルタ
    if DATE_FROM:
        items = [i for i in items if i.get('match_id', '') >= DATE_FROM]
        print(f"  {DATE_FROM}以降: {len(items)}件")

    # match_id + court_number でソート
    items.sort(key=lambda x: (x.get('match_id', ''), x.get('court_number', '')))

    # ===== match_id ごとにコートをまとめる =====
    matches = defaultdict(list)
    for item in items:
        matches[item['match_id']].append(item)

    print(f"ユニーク試合数: {len(matches)}")

    # ===== 試合ごとにペアを抽出 =====
    match_pairs = []  # [(match_id, [frozenset, ...]), ...]

    for match_id in sorted(matches.keys()):
        courts = matches[match_id]
        pairs = []
        for court in courts:
            for team_key in ['team_a', 'team_b']:
                team = court.get(team_key, [])
                if len(team) == 2:
                    names = [p.get('display_name', '') for p in team]
                    if any(n in EXCLUDE_PLAYERS for n in names):
                        continue
                    pairs.append(frozenset(names))

        if pairs:
            match_pairs.append((match_id, pairs))

    print(f"ペアが取得できた試合数: {len(match_pairs)}")

    # ===== 直近N試合以内の重複チェック =====
    total_pairs   = 0
    repeat_pairs  = 0
    repeat_detail = defaultdict(int)
    pair_history  = []

    for match_id, pairs in match_pairs:
        recent = set()
        for past_pairs in pair_history[-RECENT_N:]:
            recent.update(past_pairs)

        for pair in pairs:
            total_pairs += 1
            if pair in recent:
                repeat_pairs += 1
                repeat_detail[pair] += 1

        pair_history.append(pairs)

    # ===== 理論値計算 =====
    theory_rates = []
    for _, pairs in match_pairs:
        n_active = len(pairs) * 2
        if n_active < 4:
            continue
        p = pair_repeat_probability(n_active)
        p_within_n = 1 - (1 - p) ** RECENT_N
        theory_rates.append(p_within_n * 100)

    expected_rate = sum(theory_rates) / len(theory_rates) if theory_rates else 0
    actual_rate   = repeat_pairs / total_pairs * 100 if total_pairs else 0

    # ===== 結果表示 =====
    print()
    print("=" * 60)
    print(f"  ペア重複率分析（直近{RECENT_N}試合以内を重複とみなす）")
    print("=" * 60)
    print(f"  対象試合数        : {len(match_pairs)}")
    print(f"  総ペア数          : {total_pairs}")
    print()
    print(f"  実際の重複ペア数  : {repeat_pairs} ({actual_rate:.1f}%)")
    print(f"  理論上の期待重複率: {expected_rate:.1f}%")
    print()
    if actual_rate > expected_rate * 1.3:
        print("  ⚠️  実際の重複率が理論値より30%以上高い → アルゴリズムに偏りあり")
    elif actual_rate > expected_rate * 1.1:
        print("  △  実際の重複率が理論値より若干高い → 許容範囲内だが注意")
    else:
        print("  ✅  実際の重複率は理論値と同等 → アルゴリズムは正常")
    print()

    print("【重複が多いペア TOP10】")
    for pair, cnt in sorted(repeat_detail.items(), key=lambda x: -x[1])[:10]:
        names = list(pair)
        print(f"  {names[0]} & {names[1]:<20} {cnt}回重複")

    print()
    print("【参考：人数別 理論重複率（純粋ランダム・直近3試合以内）】")
    for n in [8, 10, 12, 14, 16]:
        p = pair_repeat_probability(n)
        p3 = 1 - (1 - p) ** 3
        print(f"  {n:2d}人: 期待重複率 {p3*100:.1f}%")

if __name__ == '__main__':
    main()

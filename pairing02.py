import itertools
from typing import List, Dict, Tuple

def generate_optimal_pairs(players: List[Dict], previous_pairs: set = None) -> List[Tuple[Dict, Dict]]:
    """
    バドミントンのペアを最適に決定する関数。
    
    Args:
        players: [{'name': 'A', 'score': 70}, {'name': 'B', 'score': 60}, ...]
        previous_pairs: 過去にペアを組んだ人の組み合わせ（{('A', 'B'), ('C', 'D')} など）
    
    Returns:
        最適なペアのリスト（例：[(player1, player2), (player3, player4), ...]）
    """
    if previous_pairs is None:
        previous_pairs = set()

    # プレイヤー数が偶数であることを保証
    if len(players) % 2 != 0:
        raise ValueError("プレイヤー数は偶数である必要があります")

    # 全てのペアの組み合わせ
    all_pairs = list(itertools.combinations(players, 2))
    
    # ペアごとのスコア差（ばらつき）と履歴を考慮
    valid_pairs = []
    for p1, p2 in all_pairs:
        pair_names = tuple(sorted([p1['name'], p2['name']]))
        if pair_names not in previous_pairs:
            score_sum = p1['score'] + p2['score']
            score_diff = abs(p1['score'] - p2['score'])
            valid_pairs.append(((p1, p2), score_sum, score_diff))

    # スコア差とスコア合計のバランスをとってソート
    valid_pairs.sort(key=lambda x: (x[2], abs(x[1] - 100)))  # スコア差優先、次に合計点が平均的なもの

    # 最適なペアをGreedyで選出（プレイヤーの重複を避ける）
    used_names = set()
    result_pairs = []
    for (p1, p2), _, _ in valid_pairs:
        if p1['name'] not in used_names and p2['name'] not in used_names:
            result_pairs.append((p1, p2))
            used_names.update([p1['name'], p2['name']])

        if len(used_names) == len(players):
            break

    return result_pairs

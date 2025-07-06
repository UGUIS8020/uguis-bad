from flask import current_app
from flask_caching import logger
from trueskill import TrueSkill, Rating, rate
from decimal import Decimal
import random
import itertools
import boto3
from typing import List, Tuple, Dict, Set
from dataclasses import dataclass


# 環境設定
env = TrueSkill(draw_probability=0.0)  # 引き分けなし

def update_trueskill_for_players(result_item):
    """
    TrueSkill を使って各プレイヤーのスキルスコアを更新
    result_item = {
        "team_a": [{"user_id": ..., "display_name": ...}, ...],
        "team_b": [{"user_id": ..., "display_name": ...}, ...],
        "winner": "A" または "B"
    }
    """
    user_table = current_app.dynamodb.Table("bad-users")

    def get_team_ratings(team):
        """チームのTrueSkill Ratingを取得する"""
        ratings = []
        for player in team:
            user_id = player.get("user_id")
            user_data = get_user_data(user_id, user_table)  # ← ここ！
            if user_data:
                current_score = float(user_data.get("skill_score", 50))
                rating = Rating(mu=current_score, sigma=3)
                ratings.append((user_id, rating, current_score))
        return ratings

    ratings_a = get_team_ratings(result_item["team_a"])
    ratings_b = get_team_ratings(result_item["team_b"])
    winner = result_item.get("winner", "A")

    if not ratings_a or not ratings_b:
        current_app.logger.warning("⚠️ チームが空です。TrueSkill評価をスキップ")
        return

    if winner.upper() == "A":
        new_ratings = rate([[r for _, r, _ in ratings_a], [r for _, r, _ in ratings_b]])
    else:
        new_ratings = rate([[r for _, r, _ in ratings_b], [r for _, r, _ in ratings_a]])
        new_ratings = new_ratings[::-1]

    new_ratings_a, new_ratings_b = new_ratings

    def save(team_ratings, new_ratings, label):
        for (user_id, old_rating, name), new_rating in zip(team_ratings, new_ratings):
            new_score = round(new_rating.mu, 2)
            delta = round(new_rating.mu - old_rating.mu, 2)
            user_table.update_item(
                Key={"user#user_id": user_id},
                UpdateExpression="SET skill_score = :s",
                ExpressionAttributeValues={":s": Decimal(str(new_score))}
            )
            current_app.logger.info(f"[{label}] {name}: {old_rating.mu:.2f} → {new_score:.2f}（Δ{delta:+.2f}）")

    save(ratings_a, new_ratings_a, "Team A")
    save(ratings_b, new_ratings_b, "Team B")

def parse_players(team):
    """文字列 or 辞書が混在しているチームデータを統一フォーマットに変換"""
    parsed = []
    for p in team:
        if isinstance(p, str):
            # 古い形式：user_id のみ
            parsed.append({"user_id": p, "display_name": "", "skill_score": 50})
        elif isinstance(p, dict):
            parsed.append({
                "user_id": p.get("user_id"),
                "display_name": p.get("display_name", ""),
                "skill_score": int(p.get("skill_score", 50))
            })
    return parsed

def get_user_data(user_id, user_table):
    try:
        response = user_table.get_item(Key={"user#user_id": user_id})
        return response.get("Item")
    except Exception as e:
        current_app.logger.warning(f"[get_user_data ERROR] user_id={user_id}: {e}")
        return None
    




@dataclass
class Player:
    name: str
    level: int  # 30-100
    gender: str  # 'M' または 'F'
    
    def __str__(self):
        return f"{self.name}({self.level}点/{self.gender})"

class BadmintonPairing:
    def __init__(self, players: List[Player]):
        self.players = players
        self.used_pairs: Set[Tuple[str, str]] = set()
        self.match_history: List[List[Tuple[Tuple[Player, Player], Tuple[Player, Player]]]] = []
        
    def pair_compatibility_score(self, p1: Player, p2: Player) -> float:
        """ペアの相性スコアを計算（低いほど良い）"""
        level_diff = abs(p1.level - p2.level)
        level_penalty = level_diff * 2

        # 性別バランスを確認してボーナスを調整
        num_male = sum(1 for p in self.players if p.gender == 'M')
        num_female = sum(1 for p in self.players if p.gender == 'F')

        gender_diff = abs(num_male - num_female)
        total_players = len(self.players)

        # 男女比がバランスしていればボーナス大、偏っていれば小
        if gender_diff <= 2:
            gender_bonus = -10 if p1.gender != p2.gender else 0
        elif gender_diff <= total_players // 4:
            gender_bonus = -5 if p1.gender != p2.gender else 0
        else:
            gender_bonus = 0  # 偏りが大きすぎる場合はボーナスなし

        return level_penalty + gender_bonus
    
    def match_balance_score(self, pair1: Tuple[Player, Player], pair2: Tuple[Player, Player]) -> float:
        """試合バランススコアを計算（低いほど良い）"""
        team1_total = pair1[0].level + pair1[1].level
        team2_total = pair2[0].level + pair2[1].level
        
        # チーム間のレベル差
        team_diff = abs(team1_total - team2_total)
        
        return team_diff
    
    def is_pair_used(self, p1: Player, p2: Player) -> bool:
        """このペアが過去に使用されたかチェック"""
        pair_key = tuple(sorted([p1.name, p2.name]))
        return pair_key in self.used_pairs
    
    def add_used_pair(self, p1: Player, p2: Player):
        """使用済みペアを記録"""
        pair_key = tuple(sorted([p1.name, p2.name]))
        self.used_pairs.add(pair_key)
    
    def generate_all_possible_pairs(self) -> List[Tuple[Player, Player]]:
        """全ての可能なペアを生成"""
        return list(itertools.combinations(self.players, 2))
    
    def generate_best_pairs_for_round(self, available_players):
        all_possible_pairs = list(itertools.combinations(available_players, 2))

        # ✅ 1. 使用済みペアを除外（第一優先）
        filtered_pairs = [
            (p1, p2) for p1, p2 in all_possible_pairs
            if not self.is_pair_used(p1, p2)
        ]

        if len(filtered_pairs) < 3:  # 🔁 組めるペアが少なければ fallback
            print("⚠️ 使用済みペアが多すぎるため、すべてのペアから生成します")
            filtered_pairs = all_possible_pairs  # ← 全ペア使う

        # ✅ 2. スコア差などでペア適性を評価
        scored_pairs = [
            ((p1, p2), self.pair_compatibility_score(p1, p2))
            for p1, p2 in filtered_pairs
        ]

        # ✅ 3. スコア順でソート
        scored_pairs.sort(key=lambda x: x[1])

        # ✅ 4. ペアを決定（重複なし）
        used_ids = set()
        selected_pairs = []
        for (p1, p2), score in scored_pairs:
            if p1.user_id in used_ids or p2.user_id in used_ids:
                continue
            selected_pairs.append((p1, p2))
            used_ids.update([p1.user_id, p2.user_id])

        return selected_pairs
    
    def generate_best_pairs_for_round(self, available_players):
        all_possible_pairs = list(itertools.combinations(available_players, 2))

        # 使用済みペアを除外
        filtered_pairs = [
            (p1, p2) for p1, p2 in all_possible_pairs
            if not self.is_pair_used(p1, p2)
        ]

        # 使用済みペアが多すぎる場合はすべて使う
        if len(filtered_pairs) < 3:
            print("⚠️ 使用済みペアが多すぎるため、すべてのペアから生成します")
            filtered_pairs = all_possible_pairs

        # スコア差でソート（ペア適性スコア）
        scored_pairs = [
            ((p1, p2), self.pair_compatibility_score(p1, p2))
            for p1, p2 in filtered_pairs
        ]
        scored_pairs.sort(key=lambda x: x[1])

        # 使用済みプレイヤー除外しながらペア選出
        used_ids = set()
        selected_pairs = []
        for (p1, p2), score in scored_pairs:
            if p1.name in used_ids or p2.name in used_ids:
                continue
            selected_pairs.append((p1, p2))
            used_ids.update([p1.name, p2.name])

        return selected_pairs
    
    def generate_tournament_schedule(self, num_rounds: int = 7, courts_per_round: int = 3) -> List[List[Tuple[Tuple[Player, Player], Tuple[Player, Player]]]]:
        """トーナメント全体のスケジュールを生成"""
        tournament_schedule = []
        players_per_round = courts_per_round * 4  # 1コート4人
        
        for round_num in range(num_rounds):
            print(f"\n=== 第{round_num + 1}ラウンド ===")
            
            # 参加プレイヤーを選択（全員参加または一部参加）
            if len(self.players) <= players_per_round:
                round_players = self.players  # 全員参加
            else:
                # 簡単な選択アルゴリズム：試合数が少ない人を優先
                # 実際の実装では、より複雑な選択ロジックが必要
                round_players = random.sample(self.players, players_per_round)
            
            print(f"参加プレイヤー数: {len(round_players)}人")
            
            try:
                # ペア生成
                pairs = self.generate_best_pairs_for_round(round_players)
                print(f"生成されたペア数: {len(pairs)}ペア")
                
                # ペアを記録
                for p1, p2 in pairs:
                    self.add_used_pair(p1, p2)
                
                # 試合生成
                matches = self.generate_matches_for_round(pairs)
                tournament_schedule.append(matches)
                
                # 結果表示
                for i, ((p1, p2), (p3, p4)) in enumerate(matches):
                    team1_total = p1.level + p2.level
                    team2_total = p3.level + p4.level
                    diff = abs(team1_total - team2_total)
                    
                    print(f"コート{i+1}: [{p1} & {p2}] vs [{p3} & {p4}]")
                    print(f"         レベル合計: {team1_total} vs {team2_total} (差: {diff})")
            
            except Exception as e:
                print(f"ラウンド{round_num + 1}の生成に失敗: {e}")
                break
        
        self.match_history = tournament_schedule
        return tournament_schedule
    
    def generate_pairs_with_rest_handling(self, all_players: List[Player]) -> Tuple[List[Tuple[Player, Player]], List[Player]]:
        """奇数プレイヤーに対応したペア生成（未出場者を優先）"""
        if len(all_players) < 4:
            raise ValueError("プレイヤー数は4人以上である必要があります")

        import random

        # match_count=0 の人を優先的に出場させる
        never_played = [p for p in all_players if getattr(p, 'match_count', 0) == 0]
        others = [p for p in all_players if p not in never_played]

        # アクティブプレイヤー候補を作る
        active_players = never_played + others
        active_players = active_players[:len(active_players) - (len(active_players) % 2)]  # 偶数にする

        # 残りは休憩にする
        resting_players = [p for p in all_players if p not in active_players]

        print("✅ アクティブプレイヤー:")
        for p in active_players:
            print(f" - {p.name} (試合: {p.match_count}, 休憩: {p.rest_count})")

        print("🛌 休憩プレイヤー:")
        for p in resting_players:
            print(f" - {p.name} (試合: {p.match_count}, 休憩: {p.rest_count})")

        # ペア生成
        pairs = self.generate_best_pairs_for_round(active_players)

        return pairs, resting_players    
    
    def generate_matches_for_round(self, pairs: List[Tuple[Player, Player]]) -> List[Tuple[Tuple[Player, Player], Tuple[Player, Player]]]:
        """
        ペアを使って試合（2ペア1試合）を作成する。
        余ったペアは無視する。
        """
        matches = []
        for i in range(0, len(pairs) - 1, 2):
            pair1 = pairs[i]
            pair2 = pairs[i + 1]
            matches.append((pair1, pair2))
        return matches
    
def load_used_pairs_from_dynamodb(table_name: str, pairing: BadmintonPairing):
    """DynamoDBから当日の試合ペアを読み込み、used_pairs に追加する"""
    from datetime import date
    import boto3

    dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")
    table = dynamodb.Table(table_name)

    response = table.scan()
    items = response.get("Items", [])

    today_str = date.today().strftime("%Y%m%d")

    for item in items:
        match_id = item.get("match_id", "")
        if not match_id.startswith(today_str):
            continue

        for team_key in ["team_a", "team_b"]:
            team = item.get(team_key, [])
            if len(team) == 2:
                try:
                    p1_name = team[0]["display_name"]
                    p2_name = team[1]["display_name"]
                    pairing.add_used_pair(Player(p1_name, 0, "M"), Player(p2_name, 0, "M"))  # スコア/性別は仮
                except Exception as e:
                    print(f"[⚠️ ペア履歴読み込み失敗] {e}")

def generate_balanced_pairs_and_matches(players: List[Player], max_courts: int) -> Tuple[
    List[Tuple[Player, Player]],  # all pairs
    List[Tuple[Tuple[Player, Player], Tuple[Player, Player]]],  # matches
    List[Player]  # waiting players
]:
    """
    プレイヤー一覧からペアをランダムに作成し、スキルバランスが取れた試合を組む。
    """
    # ステップ①：まずランダムにペアを作る（この関数は既に正しく機能している）
    pairs, _, waiting_players = generate_random_pairs_and_matches(players, max_courts)

    # ステップ②：ペアからスキルが近い同士でマッチを組む
    matches, unused_pairs = generate_matches_by_pair_skill_balance(pairs, max_courts)

    # ステップ③：使われなかったペアのメンバーも待機者として追加
    for pair in unused_pairs:
        waiting_players.extend(pair)

    return pairs, matches, waiting_players



def generate_random_pairs_and_matches(
    players: List[Player],
    max_courts: int
) -> Tuple[List[Tuple[Player, Player]], List[Tuple[Tuple[Player, Player], Tuple[Player, Player]]], List[Player]]:
    """
    entry_status が pending のプレイヤーだけを対象に
    完全ランダムでペアとマッチを作成する。
    """
    import logging
    logger = logging.getLogger("generate_random_pairs")

    # 🔀 シャッフル前のプレイヤー
    logger.info(f"[START] プレイヤー数: {len(players)}")
    logger.info("▶ シャッフル前: " + ", ".join([p.name for p in players]))

    # シャッフル
    random.shuffle(players)   
    
    logger.info("▶ シャッフル後: " + ", ".join([p.name for p in players]))

    # ➗ 最大試合数と使用するプレイヤー数
    possible_matches = len(players) // 4
    used_courts = min(possible_matches, max_courts)
    total_required_players = used_courts * 4
    required_pairs = used_courts * 2
    logger.info(f"▶ 使用コート: {used_courts}, 必要人数: {total_required_players}, 必要ペア数: {required_pairs}")

    # 🤝 ペア作成
    pairs = []
    for i in range(0, len(players) - 1, 2):
        pairs.append((players[i], players[i + 1]))
    logger.info(f"▶ 作成ペア数: {len(pairs)}")

    # 🙋 余った人数（奇数・余剰）
    waiting_players = []
    if len(players) % 2 == 1:
        waiting_players.append(players[-1])
        logger.info(f"▶ 奇数のため余った1人: {players[-1].name}")

    # 🎮 試合構成
    used_pairs = pairs[:required_pairs]
    matches = []
    for i in range(0, len(used_pairs) - 1, 2):
        matches.append((used_pairs[i], used_pairs[i + 1]))

    logger.info(f"▶ 作成試合数: {len(matches)}")

    # 💡 使われなかったペアから待機者を追加
    unused_pairs = pairs[required_pairs:]
    for pair in unused_pairs:
        waiting_players.extend(pair)
        logger.info(f"▶ 待機に回されたペア: {pair[0].name}, {pair[1].name}")

    logger.info(f"▶ 最終待機者: {', '.join([p.name for p in waiting_players])}")
    logger.info("[END]")

    return pairs, matches, waiting_players

def generate_matches_by_pair_skill_balance(pairs: List[Tuple[Player, Player]], max_courts: int) -> Tuple[List[Tuple[Tuple[Player, Player], Tuple[Player, Player]]], List[Tuple[Player, Player]]]:
    """
    ペア同士のスキル合計が近いように、試合を組む（1試合=2ペア）。
    余ったペアは試合に使わない（→待機として返す）。
    """
    # 各ペアのスキル合計を算出
    scored_pairs = [(pair, pair[0].level + pair[1].level) for pair in pairs]

    # スキル順に並べて、近いもの同士をペア化
    scored_pairs.sort(key=lambda x: x[1])

    max_matches = min(len(scored_pairs) // 2, max_courts)
    matches = []
    used_indices = set()

    i = 0
    while len(matches) < max_matches and i + 1 < len(scored_pairs):
        pair1 = scored_pairs[i][0]
        pair2 = scored_pairs[i + 1][0]
        matches.append((pair1, pair2))
        used_indices.update([i, i + 1])
        i += 2

    # 使用されなかったペアは待機として返す
    unused_pairs = [scored_pairs[i][0] for i in range(len(scored_pairs)) if i not in used_indices]

    return matches, unused_pairs

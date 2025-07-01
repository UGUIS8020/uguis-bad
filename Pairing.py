import random
import itertools
from typing import List, Tuple, Dict, Set
from dataclasses import dataclass

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
        # レベル差のペナルティ
        level_diff = abs(p1.level - p2.level)
        level_penalty = level_diff * 2
        
        # 性別組み合わせのボーナス/ペナルティ
        gender_bonus = 0
        if p1.gender != p2.gender:  # 男女ペア
            gender_bonus = -10  # ボーナス（スコアを下げる）
        
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
    
    def generate_best_pairs_for_round(self, available_players: List[Player]) -> List[Tuple[Player, Player]]:
        """1ラウンド分の最適なペアを生成"""
        if len(available_players) % 2 != 0:
            raise ValueError("プレイヤー数は偶数である必要があります")
        
        all_possible_pairs = list(itertools.combinations(available_players, 2))
        
        # 使用済みペアを除外し、スコア順にソート
        available_pairs = []
        for pair in all_possible_pairs:
            if not self.is_pair_used(pair[0], pair[1]):
                score = self.pair_compatibility_score(pair[0], pair[1])
                available_pairs.append((pair, score))
        
        # スコアでソート（低いほど良い）
        available_pairs.sort(key=lambda x: x[1])
        
        # 貪欲法でペアを選択
        selected_pairs = []
        used_players = set()
        
        for (p1, p2), score in available_pairs:
            if p1.name not in used_players and p2.name not in used_players:
                selected_pairs.append((p1, p2))
                used_players.add(p1.name)
                used_players.add(p2.name)
                
                if len(selected_pairs) * 2 == len(available_players):
                    break
        
        return selected_pairs
    
    def generate_matches_for_round(self, pairs: List[Tuple[Player, Player]]) -> List[Tuple[Tuple[Player, Player], Tuple[Player, Player]]]:
        """ペアから試合を生成"""
        if len(pairs) % 2 != 0:
            raise ValueError("ペア数は偶数である必要があります")
        
        matches = []
        available_pairs = pairs.copy()
        
        while len(available_pairs) >= 2:
            best_match = None
            best_score = float('inf')
            best_indices = None
            
            # 全ての組み合わせを試してベストマッチを見つける
            for i in range(len(available_pairs)):
                for j in range(i + 1, len(available_pairs)):
                    pair1, pair2 = available_pairs[i], available_pairs[j]
                    score = self.match_balance_score(pair1, pair2)
                    
                    if score < best_score:
                        best_score = score
                        best_match = (pair1, pair2)
                        best_indices = (i, j)
            
            if best_match:
                matches.append(best_match)
                # 使用したペアを削除（逆順で削除）
                available_pairs.pop(best_indices[1])
                available_pairs.pop(best_indices[0])
        
        return matches
    
    def generate_tournament_schedule(self, num_rounds: int = 7, courts_per_round: int = 3) -> List[List[Tuple[Tuple[Player, Player], Tuple[Player, Player]]]]:
        """トーナメント全体のスケジュールを生成"""
        tournament_schedule = []
        players_per_round = courts_per_round * 4  # 1コート4人
        
        for round_num in range(num_rounds):
            print(f"\n=== 第{round_num + 1}ラウンド ===")
            
            # 参加プレイヤーを選択（全員参加または一部参加）
            if len(self.players) == players_per_round:
                round_players = self.players
            else:
                # 簡単な選択アルゴリズム：試合数が少ない人を優先
                # 実際の実装では、より複雑な選択ロジックが必要
                round_players = random.sample(self.players, min(players_per_round, len(self.players)))
            
            try:
                # ペア生成
                pairs = self.generate_best_pairs_for_round(round_players)
                
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
    
    def print_statistics(self):
        """統計情報を表示"""
        print(f"\n=== 統計情報 ===")
        print(f"総ラウンド数: {len(self.match_history)}")
        print(f"使用済みペア数: {len(self.used_pairs)}")
        
        # 各プレイヤーの試合数をカウント
        player_match_count = {player.name: 0 for player in self.players}
        
        for round_matches in self.match_history:
            for ((p1, p2), (p3, p4)) in round_matches:
                player_match_count[p1.name] += 1
                player_match_count[p2.name] += 1
                player_match_count[p3.name] += 1
                player_match_count[p4.name] += 1
        
        print("\n各プレイヤーの試合数:")
        for name, count in player_match_count.items():
            print(f"{name}: {count}試合")

# 使用例
def main():
    # サンプルプレイヤー（16人）
    players = [
        Player("田中", 85, "M"), Player("佐藤", 78, "F"), Player("鈴木", 92, "M"), Player("高橋", 65, "F"),
        Player("伊藤", 88, "M"), Player("渡辺", 72, "F"), Player("山本", 95, "M"), Player("中村", 58, "F"),
        Player("小林", 80, "M"), Player("加藤", 67, "F"), Player("吉田", 90, "M"), Player("山田", 75, "F"),
        Player("佐々木", 83, "M"), Player("山口", 70, "F"), Player("松本", 87, "M"), Player("井上", 62, "F")
    ]
    
    # トーナメント生成
    tournament = BadmintonPairing(players)
    schedule = tournament.generate_tournament_schedule(num_rounds=7, courts_per_round=3)
    
    # 統計表示
    tournament.print_statistics()

if __name__ == "__main__":
    main()
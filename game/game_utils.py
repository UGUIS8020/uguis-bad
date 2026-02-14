from flask import current_app
from trueskill import TrueSkill
import random
from typing import List, Tuple, Dict, Set
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
import logging
from botocore.exceptions import ClientError
from typing import Optional

# 環境設定
env = TrueSkill(draw_probability=0.0)  # 引き分けなし

JST = ZoneInfo("Asia/Tokyo")

META_PK = "meta#current"

def _now_jst_iso():
    return datetime.now(JST).isoformat()

def get_current_match_meta():
    table = current_app.dynamodb.Table("bad-game-matches")
    resp = table.get_item(Key={"match_id": META_PK}, ConsistentRead=True)
    return resp.get("Item")

def get_current_match_id():
    meta = get_current_match_meta()
    if not meta:
        return None
    if meta.get("status") != "playing":
        return None
    return meta.get("current_match_id")

def start_match_meta(new_match_id: str, court_count: int):
    table = current_app.dynamodb.Table("bad-game-matches")
    now = _now_jst_iso()

    # 「今 playing じゃないときだけ開始OK」にする
    try:
        table.update_item(
            Key={"match_id": META_PK},
            UpdateExpression=(
                "SET entity_type=:e, "
                "current_match_id=:m, "
                "#st=:playing, "
                "court_count=:c, "
                "created_at=if_not_exists(created_at, :now), "
                "updated_at=:now, "
                "version=if_not_exists(version, :zero) + :one"
            ),
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={
                ":e": "meta",
                ":m": new_match_id,
                ":playing": "playing",
                ":c": int(court_count),
                ":now": now,
                ":zero": 0,
                ":one": 1,
            },
            ConditionExpression=(
                "attribute_not_exists(#st) OR #st <> :playing"
            ),
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            current_app.logger.warning("[meta] start blocked: already playing")
            return False
        raise

def finish_match_meta(match_id: str):
    table = current_app.dynamodb.Table("bad-game-matches")
    now = _now_jst_iso()
    try:
        table.update_item(
            Key={"match_id": META_PK},
            UpdateExpression=(
                "SET #st=:finished, "
                "updated_at=:now, "
                "version=if_not_exists(version, :zero) + :one"
            ),
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={
                ":finished": "finished",
                ":now": now,
                ":zero": 0,
                ":one": 1,
                ":m": match_id,
            },
            # 「current_match_id が一致している」ことを保証
            ConditionExpression="current_match_id = :m",
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            current_app.logger.warning("[meta] finish blocked: current_match_id mismatch")
            return False
        raise

def update_trueskill_for_players_and_return_updates(result_item):
    """
    TrueSkill を使って各プレイヤーのスキルスコアを更新し、更新結果を返す
    """
    from decimal import Decimal
    from datetime import datetime
    from trueskill import Rating, rate
    
    updated_skills = {}  # 更新されたスキルスコアを格納する辞書
    user_table = current_app.dynamodb.Table("bad-users")

    def get_team_ratings(team):
        """チームのTrueSkill Ratingを取得する"""
        ratings = []
        for player in team:
            user_id = player.get("user_id")
            if not user_id:
                current_app.logger.warning(f"ユーザーIDが空です: {player}")
                continue
                
            # DynamoDBから直接ユーザーデータを取得する
            try:
                current_app.logger.info(f"ユーザー {user_id} の取得を試みます（キー: user#user_id）")
                response = user_table.get_item(Key={"user#user_id": user_id})
                user_data = response.get("Item")
                
                if user_data:
                    current_app.logger.info(f"ユーザー {user_id} のデータ取得成功")
                    # muとsigmaの両方を取得（デフォルト値も設定）
                    current_mu = float(user_data.get("skill_score", 25.0))
                    current_sigma = float(user_data.get("skill_sigma", 8.333))
                    
                    rating = Rating(mu=current_mu, sigma=current_sigma)
                    display_name = player.get("display_name", user_data.get("display_name", "不明"))
                    ratings.append((user_id, rating, display_name))
                    current_app.logger.info(
                        f"ユーザー {user_id} ({display_name}) μ={current_mu:.2f}, σ={current_sigma:.4f}"
                    )
                else:
                    current_app.logger.warning(f"ユーザーが見つかりません: {user_id}")
            except Exception as e:
                current_app.logger.error(f"ユーザーデータ取得エラー: {str(e)}")
        
        return ratings

    ratings_a = get_team_ratings(result_item["team_a"])
    ratings_b = get_team_ratings(result_item["team_b"])
    winner = result_item.get("winner", "A")

    if not ratings_a or not ratings_b:
        current_app.logger.warning("⚠️ チームが空です。TrueSkill評価をスキップ")
        return updated_skills

    try:
        # スキル差の計算
        team_a_skill = sum(r.mu for _, r, _ in ratings_a) / len(ratings_a)
        team_b_skill = sum(r.mu for _, r, _ in ratings_b) / len(ratings_b)
        skill_diff = team_a_skill - team_b_skill
        
        # 期待される勝者を判定
        expected_winner = "A" if skill_diff > 0 else "B"
        actual_winner = winner.upper()
        
        # 標準のTrueSkill計算
        if winner.upper() == "A":
            original_new_ratings = rate([[r for _, r, _ in ratings_a], [r for _, r, _ in ratings_b]])
        else:
            original_new_ratings = rate([[r for _, r, _ in ratings_b], [r for _, r, _ in ratings_a]])
            original_new_ratings = original_new_ratings[::-1]
        
        # スキル差と勝敗の整合性を正しく計算
        if expected_winner == actual_winner:
            # 予想通りの結果: 正の値（調整を小さく）
            skill_result_consistency = abs(skill_diff)
        else:
            # 番狂わせ: 負の値（調整を大きく）
            skill_result_consistency = -abs(skill_diff)

        # スコア差の計算
        team1_score = int(result_item.get("team1_score", 0))
        team2_score = int(result_item.get("team2_score", 0))
        if winner.upper() == "A":
            score_diff = team1_score - team2_score
        else:
            score_diff = team2_score - team1_score
        
        # 基本調整係数（スコア差だけに基づく）
        min_factor = 0.8
        max_factor = 1.5
        max_diff = 20.0
        
        score_adjustment = min_factor + (max_factor - min_factor) * min(abs(score_diff) / max_diff, 1.0)
        
        # スキル差と結果の整合性に基づく追加調整
        # skill_result_consistency が負の値（予想外の結果）なら調整係数を大きく
        # skill_result_consistency が正の値（予想通りの結果）なら調整係数を小さく
        max_skill_diff = 15.0  # 想定される最大スキル差
        consistency_factor = 1.0 - min(max(skill_result_consistency / max_skill_diff, -1.0), 1.0) * 0.3
        
        # 最終的な調整係数
        final_adjustment = score_adjustment * consistency_factor
        
        current_app.logger.info(f"スコア差: {score_diff}, チームスキル差: {skill_diff:.2f}, " +
                            f"結果整合性: {skill_result_consistency:.2f}, " +
                            f"スコア調整: {score_adjustment:.2f}, " +
                            f"整合性調整: {consistency_factor:.2f}, " +
                            f"最終調整係数: {final_adjustment:.2f}")
        
        # 調整係数を適用した新しいレーティングを作成
        new_ratings_a = []
        new_ratings_b = []
        
        for i, rating in enumerate(original_new_ratings[0]):
            old_mu = ratings_a[i][1].mu
            delta = rating.mu - old_mu
            adjusted_delta = delta * final_adjustment
            new_ratings_a.append(Rating(mu=old_mu + adjusted_delta, sigma=rating.sigma))
        
        for i, rating in enumerate(original_new_ratings[1]):
            old_mu = ratings_b[i][1].mu
            delta = rating.mu - old_mu
            adjusted_delta = delta * final_adjustment
            new_ratings_b.append(Rating(mu=old_mu + adjusted_delta, sigma=rating.sigma))

    except Exception as e:
        current_app.logger.error(f"TrueSkill計算エラー: {str(e)}")
        import traceback
        current_app.logger.error(traceback.format_exc())
        return updated_skills

    def save(team_ratings, new_ratings, team_players, label):
        for i, ((user_id, old_rating, display_name), new_rating) in enumerate(zip(team_ratings, new_ratings)):
            new_mu = round(new_rating.mu, 2)
            new_sigma = round(new_rating.sigma, 4)
            delta_mu = round(new_rating.mu - old_rating.mu, 2)
            delta_sigma = round(new_rating.sigma - old_rating.sigma, 4)
            
            try:
                # muとsigmaの両方を保存
                user_table.update_item(
                    Key={"user#user_id": user_id},
                    UpdateExpression="SET skill_score = :mu, skill_sigma = :sigma, updated_at = :t",
                    ExpressionAttributeValues={
                        ":mu": Decimal(str(new_mu)),
                        ":sigma": Decimal(str(new_sigma)),
                        ":t": datetime.now().isoformat()
                    }
                )
                current_app.logger.info(
                    f"[{label}] {display_name}: μ {old_rating.mu:.2f}→{new_mu:.2f} (Δ{delta_mu:+.2f}), "
                    f"σ {old_rating.sigma:.4f}→{new_sigma:.4f} (Δ{delta_sigma:+.4f})"
                )
                
                # 更新されたスキルスコアを記録
                # チームプレイヤーからentry_idを検索
                for player in team_players:
                    if player.get("user_id") == user_id:
                        entry_id = player.get("entry_id")
                        updated_skills[user_id] = {
                            "skill_score": new_mu,
                            "skill_sigma": new_sigma,
                            "display_name": display_name,
                            "entry_id": entry_id
                        }
                        break
                
            except Exception as e:
                current_app.logger.error(f"スコア更新エラー: {user_id} {str(e)}")

    save(ratings_a, new_ratings_a, result_item["team_a"], "Team A")
    save(ratings_b, new_ratings_b, result_item["team_b"], "Team B")
    
    return updated_skills


def sync_match_entries_with_updated_skills(entry_mapping, updated_skills):
    """
    更新されたスキルスコアでmatch_entriesテーブルを同期する
    """
    from decimal import Decimal
    
    match_table = current_app.dynamodb.Table("bad-game-match_entries")
    sync_count = 0
    
    try:
        current_app.logger.info(f"エントリーテーブル同期開始: {len(updated_skills)}件のスキルスコア更新")
        
        for user_id, data in updated_skills.items():
            entry_id = data.get("entry_id") or entry_mapping.get(user_id)
            
            if not entry_id:
                current_app.logger.warning(f"ユーザー {user_id} のエントリーIDが見つかりません")
                continue
                
            try:
                # 👇 skill_sigmaも追加
                match_table.update_item(
                    Key={"entry_id": entry_id},
                    UpdateExpression="SET skill_score = :mu, skill_sigma = :sigma",
                    ExpressionAttributeValues={
                        ":mu": Decimal(str(data["skill_score"])),
                        ":sigma": Decimal(str(data["skill_sigma"]))
                    }
                )
                sync_count += 1
                current_app.logger.debug(
                    f"エントリー更新: {entry_id}, ユーザー: {data.get('display_name')}, "
                    f"μ={data['skill_score']:.2f}, σ={data['skill_sigma']:.4f}"
                )
            except Exception as e:
                current_app.logger.error(f"エントリー更新エラー: {entry_id} - {str(e)}")
    
    except Exception as e:
        current_app.logger.error(f"エントリー同期エラー: {str(e)}")
    
    return sync_count


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
    level: float  # 保守的スキル（μ - 3σ）で計算される
    gender: str  # 'M' または 'F'
    skill_score: Optional[float] = None  # μ（平均スキル）
    skill_sigma: Optional[float] = None  # σ（不確実性）
    
    def __str__(self):
        return f"{self.name}({self.level:.1f}点/{self.gender})"
    
    @property
    def conservative_skill(self) -> float:
        """保守的スキル推定値（ペアリング用）"""
        if self.skill_score is not None and self.skill_sigma is not None:
            return self.skill_score - 3 * self.skill_sigma
        return self.level


def generate_balanced_pairs_and_matches(players: List[Player], max_courts: int) -> Tuple[
    List[Tuple[Player, Player]],  # all pairs
    List[Tuple[Tuple[Player, Player], Tuple[Player, Player]]],  # matches
    List[Player]  # waiting players
]:
    """
    プレイヤー一覧からペアをランダムに作成し、スキルバランスが取れた試合を組む。
    """
    # ステップ①：まずランダムにペアを作る
    pairs, waiting_players = generate_random_pairs(players)

    # ステップ②：ペアからスキルが近い同士でマッチを組む
    matches, unused_pairs = generate_matches_by_pair_skill_balance(pairs, max_courts)

    # ステップ③：使われなかったペアのメンバーも待機者として追加
    for pair in unused_pairs:
        waiting_players.extend(pair)

    return pairs, matches, waiting_players

def _names_sample(players: List["Player"], n: int = 12) -> str:
    """ログ用：先頭n人だけ名前を出す（多い時は ... を付ける）"""
    names = [p.name for p in players]
    if len(names) <= n:
        return ", ".join(names)
    return ", ".join(names[:n]) + f", ... (+{len(names)-n})"

def generate_random_pairs(players: List["Player"]) -> Tuple[List[Tuple["Player", "Player"]], List["Player"]]:
    """
    プレイヤーリストから完全ランダムでペアを作成する。
    奇数の場合は最後のプレイヤーを待機リストに入れる。
    ※元の players は変更しない
    """
    logger = logging.getLogger("generate_random_pairs")

    # INFO: 要約だけ（普段の運用）
    logger.info("[pairs] start n=%d", len(players))

    shuffled = players.copy()
    random.shuffle(shuffled)

    # DEBUG: 詳細（必要な時だけ）
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("[pairs] input  : %s", _names_sample(players, n=16))
        logger.debug("[pairs] shuffled: %s", _names_sample(shuffled, n=16))

    pairs: List[Tuple["Player", "Player"]] = []
    for i in range(0, len(shuffled) - 1, 2):
        pairs.append((shuffled[i], shuffled[i + 1]))

    waiting_players: List["Player"] = []
    if len(shuffled) % 2 == 1:
        waiting_players.append(shuffled[-1])

    # INFO: 結果要約
    if waiting_players:
        logger.info("[pairs] made=%d waiting=1 (%s)", len(pairs), waiting_players[0].name)
    else:
        logger.info("[pairs] made=%d waiting=0", len(pairs))

    return pairs, waiting_players

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

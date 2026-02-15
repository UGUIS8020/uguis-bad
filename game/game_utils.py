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
    from decimal import Decimal
    from trueskill import Rating, rate
    
    updated_skills = {}
    user_table = current_app.dynamodb.Table("bad-users")

    def safe_get_score(item, keys):
        for k in keys:
            val = item.get(k)
            if val is not None:
                try: return int(float(val))
                except: continue
        return 0

    t1 = safe_get_score(result_item, ["team1_score", "team_a_score", "score1"])
    t2 = safe_get_score(result_item, ["team2_score", "team_b_score", "score2"])
    winner = str(result_item.get("winner", "A")).upper()
    score_diff = (t1 - t2) if winner == "A" else (t2 - t1)

    # 【削除】RawItemのログを削除（中身が巨大なため）

    def get_team_ratings(team):
        ratings = []
        for player in team:
            uid = player.get("user_id")
            if not uid: continue
            try:
                # 【整理】個別取得の開始ログを削除し、失敗時のみログを出す
                res = user_table.get_item(Key={"user#user_id": uid})
                data = res.get("Item", {})
                mu = float(data.get("skill_score", 25.0))
                sig = float(data.get("skill_sigma", 8.333))
                ratings.append((uid, Rating(mu=mu, sigma=sig)))
            except:
                ratings.append((uid, Rating(mu=25.0, sigma=8.333)))
        return ratings

    ratings_a = get_team_ratings(result_item.get("team_a", []))
    ratings_b = get_team_ratings(result_item.get("team_b", []))

    if not ratings_a or not ratings_b: return {}

    try:
        if winner == "A":
            new_r = rate([[r for _, r in ratings_a], [r for _, r in ratings_b]])
            new_a_list, new_b_list = new_r[0], new_r[1]
        else:
            new_r = rate([[r for _, r in ratings_b], [r for _, r in ratings_a]])
            new_a_list, new_b_list = new_r[1], new_r[0]

        team_a_mu = sum(r.mu for _, r in ratings_a) / len(ratings_a)
        team_b_mu = sum(r.mu for _, r in ratings_b) / len(ratings_b)
        skill_diff = team_a_mu - team_b_mu
        
        expected = "A" if skill_diff > 0 else "B"
        consistency = abs(skill_diff) if expected == winner else -abs(skill_diff)
        
        score_adj = 0.8 + (1.5 - 0.8) * min(abs(score_diff) / 20.0, 1.0)
        const_adj = 1.0 - (min(max(consistency / 15.0, -1.0), 1.0) * 0.3)
        final_adj = score_adj * const_adj

        # --- 更新サマリーを1行に凝縮 ---
        # どのコートか特定するために match_id も含める
        m_id = result_item.get("match_id", "???")
        current_app.logger.info(
            f"SkillCalc: match={m_id} | 勝者:{winner} ({t1}-{t2}) | "
            f"チームスキル差:{skill_diff:.2f} | 最終調整係数:{final_adj:.2f}"
        )

        for i, (uid, old_r) in enumerate(ratings_a):
            delta = new_a_list[i].mu - old_r.mu
            updated_skills[uid] = {
                "skill_score": old_r.mu + (delta * final_adj),
                "skill_sigma": new_a_list[i].sigma
            }
        for i, (uid, old_r) in enumerate(ratings_b):
            delta = new_b_list[i].mu - old_r.mu
            updated_skills[uid] = {
                "skill_score": old_r.mu + (delta * final_adj),
                "skill_sigma": new_b_list[i].sigma
            }

    except Exception as e:
        current_app.logger.error(f"TrueSkillエラー: {str(e)}")
        
    return updated_skills


def sync_match_entries_with_updated_skills(entry_mapping, updated_skills):
    """
    更新されたスキルスコアでmatch_entriesテーブルを同期する
    """
    from decimal import Decimal
    
    match_table = current_app.dynamodb.Table("bad-game-match_entries")
    sync_count = 0
    total_count = len(updated_skills)
    
    try:
        # 同期開始のサマリー
        current_app.logger.info(f"エントリー同期開始: 対象 {total_count} 件")
        
        for user_id, data in updated_skills.items():
            entry_id = data.get("entry_id") or entry_mapping.get(user_id)
            
            if not entry_id:
                # 警告ログは重要なので残すが、簡潔に
                current_app.logger.warning(f"エントリーID未発見: user_id={user_id}")
                continue
                
            try:
                match_table.update_item(
                    Key={"entry_id": entry_id},
                    UpdateExpression="SET skill_score = :mu, skill_sigma = :sigma",
                    ExpressionAttributeValues={
                        ":mu": Decimal(str(data["skill_score"])),
                        ":sigma": Decimal(str(data["skill_sigma"]))
                    }
                )
                sync_count += 1
                
                # 【削除】1件ずとの詳細な DEBUG ログは削除しました
                
            except Exception as e:
                # 失敗時は原因を特定したいので詳細を出す
                current_app.logger.error(f"更新失敗 entry_id={entry_id}: {str(e)}")
        
        # 完了報告を 1 行で出力
        current_app.logger.info(f"同期完了: {sync_count}/{total_count} 件のスキルを反映しました")
    
    except Exception as e:
        current_app.logger.error(f"同期プロセス異常終了: {str(e)}")
    
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

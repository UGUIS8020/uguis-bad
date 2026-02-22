from flask import current_app
from trueskill import TrueSkill
import random
from typing import List, Tuple, Dict, Any
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
import logging
from botocore.exceptions import ClientError
from typing import Optional
from decimal import Decimal
from trueskill import Rating, rate

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

def normalize_user_pk(uid: str) -> str:
    if uid is None:
        raise ValueError("uid is None")
    s = str(uid).strip()
    if not s:
        raise ValueError("uid is empty")
    return s if s.startswith("user#") else f"user#{s}"

def update_trueskill_for_players_and_return_updates(result_item):
    """
    result_item から team_a/team_b のプレイヤーを取り出して TrueSkill 更新し、
    updated_skills = {user_id: {skill_score: float, skill_sigma: float}} を返す。
    同時に bad-users にも永続化する（test_ は除外）。
    """
    user_table = current_app.dynamodb.Table("bad-users")

    def safe_get_score(item, keys):
        for k in keys:
            val = item.get(k)
            if val is not None:
                try:
                    return int(float(val))
                except Exception:
                    continue
        return 0

    t1 = safe_get_score(result_item, ["team1_score", "team_a_score", "score1"])
    t2 = safe_get_score(result_item, ["team2_score", "team_b_score", "score2"])
    winner = str(result_item.get("winner", "A")).upper()
    # score_diff は今は未使用なら消してOK（残したいならそのまま）
    score_diff = (t1 - t2) if winner == "A" else (t2 - t1)

    def get_team_ratings(team):
        """
        team: [{user_id, skill_score?, skill_sigma?, ...}, ...]
        戻り: [(player_uid, Rating), ...]
        """
        out = []
        for player in team or []:
            player_uid = player.get("user_id")
            if not player_uid:
                continue

            mu = player.get("skill_score")
            sig = player.get("skill_sigma")

            # 足りなければ bad-users から拾う
            if mu is None or sig is None:
                try:
                    res = user_table.get_item(
                        Key={"user#user_id": normalize_user_pk(player_uid)}
                    )
                    data = res.get("Item") or {}
                    mu = data.get("skill_score", 25.0)
                    sig = data.get("skill_sigma", 8.333)
                except Exception:
                    mu, sig = 25.0, 8.333

            try:
                mu = float(mu)
            except Exception:
                mu = 25.0
            try:
                sig = float(sig)
            except Exception:
                sig = 8.333

            out.append((str(player_uid), Rating(mu=mu, sigma=sig)))

        return out

    ratings_a = get_team_ratings(result_item.get("team_a", []))
    ratings_b = get_team_ratings(result_item.get("team_b", []))

    if not ratings_a or not ratings_b:
        return {}

    team_a_uids = [uid for uid, _r in ratings_a]
    team_b_uids = [uid for uid, _r in ratings_b]
    team_a_ratings = [_r for _uid, _r in ratings_a]
    team_b_ratings = [_r for _uid, _r in ratings_b]

    ranks = [0, 1] if winner == "A" else [1, 0]
    new_team_a, new_team_b = rate([team_a_ratings, team_b_ratings], ranks=ranks)

    updated_skills = {}
    for uid, new_r in zip(team_a_uids, new_team_a):
        updated_skills[uid] = {"skill_score": float(new_r.mu), "skill_sigma": float(new_r.sigma)}
    for uid, new_r in zip(team_b_uids, new_team_b):
        updated_skills[uid] = {"skill_score": float(new_r.mu), "skill_sigma": float(new_r.sigma)}

    current_app.logger.info(
        "[ts-after-rate] match=%s court=%s | updated=%d sample=%s",
        result_item.get("match_id"),
        result_item.get("court_number"),
        len(updated_skills),
        next(iter(updated_skills.items()), None),
    )    

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


def parse_players(val):
    """
    team_a/team_b を list[dict] に正規化して返す
    - val が list ならそのまま
    - val が JSON文字列なら loads
    - それ以外は []
    """
    if val is None:
        return []

    # すでに list で来ている（DynamoDBからの復元でよくある）
    if isinstance(val, list):
        out = []
        for x in val:
            if isinstance(x, dict):
                # user_id を str に寄せる（後段のキー一致のため）
                if "user_id" in x:
                    x["user_id"] = str(x["user_id"])
                out.append(x)
        return out

    # 文字列（JSON）
    if isinstance(val, str):
        try:
            obj = json.loads(val)
            return parse_players(obj)  # 再帰でlist処理へ
        except Exception:
            return []

    # その他（dict単体など）も一応吸う
    if isinstance(val, dict):
        if "user_id" in val:
            val["user_id"] = str(val["user_id"])
        return [val]

    return []

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


def generate_matches_by_pair_skill_balance(
    pairs: List[Tuple[Player, Player]],
    max_courts: int
):
    scored = [(pair, pair_strength(pair[0], pair[1])) for pair in pairs]
    scored.sort(key=lambda x: x[1])

    max_matches = min(len(scored) // 2, max_courts)
    matches: List[Tuple[Tuple[Player, Player], Tuple[Player, Player]]] = []
    used = [False] * len(scored)

    for _ in range(max_matches):
        # まだ使ってない最小のペアを探す
        i = next((k for k, u in enumerate(used) if not u), None)
        if i is None:
            break
        used[i] = True

        # i と最も差が小さい未使用ペアを探す
        best_j = None
        best_diff = float("inf")
        for j in range(i + 1, len(scored)):
            if used[j]:
                continue
            diff = abs(scored[i][1] - scored[j][1])
            if diff < best_diff:
                best_diff = diff
                best_j = j
                if best_diff == 0:
                    break

        if best_j is None:
            # 相手が見つからないなら戻す
            used[i] = False
            break

        used[best_j] = True
        matches.append((scored[i][0], scored[best_j][0]))

    unused_pairs = [scored[k][0] for k, u in enumerate(used) if not u]
    return matches, unused_pairs


def pair_strength(p1: Player, p2: Player) -> float:
    c1 = getattr(p1, "conservative", None)
    c2 = getattr(p2, "conservative", None)
    if c1 is not None and c2 is not None:
        return float(c1) + float(c2)

    s1 = getattr(p1, "skill_score", None)
    s2 = getattr(p2, "skill_score", None)
    if s1 is not None and s2 is not None:
        return float(s1) + float(s2)

    return float(getattr(p1, "level", 0)) + float(getattr(p2, "level", 0))


def generate_ai_best_pairings(active_players, max_courts, iterations=1000):
    """
    シミュレーションを行い、全コートのスキルバランスが最も均等な組み合わせを返す。
    """
    # 試合に必要な人数（4の倍数）
    num_active = len(active_players)
    required_players_count = (num_active // 4) * 4
    num_courts = min(max_courts, required_players_count // 4)

    if num_courts == 0:
        return [], active_players

    best_matches = []
    best_waiting = []
    min_total_penalty = float('inf')

    for i in range(iterations):
        # 1) シャッフルして仮の組分けを作る
        temp_players = active_players[:]
        random.shuffle(temp_players)
        
        current_active = temp_players[:num_courts * 4]
        current_waiting = temp_players[num_courts * 4:]
        
        current_matches = []
        total_penalty = 0
        
        # 2) 4人ずつコートに割り振り、スキル差を計算
        for c in range(num_courts):
            # 4人抽出
            p1, p2, p3, p4 = current_active[c*4 : (c+1)*4]
            
            # チーム分けの全3パターンを試して、そのコート内でのベストを探す
            # (p1,p2 vs p3,p4), (p1,p3 vs p2,p4), (p1,p4 vs p2,p3)
            possible_teams = [
                ((p1, p2), (p3, p4)),
                ((p1, p3), (p2, p4)),
                ((p1, p4), (p2, p3))
            ]
            
            best_court_diff = float('inf')
            best_court_pair = None
            
            for t1, t2 in possible_teams:
                # 平均スキルの差（conservativeスキルを使用）
                avg1 = (t1[0].conservative + t1[1].conservative) / 2
                avg2 = (t2[0].conservative + t2[1].conservative) / 2
                diff = abs(avg1 - avg2)
                
                if diff < best_court_diff:
                    best_court_diff = diff
                    best_court_pair = (t1, t2)
            
            current_matches.append(best_court_pair)
            # 二乗ペナルティ：大きな実力差があるコートをより厳しく評価
            total_penalty += (best_court_diff ** 2)

        # 3) 全体評価が過去最高なら更新
        if total_penalty < min_total_penalty:
            min_total_penalty = total_penalty
            best_matches = current_matches
            best_waiting = current_waiting

    return best_matches, best_waiting


# =========================================================
# Rest queue (queue 방식 + late joiners at tail)
# =========================================================
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timezone
import random

from botocore.exceptions import ClientError
from flask import current_app


def _parse_iso_dt(s: Optional[str]) -> Optional[datetime]:
    """ISO8601文字列をUTC datetimeへ。失敗したらNone。"""
    if not s or not isinstance(s, str):
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _rest_queue_pk(queue_key: str) -> str:
    return f"meta#{queue_key}"


def _load_rest_queue(meta_table, *, queue_key: str = "rest_queue") -> Dict[str, Any]:
    """
    Returns dict:
      queue: List[str]
      generation: int
      version: int
      cycle_started_at: Optional[str]
    """
    pk = _rest_queue_pk(queue_key)
    try:
        resp = meta_table.get_item(Key={"match_id": pk}, ConsistentRead=True)
        item = resp.get("Item") or {}

        q = item.get("queue", [])
        if not isinstance(q, list):
            q = []

        return {
            "queue": q,
            "generation": int(item.get("generation", 1) or 1),
            "version": int(item.get("version", 0) or 0),
            "cycle_started_at": item.get("cycle_started_at"),
        }
    except ClientError as e:
        current_app.logger.error("[rest_queue][LOAD_ERR] %s", e)
        return {"queue": [], "generation": 1, "version": 0, "cycle_started_at": None}


def _save_rest_queue_optimistic(
    meta_table,
    *,
    queue_key: str,
    queue: List[str],
    generation: int,
    prev_version: int,
    cycle_started_at: Optional[str] = None,
) -> bool:
    """
    楽観ロックで rest_queue を保存する
    - match_id = meta#<queue_key>
    - version が prev_version と一致する場合のみ更新（初回は version 未存在でも可）
    - cycle_started_at は指定された場合のみ更新
    """
    pk = _rest_queue_pk(queue_key)
    new_version = int(prev_version) + 1

    expr_names = {"#q": "queue", "#g": "generation", "#v": "version"}
    expr_vals = {":q": list(queue), ":g": int(generation), ":nv": int(new_version), ":pv": int(prev_version)}

    if cycle_started_at is not None:
        expr_names["#cs"] = "cycle_started_at"
        expr_vals[":cs"] = cycle_started_at
        update_expr = "SET #q=:q, #g=:g, #v=:nv, #cs=:cs"
    else:
        update_expr = "SET #q=:q, #g=:g, #v=:nv"

    try:
        meta_table.update_item(
            Key={"match_id": pk},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_vals,
            ConditionExpression="attribute_not_exists(#v) OR #v = :pv",
        )
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code == "ConditionalCheckFailedException":
            return False
        current_app.logger.error("[rest_queue][SAVE_ERR] %s", e)
        return False


def _weighted_sample_from_queue(
    *,
    queue: List[str],
    waiting_count: int,
    by_id: Dict[str, Dict[str, Any]],
    skill_key: str = "skill_score",
    bottom_n: int = 2,
    boost: float = 1.2,
) -> Tuple[List[str], List[str]]:
    """
    queue から waiting_count 人を重み付きで非復元抽出。
    戻り値: (picked_uids, remaining_queue)
    ※ remaining_queue は picked を除いた queue（元の順序保持）
    """
    if waiting_count <= 0 or not queue:
        return [], list(queue)

    cand = list(queue)

    def get_skill(uid: str) -> float:
        v = by_id.get(uid, {}).get(skill_key, 50)
        try:
            return float(v)
        except Exception:
            return 50.0

    skills = {uid: get_skill(uid) for uid in cand}
    low_uids = set(sorted(cand, key=lambda u: skills[u])[: max(0, int(bottom_n))])
    weights = {uid: (boost if uid in low_uids else 1.0) for uid in cand}

    picked: List[str] = []
    remaining = list(cand)

    for _ in range(min(waiting_count, len(remaining))):
        total = sum(weights[uid] for uid in remaining)
        r = random.random() * total
        acc = 0.0
        chosen = remaining[-1]
        for uid in remaining:
            acc += weights[uid]
            if acc >= r:
                chosen = uid
                break
        picked.append(chosen)
        remaining = [u for u in remaining if u != chosen]

    picked_set = set(picked)
    remaining_queue = [uid for uid in queue if uid not in picked_set]
    return picked, remaining_queue


def _safe_float(v: Any, default: float) -> float:
    if v is None:
        return default
    try:
        if isinstance(v, Decimal):
            return float(v)
        return float(v)
    except Exception:
        return default


def _weighted_sample_from_queue(
    queue: List[str],
    waiting_count: int,
    by_id: Dict[str, Any],
    *,
    skill_key: str = "skill_score",
    bottom_n: int = 2,
    boost: float = 1.2,
) -> Tuple[List[str], List[str]]:
    """
    キュー内でスキル下位 bottom_n 名の選出確率を boost 倍にする
    """
    if not queue or waiting_count <= 0:
        return [], queue

    remaining = list(queue)
    take = min(waiting_count, len(remaining))
    selected: List[str] = []

    # キュー内の中央値をデフォルトに
    nums: List[float] = []
    for uid in remaining:
        v = by_id.get(uid, {}).get(skill_key)
        if v is None:
            continue
        try:
            nums.append(float(v) if not isinstance(v, Decimal) else float(v))
        except Exception:
            pass
    if nums:
        nums.sort()
        default_score = float(nums[len(nums) // 2])
    else:
        default_score = 50.0

    for i in range(take):
        # 毎回 bottom_n を更新
        scores = {uid: _safe_float(by_id.get(uid, {}).get(skill_key), default_score) for uid in remaining}
        bottom_ids = set(sorted(remaining, key=lambda uid: scores[uid])[:max(1, min(bottom_n, len(remaining)))])

        # ★【追加ログ】ブースト対象者の名前を表示
        bottom_names = [by_id[uid].get("display_name", "不明") for uid in bottom_ids if uid in by_id]
        current_app.logger.info("[rest_queue] 抽選%d回目 - ブースト対象(x%.2f): %s", i+1, boost, bottom_names)

        weights = [boost if uid in bottom_ids else 1.0 for uid in remaining]
        total = sum(weights)

        r = random.uniform(0, total)
        cumulative = 0.0
        for j, w in enumerate(weights):
            cumulative += w
            if r <= cumulative:
                picked_uid = remaining[j]
                picked_name = by_id.get(picked_uid, {}).get("display_name", "不明")
                is_boosted = " (ブースト適用済)" if picked_uid in bottom_ids else ""
                
                # ★【追加ログ】実際に誰が選ばれたかを表示
                current_app.logger.info("[rest_queue] 選出結果: %s%s", picked_name, is_boosted)
                
                selected.append(picked_uid)
                remaining.pop(j)
                break

    return selected, remaining
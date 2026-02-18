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

    def normalize_user_pk(uid: str) -> str:
        uid = str(uid)
        return uid if uid.startswith("user#") else f"user#{uid}"

    def get_team_ratings(team, user_table):
        ratings = []
        for player in team:
            uid = player.get("user_id")
            if not uid:
                continue

            mu = player.get("skill_score")
            sig = player.get("skill_sigma")

            if mu is None or sig is None:
                try:
                    res = user_table.get_item(Key={"user#user_id": normalize_user_pk(uid)})
                    data = res.get("Item", {}) or {}
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

            ratings.append((str(uid), Rating(mu=mu, sigma=sig)))

        return ratings

    # --- ここで user_table を用意 ---
    user_table = current_app.dynamodb.Table("bad-users")

    ratings_a = get_team_ratings(result_item.get("team_a", []), user_table)
    ratings_b = get_team_ratings(result_item.get("team_b", []), user_table)

    if not ratings_a or not ratings_b:
        return {}

    team_a_uids = [uid for uid, r in ratings_a]
    team_b_uids = [uid for uid, r in ratings_b]
    team_a_ratings = [r for uid, r in ratings_a]
    team_b_ratings = [r for uid, r in ratings_b]

    # 勝敗 → ranks（小さいほど勝ち）
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

    for uid, vals in updated_skills.items():
        if str(uid).startswith("test_"):
            continue
        try:
            user_table.update_item(
                Key={"user#user_id": normalize_user_pk(uid)},
                UpdateExpression="SET skill_score = :s, skill_sigma = :g",
                ExpressionAttributeValues={
                    ":s": Decimal(str(round(vals["skill_score"], 2))),
                    ":g": Decimal(str(round(vals["skill_sigma"], 4))),
                }
            )
            current_app.logger.info(f"スキル永続化: {uid} → {vals['skill_score']:.2f}")
        except Exception as e:
            current_app.logger.error(f"スキル永続化エラー [{uid}]: {e}")

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
                avg1 = (t1[0].skill_score + t1[1].skill_score) / 2
                avg2 = (t2[0].skill_score + t2[1].skill_score) / 2
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


# 待機者選出ロジック（キュー方式）
def _select_waiting_entries(sorted_entries: list, waiting_count: int) -> tuple[list, list]:
    """
    sorted_entries: 休み選出前の候補（既に優先度順などでソート済みを想定）
    waiting_count: 休みにする人数

    Returns:
        (active_entries, waiting_entries)
    """
    if waiting_count <= 0:
        return sorted_entries, []

    n = len(sorted_entries)
    if n == 0:
        return [], []
    # 全員休み事故を防止（必要なら調整）
    if waiting_count >= n:
        waiting_count = max(0, n - 1)

    active_entries, waiting_entries, meta = _pick_waiters_by_rest_queue(
        entries=sorted_entries,
        waiting_count=waiting_count,
    )

    current_app.logger.info(
        "[rest_queue] gen=%s ver=%s waiting=%s queue_remaining=%s",
        meta.get("generation"),
        meta.get("version"),
        ", ".join([e.get("display_name", "?") for e in waiting_entries]),
        meta.get("queue_remaining"),
    )
    return active_entries, waiting_entries


def _pick_waiters_by_rest_queue(
    entries: List[Dict[str, Any]],
    waiting_count: int,
    *,
    queue_key: str = "rest_queue",
    max_retries: int = 5,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """
    休みロジック本体（キュー方式）
    - 1巡するまで同じ人が2回休みにならない（キュー消費）
    - 途中参加者は末尾
    - 離脱者は除去
    - キュー不足時は「残りを使い切ってから」次巡で補う（途中参加末尾の思想を壊しにくい）
    - DynamoDB に version を持たせて簡易の競合対策（楽観ロック）

    Returns:
      active_entries, waiting_entries, meta
    """
    meta_table = current_app.dynamodb.Table("bad-game-matches")

    # entries から必要情報
    current_user_ids = [e["user_id"] for e in entries]
    current_user_set = set(current_user_ids)
    by_id = {e["user_id"]: e for e in entries}

    for attempt in range(1, max_retries + 1):
        queue_item = _load_rest_queue(meta_table, queue_key=queue_key)

        queue: List[str] = list(queue_item.get("queue", []))
        generation: int = int(queue_item.get("generation", 1))
        version: int = int(queue_item.get("version", 0))

        # --- 1) 不整合修正: 離脱者除去 ---
        queue = [uid for uid in queue if uid in current_user_set]

        # --- 2) 途中参加者: 末尾追加（キューにいない人） ---
        queued_set = set(queue)
        newcomers = [uid for uid in current_user_ids if uid not in queued_set]
        if newcomers:
            # 末尾に追加（順序はランダムでも良い／固定でも良い）
            random.shuffle(newcomers)
            queue.extend(newcomers)
            current_app.logger.info("[rest_queue] newcomers added: %s", newcomers)

        # --- 3) waiting_pick を作る（不足分は次巡から補う） ---
        waiting_pick: List[str] = []

        take1 = min(waiting_count, len(queue))
        if take1 > 0:
            waiting_pick.extend(queue[:take1])
            queue = queue[take1:]

        need = waiting_count - len(waiting_pick)
        if need > 0:
            # 次巡を生成して不足分だけ取る（残りは次巡のキューとして保存）
            new_queue = list(current_user_ids)
            random.shuffle(new_queue)
            generation += 1

            waiting_pick.extend(new_queue[:need])
            queue = new_queue[need:]

        waiting_ids = set(waiting_pick)

        # --- 4) 返却: waiting はキュー順（waiting_pick順）で返す ---
        waiting_entries = [by_id[uid] for uid in waiting_pick if uid in by_id]
        active_entries = [e for e in entries if e["user_id"] not in waiting_ids]

        # --- 5) 保存（version で楽観ロック） ---
        save_ok = _save_rest_queue_optimistic(
            meta_table,
            queue_key=queue_key,
            queue=queue,
            generation=generation,
            prev_version=version,
        )
        if save_ok:
            meta = {
                "generation": generation,
                "version": version + 1,
                "queue_remaining": len(queue),
                "attempt": attempt,
            }
            return active_entries, waiting_entries, meta

        # 競合したのでリトライ
        current_app.logger.warning("[rest_queue] conflict retry %d/%d", attempt, max_retries)

    # リトライ尽きたら最後は安全側：今回だけはキュー無しランダムで返す（落とさないため）
    current_app.logger.error("[rest_queue] failed to save after retries; fallback random")
    uids = list(current_user_ids)
    random.shuffle(uids)
    waiting_pick = uids[:waiting_count]
    waiting_ids = set(waiting_pick)
    waiting_entries = [by_id[uid] for uid in waiting_pick if uid in by_id]
    active_entries = [e for e in entries if e["user_id"] not in waiting_ids]
    meta = {"generation": None, "version": None, "queue_remaining": None, "attempt": max_retries, "fallback": True}
    return active_entries, waiting_entries, meta


def _load_rest_queue(meta_table, *, queue_key: str) -> Dict[str, Any]:
    resp = meta_table.get_item(Key={"match_id": queue_key}, ConsistentRead=True)
    item = resp.get("Item") or {}
    # 初回のデフォルト
    if "queue" not in item:
        item["queue"] = []
    if "generation" not in item:
        item["generation"] = 1
    if "version" not in item:
        item["version"] = 0
    return item


def _save_rest_queue_optimistic(
    meta_table,
    *,
    queue_key: str,
    queue: List[str],
    generation: int,
    prev_version: int,
) -> bool:
    """
    version を使った簡易な競合対策（楽観ロック）
    - 既存アイテムがある: version が prev_version のときだけ更新
    - 無い場合: attribute_not_exists(match_id) で作成（version=1）
    """
    now = datetime.now(JST).isoformat()

    # 既存あり更新（Condition: version一致）
    try:
        meta_table.update_item(
            Key={"match_id": queue_key},
            UpdateExpression="SET #q=:q, generation=:g, updated_at=:u, version=:nv",
            ConditionExpression="attribute_exists(match_id) AND version = :pv",
            ExpressionAttributeNames={"#q": "queue"},
            ExpressionAttributeValues={
                ":q": queue,
                ":g": int(generation),
                ":u": now,
                ":pv": int(prev_version),
                ":nv": int(prev_version + 1),
            },
        )
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code == "ConditionalCheckFailedException":
            # 既存が無い or version競合の可能性
            pass
        else:
            # 想定外は上に投げても良いが、ここでは失敗扱い
            current_app.logger.exception("[rest_queue] update_item failed: %s", e)
            return False

    # 既存が無いケース（作成を試みる）
    try:
        meta_table.put_item(
            Item={
                "match_id": queue_key,
                "queue": queue,
                "generation": int(generation),
                "updated_at": now,
                "version": 1,
            },
            ConditionExpression="attribute_not_exists(match_id)",
        )
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code == "ConditionalCheckFailedException":
            return False  # 他が先に作った
        current_app.logger.exception("[rest_queue] put_item failed: %s", e)
        return False
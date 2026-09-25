"""
テストコート（game2）: 既存のマッチングシステム（game/views.py）とは
完全に独立した並行実装。

- 参加者データは専用テーブル（bad-game2-match_entries, bad-game2-results）に
  保存し、既存の bad-game-match_entries / bad-game-results には一切触れない。
- 進行状態は bad-game-matches テーブルに meta#continuous_current /
  meta#continuous_pairing / meta#continuous_rest_queue という別キーで保存する
  （既存の meta#current / meta#pairing / meta#rest_queue とはキーが違うので
  衝突しない）。
- スキル値（skill_score/skill_sigma）だけは bad-users を共有する（同じ人の
  実力なので、どちらのコートで試合しても更新されるのが自然なため）。
- 何か問題が起きた場合、管理者は「緊急: 全員を通常コートへ移動」ボタンで
  テストコートの参加者全員を既存システムの待機列（pending）に移せる。

Phase 2: 最初の組み合わせだけ管理者が「組み合わせ作成」ボタンで作る。
その後は、あるコートのスコアが送信されるたびに、そのコートの4人だけを
TrueSkill更新してpending化し、休憩ローテーションで「次に出るべき」上位候補の
中から実力バランスが良い4人を選んで、そのコートだけ自動的に次の試合を作る。
他のコートは無関係に進行中のまま影響を受けない。
"""
from flask import Blueprint, render_template, redirect, url_for, flash, request, current_app, jsonify
from flask_login import login_required, current_user
from datetime import datetime
from decimal import Decimal
import uuid
import random
import logging

from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from game.game_utils import (
    Player,
    generate_ai_best_pairings,
    generate_balanced_pairs_and_matches,
    generate_full_random_pairings,
    update_trueskill_for_players_and_return_updates,
)
from game.views import (
    persist_skill_to_bad_users,
    _pick_waiters_by_rest_queue,
)
from utils.timezone import JST

logger = logging.getLogger(__name__)

bp_game2 = Blueprint('game2', __name__)

REST_QUEUE_KEY = "continuous_rest_queue"
META_CURRENT_PK = "meta#continuous_current"
META_PAIRING_PK = "meta#continuous_pairing"


def _entry_table():
    return current_app.dynamodb.Table("bad-game2-match_entries")


def _results_table():
    return current_app.dynamodb.Table("bad-game2-results")


def _meta_table():
    return current_app.dynamodb.Table("bad-game-matches")


def generate_match_id2():
    """
    試合ID生成（既存システムのIDと絶対に衝突しないよう g2_ を付ける）。
    連続マッチングでは短時間に複数コートが立て続けに補充されるため、
    秒単位の時刻だけだと同一match_idが発行されうる（result_idの重複判定が
    誤動作し、そのコートが止まる原因になる）。末尾に短いランダム値を足して
    タイミングに関わらず一意にする。
    """
    now = datetime.now()
    match_id = "g2_" + now.strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
    current_app.logger.info(f"[game2] 生成された試合ID: {match_id}")
    return match_id


def has_ongoing_matches2():
    """テストコート側で進行中の試合があるか（既存システムとは独立に判定）"""
    try:
        entry_table = _entry_table()
        resp = entry_table.scan(FilterExpression=Attr("entry_status").eq("playing"), ConsistentRead=True)
        return len(resp.get("Items", [])) > 0
    except Exception as e:
        current_app.logger.error(f"[game2] 進行中試合チェックエラー: {str(e)}")
        return False


def sync_match_entries_with_updated_skills2(entry_mapping, updated_skills):
    """更新されたスキルスコアで bad-game2-match_entries を同期する"""
    entry_table = _entry_table()
    sync_count = 0
    for user_id, data in updated_skills.items():
        entry_id = data.get("entry_id") or entry_mapping.get(user_id)
        if not entry_id:
            current_app.logger.warning(f"[game2] エントリーID未発見: user_id={user_id}")
            continue
        try:
            entry_table.update_item(
                Key={"entry_id": entry_id},
                UpdateExpression="SET skill_score = :mu, skill_sigma = :sigma",
                ExpressionAttributeValues={
                    ":mu": Decimal(str(data["skill_score"])),
                    ":sigma": Decimal(str(data["skill_sigma"])),
                },
            )
            sync_count += 1
        except Exception as e:
            current_app.logger.error(f"[game2] 更新失敗 entry_id={entry_id}: {str(e)}")
    current_app.logger.info(f"[game2] 同期完了: {sync_count}/{len(updated_skills)} 件")
    return sync_count


@bp_game2.route("/court")
@login_required
def court():
    entry_table = _entry_table()
    meta_table = _meta_table()

    meta_current = meta_table.get_item(Key={"match_id": META_CURRENT_PK}, ConsistentRead=True).get("Item", {}) or {}
    status = meta_current.get("status", "idle")

    all_entries = entry_table.scan(ConsistentRead=True).get("Items", [])
    my_entry = next((e for e in all_entries if e.get("user_id") == current_user.get_id()), None)

    # ★Phase2: 全コート共通の1つのmatch_idではなく、コートごとに独立した
    #   match_idを持つので、entry_status=="playing"を持つ人をコート番号で
    #   グルーピングし、そのコートの現在のmatch_idも一緒に持たせる。
    courts = {}
    for e in all_entries:
        if e.get("entry_status") == "playing" and e.get("court_number") is not None:
            c = int(e.get("court_number"))
            courts.setdefault(c, {"A": [], "B": [], "match_id": e.get("match_id")})
            team = e.get("team", "A")
            courts[c][team].append(e)

    pending = [e for e in all_entries if e.get("entry_status") == "pending"]
    resting = [e for e in all_entries if e.get("entry_status") == "resting"]

    is_admin = getattr(current_user, "administrator", False)

    # ★管理者以外は自分が今出ているコートだけを見せる（他コートの様子は非表示）
    if not is_admin:
        if my_entry and my_entry.get("entry_status") == "playing" and my_entry.get("court_number") is not None:
            my_court_num = int(my_entry.get("court_number"))
            courts = {my_court_num: courts[my_court_num]} if my_court_num in courts else {}
        else:
            courts = {}

    # ★コート番号順(1,2,3...)で常に同じ並びになるようにする
    courts = dict(sorted(courts.items()))

    return render_template(
        "game2/court.html",
        status=status,
        courts=courts,
        pending=pending,
        resting=resting,
        my_entry=my_entry,
        is_admin=is_admin,
    )


@bp_game2.route("/entry", methods=["POST"])
@login_required
def entry():
    user_id = current_user.get_id()
    now = datetime.now(JST).isoformat()
    entry_table = _entry_table()
    user_table = current_app.dynamodb.Table("bad-users")

    existing = entry_table.scan(
        FilterExpression=Attr("user_id").eq(user_id) & Attr("entry_status").is_in(["pending", "resting", "playing"])
    ).get("Items", [])
    if existing:
        return redirect(url_for("game2.court"))

    cleanup = entry_table.scan(FilterExpression=Attr("user_id").eq(user_id)).get("Items", [])
    for item in cleanup:
        entry_table.delete_item(Key={"entry_id": item["entry_id"]})

    user_resp = user_table.get_item(Key={"user#user_id": user_id})
    user_data = user_resp.get("Item")
    skill_score = user_data.get("skill_score", 50) if user_data else 50
    skill_sigma = user_data.get("skill_sigma", 8.333) if user_data else 8.333
    display_name = user_data.get("display_name", "未設定") if user_data else "未設定"
    gender = user_data.get("gender", "M") if user_data else "M"

    entry_table.put_item(Item={
        "entry_id": str(uuid.uuid4()),
        "user_id": user_id,
        "match_id": "pending",
        "entry_status": "pending",
        "display_name": display_name,
        "gender": gender,
        "skill_score": Decimal(str(skill_score)),
        "skill_sigma": Decimal(str(skill_sigma)),
        "joined_at": now,
        "created_at": now,
        "rest_count": 0,
        "match_count": 0,
    })
    current_app.logger.info(f"[game2][ENTRY] 参加登録: {user_id} 名前: {display_name}")
    return redirect(url_for("game2.court"))


@bp_game2.route("/create_pairings", methods=["POST"])
@login_required
def create_pairings():
    if not current_user.administrator:
        flash("管理者のみ実行できます。", "danger")
        return redirect(url_for("game2.court"))

    if has_ongoing_matches2():
        flash("テストコートで進行中の試合があるため、新しいペアリングを実行できません。", "warning")
        return redirect(url_for("game2.court"))

    max_courts = min(max(int(request.form.get("max_courts", 2)), 1), 6)
    entry_table = _entry_table()
    meta_table = _meta_table()

    pairing_meta = meta_table.get_item(Key={"match_id": META_PAIRING_PK}, ConsistentRead=True).get("Item", {}) or {}
    cycle_index = int(pairing_meta.get("cycle_index", 0))
    if cycle_index == 1:
        mode = "full_random"
        next_cycle_index = 2
    elif cycle_index == 2:
        mode = "ai"
        next_cycle_index = 0
    else:
        mode = "random"
        next_cycle_index = cycle_index + 1

    response = entry_table.scan(FilterExpression=Attr("entry_status").eq("pending"), ConsistentRead=True)
    entries_by_user = {}
    for e in response.get("Items", []):
        uid, joined_at = e["user_id"], e.get("joined_at", "")
        if uid not in entries_by_user or joined_at > entries_by_user[uid].get("joined_at", ""):
            entries_by_user[uid] = e
    entries = list(entries_by_user.values())

    if len(entries) < 4:
        flash("テストコート: 4人以上のエントリーが必要です。", "warning")
        return redirect(url_for("game2.court"))

    sorted_entries = sorted(entries, key=lambda e: (e.get("match_count", 0), random.random()))
    cap_by_courts = min(max_courts * 4, len(sorted_entries))
    required_players = cap_by_courts - (cap_by_courts % 4)
    waiting_count = len(sorted_entries) - required_players

    if waiting_count > 0:
        active_entries, waiting_entries, _meta = _pick_waiters_by_rest_queue(
            entries=sorted_entries, waiting_count=waiting_count, queue_key=REST_QUEUE_KEY,
        )
    else:
        active_entries, waiting_entries = sorted_entries, []

    players = []
    for e in active_entries:
        skill_score = float(e.get("skill_score", 50.0))
        skill_sigma = float(e.get("skill_sigma", 8.333))
        conservative_val = skill_score - 3 * skill_sigma
        p = Player(e["display_name"], conservative_val, e.get("gender", "M"))
        p.user_id = e.get("user_id")
        p.entry_id = e.get("entry_id")
        p.conservative = conservative_val
        p.skill_score = skill_score
        p.skill_sigma = skill_sigma
        players.append(p)

    match_id = generate_match_id2()
    effective_courts = min(max_courts, len(players) // 4)

    if mode == "ai":
        matches, additional_waiting_players = generate_ai_best_pairings(players, effective_courts, iterations=1000)
    elif mode == "full_random":
        _pairs, matches, additional_waiting_players = generate_full_random_pairings(players, effective_courts)
    else:
        _pairs, matches, additional_waiting_players = generate_balanced_pairs_and_matches(players, effective_courts)

    if not matches:
        flash("テストコート: 試合を作成できませんでした。", "warning")
        return redirect(url_for("game2.court"))

    current_app.logger.info(
        "[game2][pairing] match_id=%s mode=%s courts=%d", match_id, mode, len(matches)
    )

    now_jst = datetime.now(JST).isoformat()
    import boto3
    dynamodb_client = boto3.client("dynamodb", region_name="ap-northeast-1")

    tx_items = [{
        "Update": {
            "TableName": "bad-game-matches",
            "Key": {"match_id": {"S": META_CURRENT_PK}},
            "UpdateExpression": "SET #st = :playing, #cm = :mid, #cc = :cc, #ua = :now, #pm = :mode",
            "ConditionExpression": "attribute_not_exists(#st) OR #st <> :playing",
            "ExpressionAttributeNames": {
                "#st": "status", "#cm": "current_match_id", "#cc": "court_count",
                "#ua": "updated_at", "#pm": "pairing_mode",
            },
            "ExpressionAttributeValues": {
                ":playing": {"S": "playing"}, ":mid": {"S": str(match_id)},
                ":cc": {"N": str(len(matches))}, ":now": {"S": now_jst}, ":mode": {"S": mode},
            },
        }
    }]

    for court_num, ((a1, a2), (b1, b2)) in enumerate(matches, 1):
        for pl, team in [(a1, "A"), (a2, "A"), (b1, "B"), (b2, "B")]:
            entry_id = str(getattr(pl, "entry_id", "") or "")
            tx_items.append({
                "Update": {
                    "TableName": "bad-game2-match_entries",
                    "Key": {"entry_id": {"S": entry_id}},
                    "UpdateExpression": "SET entry_status=:playing, match_id=:mid, court_number=:c, team=:t, updated_at=:now",
                    "ConditionExpression": "entry_status = :pending",
                    "ExpressionAttributeValues": {
                        ":playing": {"S": "playing"}, ":pending": {"S": "pending"},
                        ":mid": {"S": str(match_id)}, ":c": {"N": str(court_num)},
                        ":t": {"S": team}, ":now": {"S": now_jst},
                    },
                }
            })

    try:
        dynamodb_client.transact_write_items(TransactItems=tx_items)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "TransactionCanceledException":
            flash("テストコート: 進行中の試合があるためペアリングできませんでした。", "warning")
            return redirect(url_for("game2.court"))
        raise

    meta_table.update_item(
        Key={"match_id": META_PAIRING_PK},
        UpdateExpression="SET cycle_index=:ci, last_mode=:m, last_match_id=:mid, updated_at=:now",
        ExpressionAttributeValues={
            ":ci": next_cycle_index, ":m": mode, ":mid": str(match_id), ":now": now_jst,
        },
    )

    flash(f"テストコート: {len(matches)}コート分の組み合わせを作成しました（モード: {mode}）", "success")
    return redirect(url_for("game2.court"))


def _refill_candidate_pool(entry_table, pool_size=20):
    """
    pending中の人を「休憩ローテーション上、次に出るべき順」に並べて返す。
    pool_sizeは組み合わせ探索が重くなりすぎないための保険的な上限で、
    実運用の人数(20人程度まで)なら実質pending全員が候補になる
    （以前は8人に絞っていたため、直前に空いた4人を含めても候補が
    足りず、重複回避の余地が狭くなっていた）。
    """
    pending = entry_table.scan(FilterExpression=Attr("entry_status").eq("pending"), ConsistentRead=True).get("Items", [])
    sorted_pending = sorted(pending, key=lambda e: (e.get("match_count", 0), e.get("joined_at", "")))
    return sorted_pending[:max(4, min(pool_size, len(sorted_pending)))]


RECENT_HISTORY_RESULTS = 40  # 直近何件の試合結果を「最近」とみなすか
PARTNER_REPEAT_WEIGHT = 1
OPPONENT_REPEAT_WEIGHT = 2  # 対戦相手の重複の方が体感の偏りが大きいため重めに
BALANCE_TIEBREAK_MARGIN_RATIO = 0.05  # 最良バランスの5%以内を「僅差」とみなす
BALANCE_TIEBREAK_MARGIN_FLOOR = 1.0


def _get_recent_pair_history2(results_table, max_results=RECENT_HISTORY_RESULTS):
    """
    直近の試合結果から「誰と誰がパートナーだったか」「誰と誰が対戦したか」を
    集計する。件数がまだ少ないテーブルなのでシンプルに全件スキャン→
    created_atで新しい順にmax_results件だけ使う。
    """
    from collections import Counter

    all_results = results_table.scan().get("Items", [])
    all_results.sort(key=lambda r: r.get("created_at", ""), reverse=True)
    recent = all_results[:max_results]

    partner_counter = Counter()
    opponent_counter = Counter()
    for r in recent:
        a_uids = [p.get("user_id") for p in (r.get("team_a") or []) if p.get("user_id")]
        b_uids = [p.get("user_id") for p in (r.get("team_b") or []) if p.get("user_id")]
        if len(a_uids) == 2:
            partner_counter[frozenset(a_uids)] += 1
        if len(b_uids) == 2:
            partner_counter[frozenset(b_uids)] += 1
        for x in a_uids:
            for y in b_uids:
                opponent_counter[frozenset([x, y])] += 1
    return partner_counter, opponent_counter


def _repeat_penalty2(team_a, team_b, partner_counter, opponent_counter):
    a_uids = [e.get("user_id") for e in team_a]
    b_uids = [e.get("user_id") for e in team_b]
    penalty = 0
    if a_uids[0] and a_uids[1]:
        penalty += partner_counter.get(frozenset(a_uids), 0) * PARTNER_REPEAT_WEIGHT
    if b_uids[0] and b_uids[1]:
        penalty += partner_counter.get(frozenset(b_uids), 0) * PARTNER_REPEAT_WEIGHT
    for x in a_uids:
        for y in b_uids:
            if x and y:
                penalty += opponent_counter.get(frozenset([x, y]), 0) * OPPONENT_REPEAT_WEIGHT
    return penalty


def _best_balanced_four(candidates, partner_counter=None, opponent_counter=None):
    """
    候補(4人以上、休憩ローテーション上「次に出るべき順」にソート済み)の中から
    4人+2v2分けを選ぶ。

    先頭(＝最も長く待っている・試合数が少ない人)は必ず含める。これを
    せずに純粋にバランス最良の4人だけを毎回総当たりで選んでいたところ、
    特定の人がスキル値の組み合わせの都合で何度選んでも外れ続け、
    ずっと待機のままになる不具合があった（休憩の公平性が実質機能しない）。
    残り3人は、その1人と組んだときに実力バランスが最良になるよう選ぶ。

    さらに、実力バランスがほぼ同点の候補が複数あるときは、直近の試合で
    同じ相手とパートナー/対戦済みの頻度が低い方を優先する（実力バランス
    自体を崩してまで多様性を優先することはない）。
    """
    from itertools import combinations

    def conservative(e):
        return float(e.get("skill_score", 50.0)) - 3 * float(e.get("skill_sigma", 8.333))

    must_include = candidates[0]
    rest_pool = candidates[1:]
    pairing_patterns = [((0, 1), (2, 3)), ((0, 2), (1, 3)), ((0, 3), (1, 2))]

    all_options = []  # [(diff, team_a, team_b), ...]
    for combo3 in combinations(rest_pool, 3):
        combo = (must_include,) + combo3
        scores = [conservative(e) for e in combo]
        for (i1, i2), (i3, i4) in pairing_patterns:
            diff = abs((scores[i1] + scores[i2]) - (scores[i3] + scores[i4]))
            all_options.append((diff, [combo[i1], combo[i2]], [combo[i3], combo[i4]]))

    min_diff = min(o[0] for o in all_options)

    use_tiebreak = bool(partner_counter) or bool(opponent_counter)
    if use_tiebreak:
        margin = max(BALANCE_TIEBREAK_MARGIN_FLOOR, min_diff * BALANCE_TIEBREAK_MARGIN_RATIO)
        near_best = [o for o in all_options if o[0] <= min_diff + margin]
        chosen = min(near_best, key=lambda o: _repeat_penalty2(o[1], o[2], partner_counter, opponent_counter))
    else:
        chosen = min(all_options, key=lambda o: o[0])

    diff, team_a, team_b = chosen
    return team_a, team_b, diff


def _fairness_first_four(candidates):
    """
    休憩ローテーション上の待機順、上位4人をそのまま採用する（スキルバランスは
    「誰を選ぶか」には一切使わない）。ただし選ばれた4人をどう2チームに
    分けるかだけは、3通りのパターンの中で最も実力差が小さいものを選ぶ
    （これは公平性に影響しないので併用して問題ない）。
    """
    def conservative(e):
        return float(e.get("skill_score", 50.0)) - 3 * float(e.get("skill_sigma", 8.333))

    four = candidates[:4]
    scores = [conservative(e) for e in four]
    pairing_patterns = [((0, 1), (2, 3)), ((0, 2), (1, 3)), ((0, 3), (1, 2))]

    best = None
    best_diff = float("inf")
    for (i1, i2), (i3, i4) in pairing_patterns:
        diff = abs((scores[i1] + scores[i2]) - (scores[i3] + scores[i4]))
        if diff < best_diff:
            best_diff = diff
            best = ([four[i1], four[i2]], [four[i3], four[i4]])

    team_a, team_b = best
    return team_a, team_b, best_diff


AI_PAIRING_POOL_SIZE = 6  # AIペアリングモードで「待機上位」とみなす人数

# 12ステップのサイクル:
#   バランス重視×3 → AIペアリング×2(待機調整)
#   → バランス無視×3 → AIペアリング×1(待機調整)
#   → バランス重視×2 → AIペアリング×1(待機調整) → 繰り返し
REFILL_MODE_CYCLE = [
    "balance_only", "balance_only", "balance_only",
    "ai_pairing", "ai_pairing",
    "fairness_first", "fairness_first", "fairness_first",
    "ai_pairing",
    "balance_only", "balance_only",
    "ai_pairing",
]


def _next_refill_mode(meta_table):
    """
    補充のたびに meta#continuous_pairing の refill_count を加算し、
    REFILL_MODE_CYCLE に従って次のモードを決める。
    """
    resp = meta_table.update_item(
        Key={"match_id": META_PAIRING_PK},
        UpdateExpression="ADD refill_count :one",
        ExpressionAttributeValues={":one": 1},
        ReturnValues="UPDATED_NEW",
    )
    refill_count = int(resp["Attributes"]["refill_count"])
    mode = REFILL_MODE_CYCLE[(refill_count - 1) % len(REFILL_MODE_CYCLE)]
    return mode, refill_count


def _try_refill_court(old_match_id, court_number):
    """
    1コート分の結果が確定した直後に呼ぶ。
    その4人をTrueSkill更新してpending化し、休憩ローテーション上位の候補の中から
    実力バランスが良い4人を選んで、このコートだけ新しい試合を発行する。
    """
    entry_table = _entry_table()
    results_table = _results_table()
    meta_table = _meta_table()

    finished_entries = entry_table.scan(
        FilterExpression=Attr("match_id").eq(str(old_match_id))
        & Attr("court_number").eq(court_number)
        & Attr("entry_status").eq("playing"),
        ConsistentRead=True,
    ).get("Items", [])

    if len(finished_entries) != 4:
        current_app.logger.warning(
            "[game2][continuous] court=%s の4人が揃っていません(%d人)。補充をスキップ",
            court_number, len(finished_entries),
        )
        return

    result = results_table.get_item(Key={"result_id": f"{old_match_id}#{court_number}"}).get("Item")
    if not result:
        return

    player_mapping = {e["user_id"]: e["entry_id"] for e in finished_entries if "user_id" in e}

    try:
        from game.game_utils import parse_players
        team_a = parse_players(result.get("team_a", []))
        team_b = parse_players(result.get("team_b", []))
        for pl in team_a + team_b:
            uid = pl.get("user_id")
            if uid in player_mapping:
                pl["entry_id"] = player_mapping[uid]
        result_item = {
            "team_a": team_a, "team_b": team_b, "winner": result.get("winner", "A"),
            "match_id": old_match_id, "court_number": court_number,
            "team1_score": result.get("team1_score"), "team2_score": result.get("team2_score"),
        }
        updated_skills = update_trueskill_for_players_and_return_updates(result_item)
        sync_match_entries_with_updated_skills2(player_mapping, updated_skills)
        persist_skill_to_bad_users(updated_skills)
    except Exception as e:
        current_app.logger.error("[game2][continuous] スキル更新エラー (court=%s): %s", court_number, e)

    now_jst = datetime.now(JST).isoformat()
    for e in finished_entries:
        entry_id = e["entry_id"]
        if e.get("rest_requested"):
            entry_table.update_item(
                Key={"entry_id": entry_id},
                UpdateExpression=(
                    "SET entry_status=:resting, updated_at=:now, "
                    "rest_count = if_not_exists(rest_count, :zero) + :one "
                    "REMOVE court_number, team, match_id, rest_requested"
                ),
                ExpressionAttributeValues={":resting": "resting", ":now": now_jst, ":zero": 0, ":one": 1},
            )
        else:
            entry_table.update_item(
                Key={"entry_id": entry_id},
                UpdateExpression=(
                    "SET entry_status=:pending, updated_at=:now, "
                    "match_count = if_not_exists(match_count, :zero) + :one "
                    "REMOVE court_number, team, match_id"
                ),
                ExpressionAttributeValues={":pending": "pending", ":now": now_jst, ":zero": 0, ":one": 1},
            )

    mode, refill_count = _next_refill_mode(meta_table)

    if mode == "ai_pairing":
        # 待機上位(長く待っている人)だけに候補を絞ってから、その中でバランス+履歴を見る
        candidates = _refill_candidate_pool(entry_table, pool_size=AI_PAIRING_POOL_SIZE)
    else:
        candidates = _refill_candidate_pool(entry_table)

    if len(candidates) < 4:
        current_app.logger.info(
            "[game2][continuous] court=%s 補充する人数が足りないため空けたままにします(候補%d人)",
            court_number, len(candidates),
        )
        return

    if mode == "fairness_first":
        team_a_entries, team_b_entries, diff = _fairness_first_four(candidates)
    elif mode == "ai_pairing":
        partner_counter, opponent_counter = _get_recent_pair_history2(results_table)
        team_a_entries, team_b_entries, diff = _best_balanced_four(candidates, partner_counter, opponent_counter)
    else:  # balance_only
        team_a_entries, team_b_entries, diff = _best_balanced_four(candidates)
    new_match_id = generate_match_id2()

    import boto3
    dynamodb_client = boto3.client("dynamodb", region_name="ap-northeast-1")
    tx_items = []
    for pl, team in [(team_a_entries[0], "A"), (team_a_entries[1], "A"),
                      (team_b_entries[0], "B"), (team_b_entries[1], "B")]:
        tx_items.append({
            "Update": {
                "TableName": "bad-game2-match_entries",
                "Key": {"entry_id": {"S": pl["entry_id"]}},
                "UpdateExpression": "SET entry_status=:playing, match_id=:mid, court_number=:c, team=:t, updated_at=:now",
                "ConditionExpression": "entry_status = :pending",
                "ExpressionAttributeValues": {
                    ":playing": {"S": "playing"}, ":pending": {"S": "pending"},
                    ":mid": {"S": str(new_match_id)}, ":c": {"N": str(court_number)},
                    ":t": {"S": team}, ":now": {"S": now_jst},
                },
            }
        })

    try:
        dynamodb_client.transact_write_items(TransactItems=tx_items)
        current_app.logger.info(
            "[game2][continuous] court=%s 自動補充(mode=%s, refill_count=%d): new_match_id=%s balance_diff=%.2f members=%s",
            court_number, mode, refill_count, new_match_id, diff,
            [e.get("display_name") for e in team_a_entries + team_b_entries],
        )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "TransactionCanceledException":
            current_app.logger.warning(
                "[game2][continuous] court=%s 補充tx競合のためスキップ（次の提出時に再試行される）", court_number
            )
        else:
            raise


@bp_game2.route("/submit_score/<match_id>/court/<int:court_number>", methods=["POST"])
@login_required
def submit_score(match_id, court_number):
    try:
        team1_raw = request.form.get("team1_score")
        team2_raw = request.form.get("team2_score")
        if team1_raw is None or team2_raw is None:
            flash("スコアが送信されていません", "danger")
            return redirect(url_for("game2.court"))
        team1_score = int(team1_raw)
        team2_score = int(team2_raw)
        if team1_score == team2_score:
            flash("スコアが同点です。勝者を決めてください。", "danger")
            return redirect(url_for("game2.court"))

        winner = "A" if team1_score > team2_score else "B"
        court_number_int = int(court_number)

        entry_table = _entry_table()
        entries = entry_table.scan(
            FilterExpression=Attr("match_id").eq(str(match_id)) & Attr("court_number").eq(court_number_int),
            ConsistentRead=True,
        ).get("Items", [])
        if not entries:
            flash("コートのエントリーが見つかりません（すでに次の試合に切り替わった可能性があります）", "warning")
            return redirect(url_for("game2.court"))

        if not current_user.administrator:
            court_user_ids = [str(e.get("user_id", "")) for e in entries]
            if current_user.user_id not in court_user_ids:
                flash("このコートへのスコア送信権限がありません", "danger")
                return redirect(url_for("game2.court"))

        team_a, team_b = [], []
        for e in entries:
            player = {
                "user_id": str(e.get("user_id", "")),
                "display_name": str(e.get("display_name", "不明")),
                "entry_id": str(e.get("entry_id", "")),
                "skill_score": Decimal(str(e.get("skill_score", 25.0))),
                "skill_sigma": Decimal(str(e.get("skill_sigma", 8.333))),
            }
            (team_a if e.get("team") == "A" else team_b).append(player)

        if not team_a or not team_b:
            flash("コートのチームデータが不完全です", "danger")
            return redirect(url_for("game2.court"))

        results_table = _results_table()
        result_id = f"{match_id}#{court_number_int}"
        try:
            results_table.put_item(
                Item={
                    "result_id": result_id, "match_id": str(match_id), "court_number": court_number_int,
                    "team1_score": team1_score, "team2_score": team2_score, "winner": winner,
                    "team_a": team_a, "team_b": team_b, "created_at": datetime.now(JST).isoformat(),
                },
                ConditionExpression="attribute_not_exists(result_id)",
            )
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return redirect(url_for("game2.court"))
            raise

        current_app.logger.info(
            "[game2] Score: Match=%s, Court=%d, %d-%d (Win:%s)",
            match_id, court_number_int, team1_score, team2_score, winner,
        )

        # ★Phase2: このコートの結果が確定した直後に、そのコートだけ自動補充する
        try:
            _try_refill_court(match_id, court_number_int)
        except Exception as e:
            current_app.logger.error(
                "[game2][continuous] court=%s 自動補充でエラー: %s", court_number_int, e, exc_info=True
            )

        flash(f"コート{court_number_int}のスコアを送信しました（{team1_score}-{team2_score}）", "success")
        return redirect(url_for("game2.court"))
    except Exception as e:
        current_app.logger.error("[game2][submit_score ERROR] %s", str(e), exc_info=True)
        flash("スコアの送信中にエラーが発生しました", "danger")
        return redirect(url_for("game2.court"))


@bp_game2.route("/finish_current_match", methods=["POST"])
@login_required
def finish_current_match():
    if not current_user.administrator:
        flash("管理者のみ実行できます。", "danger")
        return redirect(url_for("game2.court"))

    meta_table = _meta_table()
    meta_item = meta_table.get_item(Key={"match_id": META_CURRENT_PK}, ConsistentRead=True).get("Item") or {}
    status = meta_item.get("status")
    match_id = meta_item.get("current_match_id")
    court_count = int(meta_item.get("court_count", 0) or 0)

    if status != "playing" or not match_id:
        flash("テストコート: アクティブな試合が見つかりません", "warning")
        return redirect(url_for("game2.court"))

    results_table = _results_table()
    match_results = results_table.scan(FilterExpression=Attr("match_id").eq(match_id)).get("Items", [])
    if len(match_results) < court_count:
        flash(f"テストコート: 未送信のコートがあります（{len(match_results)}/{court_count}）", "warning")
        return redirect(url_for("game2.court"))

    entry_table = _entry_table()
    playing_players = entry_table.scan(
        FilterExpression=Attr("match_id").eq(match_id) & Attr("entry_status").eq("playing")
    ).get("Items", [])
    player_mapping = {p["user_id"]: p["entry_id"] for p in playing_players if "user_id" in p and "entry_id" in p}

    updated_skills = {}
    for result in match_results:
        try:
            from game.game_utils import parse_players
            team_a = parse_players(result.get("team_a", []))
            team_b = parse_players(result.get("team_b", []))
            for pl in team_a + team_b:
                uid = pl.get("user_id")
                if uid in player_mapping:
                    pl["entry_id"] = player_mapping[uid]
            result_item = {
                "team_a": team_a, "team_b": team_b, "winner": result.get("winner", "A"),
                "match_id": match_id, "court_number": result.get("court_number"),
                "team1_score": result.get("team1_score"), "team2_score": result.get("team2_score"),
            }
            updated_skills.update(update_trueskill_for_players_and_return_updates(result_item))
        except Exception as e:
            current_app.logger.error("[game2] スキル更新エラー (court=%s): %s", result.get("court_number"), e)

    sync_match_entries_with_updated_skills2(player_mapping, updated_skills)
    persist_skill_to_bad_users(updated_skills)

    import boto3
    dynamodb_client = boto3.client("dynamodb", region_name="ap-northeast-1")
    now_jst = datetime.now(JST).isoformat()

    tx_items = [{
        "Update": {
            "TableName": "bad-game-matches",
            "Key": {"match_id": {"S": META_CURRENT_PK}},
            "UpdateExpression": "SET #st = :idle, #ua = :now REMOVE #cm, #cc",
            "ConditionExpression": "#st = :playing AND #cm = :mid",
            "ExpressionAttributeNames": {"#st": "status", "#ua": "updated_at", "#cm": "current_match_id", "#cc": "court_count"},
            "ExpressionAttributeValues": {":idle": {"S": "idle"}, ":playing": {"S": "playing"}, ":mid": {"S": str(match_id)}, ":now": {"S": now_jst}},
        }
    }]
    for p in playing_players:
        entry_id = p.get("entry_id")
        if not entry_id:
            continue
        tx_items.append({
            "Update": {
                "TableName": "bad-game2-match_entries",
                "Key": {"entry_id": {"S": str(entry_id)}},
                "UpdateExpression": "SET entry_status=:pending, updated_at=:now REMOVE court_number, team, match_id",
                "ConditionExpression": "entry_status = :playing AND match_id = :mid",
                "ExpressionAttributeValues": {
                    ":pending": {"S": "pending"}, ":playing": {"S": "playing"},
                    ":mid": {"S": str(match_id)}, ":now": {"S": now_jst},
                },
            }
        })

    try:
        dynamodb_client.transact_write_items(TransactItems=tx_items)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "TransactionCanceledException":
            return jsonify({"success": False, "error": "finish transaction canceled"}), 409
        raise

    flash("テストコート: 試合を終了しました", "success")
    return redirect(url_for("game2.court"))


@bp_game2.route("/emergency_transfer", methods=["POST"])
@login_required
def emergency_transfer():
    """
    緊急脱出ボタン: テストコートの参加者全員を、既存システム（本番のコート）の
    待機列(pending)に移す。テストコート側の状態は初期化する。
    既存システムのコードには一切触れず、bad-game-match_entries に
    「今から並び直す人」として新規レコードを作るだけなので安全。
    """
    if not current_user.administrator:
        flash("管理者のみ実行できます。", "danger")
        return redirect(url_for("game2.court"))

    entry_table = _entry_table()
    main_entry_table = current_app.dynamodb.Table("bad-game-match_entries")
    now = datetime.now(JST).isoformat()

    game2_entries = entry_table.scan().get("Items", [])

    # 既に本番側にアクティブなエントリーがある人は二重登録を避けるため対象外にする
    main_active = main_entry_table.scan(
        FilterExpression=Attr("entry_status").is_in(["pending", "playing", "resting"])
    ).get("Items", [])
    main_active_uids = {e.get("user_id") for e in main_active}

    moved, skipped = 0, 0
    for e in game2_entries:
        uid = e.get("user_id")
        if uid in main_active_uids:
            current_app.logger.warning("[game2][emergency] 本番側に既存エントリーあり、移動をスキップ: %s", uid)
            skipped += 1
        else:
            main_entry_table.put_item(Item={
                "entry_id": str(uuid.uuid4()),
                "user_id": uid,
                "match_id": "pending",
                "entry_status": "pending",
                "display_name": e.get("display_name", "未設定"),
                "gender": e.get("gender", "M"),
                "skill_score": e.get("skill_score", Decimal("50")),
                "skill_sigma": e.get("skill_sigma", Decimal("8.333")),
                "joined_at": now,
                "created_at": now,
                "rest_count": 0,
                "match_count": 0,
            })
            moved += 1
        entry_table.delete_item(Key={"entry_id": e["entry_id"]})

    meta_table = _meta_table()
    meta_table.update_item(
        Key={"match_id": META_CURRENT_PK},
        UpdateExpression="SET #st = :idle, #ua = :now REMOVE current_match_id, court_count",
        ExpressionAttributeNames={"#st": "status", "#ua": "updated_at"},
        ExpressionAttributeValues={":idle": "idle", ":now": now},
    )

    current_app.logger.warning(
        "[game2][emergency] 緊急移動実行: moved=%d skipped=%d by=%s",
        moved, skipped, current_user.get_id(),
    )
    flash(f"テストコートの参加者{moved}人を通常コートに移動しました。（本番側に既存登録があり{skipped}人はスキップ）", "warning")
    return redirect(url_for("game.court"))

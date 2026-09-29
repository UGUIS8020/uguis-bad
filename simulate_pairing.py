#!/usr/bin/env python
"""
テストコート(game2)のペアリングアルゴリズムを、本番DBに触れずローカルで
シミュレーションするスクリプト。

本番と全く同じ関数(_best_balanced_four, _fairness_first_four,
_repeat_penalty2 など)を game2/views.py からそのまま import して使うので、
「本番で実際に動いているロジック」をそのまま大量試行できる。これにより、
本番で10試合ずつ試すより遥かに大きいサンプル(数百試合)で、モード配分の
比較を素早く・ノイズ少なく行える。

使い方:
  python simulate_pairing.py                    # 現在の本番サイクルで300試合
  python simulate_pairing.py --matches 500       # 試合数を変える
  python simulate_pairing.py --compare           # 複数のサイクル案を一括比較
"""
import argparse
import random
import statistics
from collections import Counter, defaultdict
from itertools import groupby

from game2.views import (
    _best_balanced_four,
    _fairness_first_four,
    _full_random_four,
    _skill_priority_four,
    _repeat_penalty2,
    RECENT_HISTORY_RESULTS,
    WAIT_RESCUE_THRESHOLD,
    QUEUE_FORCE_COUNT,
    INITIAL_FULL_RANDOM_COUNT,
    INITIAL_AI_PURE_COUNT,
    SKILL_BURST_INTERVAL_MINUTES,
)

NUM_PLAYERS = 18
NUM_COURTS = 3


def make_players(n=NUM_PLAYERS, seed=None):
    rng = random.Random(seed)
    players = []
    for i in range(n):
        players.append({
            "user_id": f"u{i}",
            "display_name": f"P{i:02d}",
            "skill_score": rng.randint(15, 70),
            "skill_sigma": 8.333,
            "match_count": 0,
            "wait_rounds": 0,
            "joined_at": f"{i:04d}",  # チェックイン順の代わり(固定)
        })
    return players


def get_recent_history_local(recent_results, max_results=RECENT_HISTORY_RESULTS):
    """_get_recent_pair_history2 のDynamoDB版と同じロジックをメモリ上で行う"""
    recent = recent_results[-max_results:]
    partner_counter = Counter()
    opponent_counter = Counter()
    for team_a, team_b in recent:
        a_uids = [p["user_id"] for p in team_a]
        b_uids = [p["user_id"] for p in team_b]
        if len(a_uids) == 2:
            partner_counter[frozenset(a_uids)] += 1
        if len(b_uids) == 2:
            partner_counter[frozenset(b_uids)] += 1
        for x in a_uids:
            for y in b_uids:
                opponent_counter[frozenset([x, y])] += 1
    return partner_counter, opponent_counter


def skill_sorted_pending_local(pending):
    """スキルモード用: 休憩ローテーションは無視し、スキルスコア降順で返す"""
    def conservative(e):
        return e["skill_score"] - 3 * e["skill_sigma"]
    return sorted(pending, key=conservative, reverse=True)


def pop_next_from_queue_local(queue_state, pending, count=1):
    """
    本番の_pop_next_from_play_queue()と同じロジックをメモリ上で行う。
    queue_stateは{"queue": [...], "last_picked": [...]}をシミュレーション
    全体で使い回す(本番のDynamoDB永続キューに相当)。
    """
    by_id = {p["user_id"]: p for p in pending}
    current_user_set = set(by_id.keys())
    queue = [uid for uid in queue_state["queue"] if uid in current_user_set]

    if len(queue) < count:
        last_picked_uids = set(queue_state.get("last_picked", []))
        sorted_entries = sorted(pending, key=lambda e: (e["match_count"], e.get("joined_at", "")))
        all_uids = [e["user_id"] for e in sorted_entries]
        others = [uid for uid in all_uids if uid not in last_picked_uids]
        prev_picked = [uid for uid in all_uids if uid in last_picked_uids]

        shuffled = []
        for _, g in groupby(others, key=lambda uid: by_id[uid]["match_count"]):
            g = list(g)
            random.shuffle(g)
            shuffled.extend(g)

        new_queue_ordered = shuffled + prev_picked
        picked_uids = new_queue_ordered[:count]
        queue_next = new_queue_ordered[count:]
    else:
        picked_uids = queue[:count]
        queue_next = queue[count:]

    queue_state["queue"] = queue_next
    queue_state["last_picked"] = picked_uids
    return [by_id[uid] for uid in picked_uids if uid in by_id]


def simulate(n_matches, seed=None,
             num_players=NUM_PLAYERS, num_courts=NUM_COURTS,
             minutes_per_match=10.0,
             initial_full_random=INITIAL_FULL_RANDOM_COUNT,
             initial_ai_pure=INITIAL_AI_PURE_COUNT,
             skill_burst_interval_minutes=SKILL_BURST_INTERVAL_MINUTES,
             enable_continuous_balance=True,
             continuous_balance_force_lowest=False,
             enable_rescue=True,
             wait_rescue_threshold=WAIT_RESCUE_THRESHOLD,
             late_joiner_refill_counts=None,
             trace=False):
    """
    本番の_next_refill_mode() + スキルモード一斉入れ替え(_try_refill_court /
    _process_skill_burst / _execute_skill_burst)+ 継続的バランス調整と同じ
    ロジックをシミュレートする。実際の壁時計時間の代わりに、1試合あたり
    minutes_per_match分かかると仮定して経過時間を積算する(コートは
    num_courts面並行で進むので、1回の補充ごとにminutes_per_match/num_courts
    分だけ経過したとみなす)。

    モードの流れ(本番と同じ): 最初のinitial_full_random回はランダム、続く
    initial_ai_pure回は調整なしの純粋なAIペアリング、それ以降は毎回、参加
    回数が最も多い人を除外する「調整AIペアリング」のみ。

    スキルモードは、前回の一斉入れ替えからskill_burst_interval_minutes分
    経過するたびに、空いたコートを即座には補充せず全コート(num_courts面)が
    空くまで集め、待機中全員をスキル順の階層で一括採用する(held_courts集合で
    シミュレート、繰り返し発動)。
    スキルバースト後は特別扱いせず、即座に通常のfull_random/ai_pairing
    ローテーションに戻る(本番と同じ)。
    """
    rng = random.Random(seed)
    players = make_players(n=num_players, seed=seed)
    by_uid = {p["user_id"]: p for p in players}

    rng.shuffle(players)
    courts = {}  # court_num -> {"team_a":[...], "team_b":[...]}
    for c in range(1, num_courts + 1):
        four = players[(c - 1) * 4: c * 4]
        courts[c] = {"team_a": four[:2], "team_b": four[2:]}
    pending = players[num_courts * 4:]

    recent_results = []  # [(team_a, team_b), ...] 新しい順ではなく古い順に追加
    match_log = []  # 全試合: (court_num, team_a, team_b, diff)
    refill_count = 0
    queue_state = {"queue": [], "last_picked": []}  # 本番のDynamoDB永続キューに相当
    elapsed_minutes = 0.0
    # ★本番のcreate_pairings()と同じく、練習開始の瞬間を基準点にする
    last_skill_burst_at = 0.0
    held_courts = set()  # スキルモード一斉入れ替え待ちで、今は試合が無いコート

    for _ in range(n_matches):
        playing_courts = [c for c in courts if c not in held_courts]
        if not playing_courts:
            break
        # 実際の練習ではどのコートが次に終わるかはランダム(機械的な順番ではない)
        court_num = rng.choice(playing_courts)
        finished = courts[court_num]
        finished_all = finished["team_a"] + finished["team_b"]

        for p in finished_all:
            p["match_count"] += 1
            p["wait_rounds"] = 0
            pending.append(p)

        for p in pending:
            p["wait_rounds"] += 1
            p["max_wait_seen"] = max(p.get("max_wait_seen", 0), p["wait_rounds"])

        diff_for_log = abs(
            sum(p["skill_score"] - 3 * p["skill_sigma"] for p in finished["team_a"])
            - sum(p["skill_score"] - 3 * p["skill_sigma"] for p in finished["team_b"])
        )
        match_log.append((court_num, finished["team_a"], finished["team_b"], diff_for_log))
        recent_results.append((finished["team_a"], finished["team_b"]))

        refill_count += 1
        elapsed_minutes += minutes_per_match / num_courts
        del courts[court_num]  # このコートは今、試合が無い状態

        # ★遅れて来た参加者のシミュレーション: 指定したrefill_countの
        #   タイミングで、match_count=0の新規参加者をpendingに追加する
        #   (「参加回数が最も少ない人を強制参加」が遅れてきた人にどう
        #   影響するかを検証するためのオプション)
        if late_joiner_refill_counts:
            new_count = late_joiner_refill_counts.count(refill_count)
            for _ in range(new_count):
                new_id = f"late{refill_count}_{len(by_uid)}"
                newcomer = {
                    "user_id": new_id,
                    "display_name": f"遅刻{refill_count}",
                    "skill_score": rng.randint(15, 70),
                    "skill_sigma": 8.333,
                    "match_count": 0,
                    "wait_rounds": 0,
                    "joined_refill_count": refill_count,
                    "joined_at": f"9{refill_count:04d}",
                }
                by_uid[new_id] = newcomer
                pending.append(newcomer)
                if trace:
                    print(f"#{refill_count} 新規参加登録: {new_id}")

        # ★スキルモード一斉入れ替え: 収集中(held_courtsが既に非空)なら経過時間
        #   に関わらず合流。まだなら、前回の一斉入れ替えからskill_burst_
        #   interval_minutes分以上経過していれば新規に収集を始める(繰り返し発動)。
        skill_burst_collecting = bool(held_courts) or (
            elapsed_minutes - last_skill_burst_at >= skill_burst_interval_minutes
        )

        if skill_burst_collecting:
            held_courts.add(court_num)
            if trace:
                print(f"#{refill_count} court={court_num} スキルモード一斉入れ替え待ちに登録"
                      f" (held={sorted(held_courts)})")
            if len(held_courts) >= num_courts:
                candidates = skill_sorted_pending_local(pending)
                held_list = sorted(held_courts)
                usable_groups = min(len(held_list), len(candidates) // 4)
                for i in range(usable_groups):
                    c = held_list[i]
                    four = candidates[i * 4:(i + 1) * 4]
                    team_a, team_b, _diff = _skill_priority_four(four)
                    chosen_uids = {p["user_id"] for p in team_a + team_b}
                    pending = [p for p in pending if p["user_id"] not in chosen_uids]
                    courts[c] = {"team_a": team_a, "team_b": team_b}
                    if trace:
                        print(f"    → court={c} スキルモード一斉補充: {sorted(chosen_uids)}")
                held_courts -= set(held_list[:usable_groups])
                last_skill_burst_at = elapsed_minutes  # このバーストを消費済みにする
            continue

        # ★本番_next_refill_mode()と同じ時間ベースのモード選定(full_random/ai_pairingのみ)
        mode = "full_random" if refill_count <= initial_full_random else "ai_pairing"

        # ★完全ランダムは練習開始直後のinitial_full_random回だけ使う、あえて
        #   何の調整もしないシンプルなモード。救済モード・永続キューによる
        #   強制・継続的バランス調整は一切適用せず、待機中から純粋にランダムに
        #   4人選ぶ(本番と同じ)。
        if mode == "full_random":
            rescued = []
            assert len(pending) >= 4, "num_players >= 4*num_courts を前提としており、通常は発生しない"
            team_a, team_b, _diff = _full_random_four(pending, force_top_n=0)
        else:
            # ★救済モード: wait_rescue_threshold回以上待った人がいれば、
            #   強制的に含める(永続キューとは別枠の保険、本番と同じ二重構成)
            rescued = sorted(
                [p for p in pending if p["wait_rounds"] >= wait_rescue_threshold],
                key=lambda p: -p["wait_rounds"],
            )[:4] if enable_rescue else []

            if rescued:
                others = [p for p in pending if p not in rescued]
                candidates = rescued + others
                assert len(candidates) >= 4, "num_players >= 4*num_courts を前提としており、通常は発生しない"
                partner_counter, opponent_counter = get_recent_history_local(recent_results)
                team_a, team_b, _diff = _best_balanced_four(
                    candidates, partner_counter, opponent_counter, force_top_n=len(rescued)
                )
            else:
                # AIペアリング: 永続キューの先頭QUEUE_FORCE_COUNT人を必ず含める
                forced = pop_next_from_queue_local(queue_state, pending, count=QUEUE_FORCE_COUNT)
                assert forced, "pendingは直前に終わった4人を含むため通常は空にならない"
                forced_uids = {p["user_id"] for p in forced}

                # ★継続的バランス調整: ランダムinitial_full_random回・純粋な
                #   AIペアリングinitial_ai_pure回が終わった後は、毎回参加回数が
                #   最も多い人を今回の候補から除外する(休憩にはしない。次回は
                #   また対象になりうる)。
                apply_continuous_balance = (
                    enable_continuous_balance
                    and refill_count > (initial_full_random + initial_ai_pure)
                )

                excluded_uid = None
                if apply_continuous_balance:
                    if continuous_balance_force_lowest:
                        remaining = [p for p in pending if p["user_id"] not in forced_uids]
                        if remaining:
                            lowest = min(remaining, key=lambda p: p["match_count"])
                            if lowest["user_id"] not in forced_uids:
                                forced = forced + [lowest]
                                forced_uids.add(lowest["user_id"])
                    remaining2 = [p for p in pending if p["user_id"] not in forced_uids]
                    if remaining2:
                        highest = max(remaining2, key=lambda p: p["match_count"])
                        excluded_uid = highest["user_id"]

                rest_pool = [
                    p for p in pending
                    if p["user_id"] not in forced_uids and p["user_id"] != excluded_uid
                ]
                candidates = forced + rest_pool
                if len(candidates) < 4:
                    # 除外すると4人未満になる場合は、除外をやめて通常通りにする
                    rest_pool = [p for p in pending if p["user_id"] not in forced_uids]
                    candidates = forced + rest_pool
                assert len(candidates) >= 4, "num_players >= 4*num_courts を前提としており、通常は発生しない"

                partner_counter, opponent_counter = get_recent_history_local(recent_results)
                team_a, team_b, _diff = _best_balanced_four(candidates, partner_counter, opponent_counter, force_top_n=len(forced))

        chosen_uids = {p["user_id"] for p in team_a + team_b}
        if trace:
            used_mode = f"RESCUE({len(rescued)})" if rescued else mode
            pending_uids_before = sorted(p["user_id"] for p in pending)
            print(f"#{refill_count} court={court_num} mode={used_mode} 選ばれた={sorted(chosen_uids)}"
                  f"  pending_before={pending_uids_before}")
        pending = [p for p in pending if p["user_id"] not in chosen_uids]
        courts[court_num] = {"team_a": team_a, "team_b": team_b}

    # シミュレーション終了時点でまだpendingの人も、その時点のwait_roundsを
    # 最終的な「経験した最大待ち」として反映しておく
    for p in pending:
        p["max_wait_seen"] = max(p.get("max_wait_seen", 0), p["wait_rounds"])

    return match_log, by_uid


def evaluate(match_log, label, by_uid=None):
    # ★待ちラウンド: match_logの出現間隔(gap)は「自分の試合がたまたま長引いた
    #   (=自分のコートがランダム抽選で長く選ばれなかった)」場合も巻き込んで
    #   しまい、実際に待機(pending)していた時間とズレることがある。
    #   by_uidが渡されれば、各プレイヤーが実際にpending中に経験した
    #   最大wait_rounds(max_wait_seen)を使う方が正確。
    if by_uid:
        all_gaps = [p.get("max_wait_seen", 0) for p in by_uid.values()]
    else:
        appearances = defaultdict(list)
        for i, (court_num, team_a, team_b, diff) in enumerate(match_log):
            for p in team_a + team_b:
                appearances[p["user_id"]].append(i)
        all_gaps = []
        for uid, idxs in appearances.items():
            gaps = [idxs[j + 1] - idxs[j] - 1 for j in range(len(idxs) - 1)]
            all_gaps.extend(gaps)

    diffs = [d for _, _, _, d in match_log]

    partner_counter = Counter()
    opponent_counter = Counter()
    for _, team_a, team_b, _ in match_log:
        a_uids = [p["user_id"] for p in team_a]
        b_uids = [p["user_id"] for p in team_b]
        partner_counter[frozenset(a_uids)] += 1
        partner_counter[frozenset(b_uids)] += 1
        for x in a_uids:
            for y in b_uids:
                opponent_counter[frozenset([x, y])] += 1
    repeat_partner = sum(1 for v in partner_counter.values() if v >= 2)
    repeat_opp = sum(1 for v in opponent_counter.values() if v >= 2)
    max_partner = max(partner_counter.values())
    max_opp = max(opponent_counter.values())

    print(f"\n=== {label} ({len(match_log)}試合) ===")
    print(f"  待ちラウンド: 平均={statistics.mean(all_gaps):.2f}  最大={max(all_gaps)}  中央値={statistics.median(all_gaps):.1f}")
    print(f"  実力差: 平均={statistics.mean(diffs):.2f}  最大={max(diffs):.2f}  中央値={statistics.median(diffs):.2f}")
    print(f"  重複: パートナー2回以上={repeat_partner}種類(最大{max_partner}回)  対戦2回以上={repeat_opp}種類(最大{max_opp}回)")

    return {
        "wait_avg": statistics.mean(all_gaps), "wait_max": max(all_gaps),
        "diff_avg": statistics.mean(diffs), "diff_max": max(diffs),
        "repeat_partner": repeat_partner, "repeat_opp": repeat_opp,
    }


# --compareで比較する、スキルモードを差し込む間隔(分)の候補
CANDIDATE_BURST_INTERVALS = {
    "30分ごと": 30,
    "現行本番(60分ごと)": SKILL_BURST_INTERVAL_MINUTES,
    "90分ごと": 90,
    "差し込みなし(999分=実質無し)": 999,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--matches", type=int, default=300)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--compare", action="store_true")
    args = parser.parse_args()

    if args.compare:
        results = {}
        for label, interval in CANDIDATE_BURST_INTERVALS.items():
            log, by_uid = simulate(args.matches, seed=args.seed, skill_burst_interval_minutes=interval)
            results[label] = evaluate(log, label, by_uid)

        print("\n" + "=" * 70)
        print("=== まとめ ===")
        print(f"{'スキル差し込み間隔':30s} {'待ち平均':>8s} {'待ち最大':>8s} {'差平均':>8s} {'差最大':>8s} {'重複計':>6s}")
        for label, m in results.items():
            total_repeat = m["repeat_partner"] + m["repeat_opp"]
            print(f"{label:30s} {m['wait_avg']:8.2f} {m['wait_max']:8d} {m['diff_avg']:8.2f} {m['diff_max']:8.2f} {total_repeat:6d}")
    else:
        log, by_uid = simulate(args.matches, seed=args.seed)
        evaluate(log, "現行本番設定", by_uid)


if __name__ == "__main__":
    try:
        import sys
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    main()

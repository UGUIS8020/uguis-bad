"""
bad-game-results 分析スクリプト（bad-game-history から移行）
使い方:
  python analyze_history.py                    # 全履歴
  python analyze_history.py 20260305           # 日付指定
  python analyze_history.py 20260305 20260306  # 期間指定
  python analyze_history.py --skip UGUIS渋谷   # プレイヤー除外
"""

import subprocess, json, sys, argparse
from collections import defaultdict

# ─── 除外プレイヤー（デフォルト） ─────────────────────────────
DEFAULT_SKIP = ["UGUIS渋谷"]

# ─── 本番開始日（これより前はテストデータとして除外） ──────────
PRODUCTION_START = "20260305"


def fetch_results(date_prefix: str = None) -> list:
    """bad-game-results を全件スキャン（match_id プレフィックスでフィルタ可）"""
    cmd = [
        "aws", "dynamodb", "scan",
        "--table-name", "bad-game-results",
        "--output", "json", "--no-cli-pager"
    ]
    if date_prefix:
        cmd += [
            "--filter-expression", "begins_with(match_id, :d)",
            "--expression-attribute-values", json.dumps({":d": {"S": date_prefix}})
        ]
    r = subprocess.run(cmd, capture_output=True, env={**__import__("os").environ, "PYTHONUTF8": "1"})
    return json.loads(r.stdout)["Items"]


def parse_player_list(raw_list):
    """DynamoDB typed JSON の team_a / team_b リストをパース"""
    result = []
    for p in raw_list.get("L", []):
        m = p.get("M", {})
        name = m.get("display_name", {}).get("S", "")
        uid  = m.get("user_id",      {}).get("S", "")
        result.append((name, uid))
    return result


def group_results_by_match(items: list) -> dict:
    """
    1コート1レコードのリストを match_id でグループ化し、
    analyze_history.py と同じ構造の dict に再構築する。
    """
    groups = defaultdict(list)
    for item in items:
        mid = item["match_id"]["S"]
        groups[mid].append(item)

    matches = {}
    for mid, courts_raw in groups.items():
        # コート番号順にソート
        courts_raw.sort(key=lambda x: int(x.get("court_number", {}).get("N", 0)))

        # 最初のレコードから試合共通情報を取得
        first = courts_raw[0]
        mode     = first.get("pairing_mode",    {}).get("S", "unknown")
        date_str = first.get("created_at",      {}).get("S", "")[:16]
        snap_raw = first.get("skill_snapshot",  {}).get("M", {})
        waiting  = [w["S"] for w in first.get("waiting_players", {}).get("L", [])]

        # コートごとのデータ構築
        courts = []
        for c in courts_raw:
            team_a = parse_player_list(c.get("team_a", {}))
            team_b = parse_player_list(c.get("team_b", {}))

            def avg_score(team, snap, skip_names):
                scores = []
                for name, uid in team:
                    if name in skip_names:
                        continue
                    if uid in snap:
                        scores.append(float(snap[uid]["M"]["skill_score"]["S"]))
                return sum(scores) / len(scores) if scores else None

            courts.append({
                "court_number": c.get("court_number", {}).get("N", "?"),
                "team_a":  team_a,
                "team_b":  team_b,
                "score_a": str(c.get("team1_score", {}).get("N", "?")),
                "score_b": str(c.get("team2_score", {}).get("N", "?")),
                "winner":  c.get("winner", {}).get("S", "?"),
                # avg は analyze() 内で skip_names を使って計算するため仮に None
                "avg_a":   None,
                "avg_b":   None,
                "_snap":   snap_raw,  # avg 計算用に一時保持
            })

        matches[mid] = {
            "match_id":    mid,
            "mode":        mode,
            "date":        date_str,
            "court_count": str(len(courts)),
            "snap":        snap_raw,
            "courts":      courts,
            "waiting":     waiting,
        }
    return matches


def parse_items(items: list, skip_names: list) -> list:
    """グループ化 → avg_a / avg_b を skip_names を考慮して計算"""
    matches = group_results_by_match(items)
    parsed = []
    for mid, m in matches.items():
        snap = m["snap"]
        for c in m["courts"]:
            snap_for_court = c.pop("_snap", snap)

            def avg_score(team):
                scores = []
                for name, uid in team:
                    if name in skip_names:
                        continue
                    if uid in snap_for_court:
                        scores.append(float(snap_for_court[uid]["M"]["skill_score"]["S"]))
                return sum(scores) / len(scores) if scores else None

            c["avg_a"] = avg_score(c["team_a"])
            c["avg_b"] = avg_score(c["team_b"])

        parsed.append(m)

    return sorted(parsed, key=lambda x: x["match_id"])


# ─── 以下は変更なし ───────────────────────────────────────────

def print_separator(char="─", width=64):
    print(char * width)


def analyze(items: list, skip_names: list):
    parsed = parse_items(items, skip_names)
    if not parsed:
        print("データがありません")
        return

    print_separator("═")
    print(f"  🏸 bad-game-results 分析レポート")
    print(f"  対象試合数: {len(parsed)}  除外プレイヤー: {skip_names or 'なし'}")
    print_separator("═")

    # ─── 1. 試合一覧 ──────────────────────────────────────────
    print("\n【1. 試合一覧】")
    print(f"{'match_id':<22} {'mode':<12} {'コート':>4} {'待機':>4} {'記録時刻'}")
    print_separator()
    for m in parsed:
        print(f"{m['match_id']:<22} {m['mode']:<12} {m['court_count']:>4}面 {len(m['waiting']):>3}人  {m['date']}")

    # ─── 2. コートバランス分析 ────────────────────────────────
    print("\n【2. コートバランス分析（チーム平均スキル差）】")
    print(f"{'match_id':<22} {'mode':<12} {'平均差':>6}  {'最大差':>6}  {'高側勝'}")
    print_separator()
    mode_stats = defaultdict(lambda: {"diffs": [], "upsets": 0, "total": 0})
    for m in parsed:
        diffs = []
        upsets = 0
        for c in m["courts"]:
            if c["avg_a"] is None or c["avg_b"] is None:
                continue
            diff = abs(c["avg_a"] - c["avg_b"])
            diffs.append(diff)
            higher = "A" if c["avg_a"] > c["avg_b"] else "B"
            if c["winner"] != higher:
                upsets += 1
            mode_stats[m["mode"]]["diffs"].append(diff)
            mode_stats[m["mode"]]["total"] += 1
            if c["winner"] != higher:
                mode_stats[m["mode"]]["upsets"] += 1
        if diffs:
            upset_str = f"{upsets}/{len(diffs)}番狂わせ"
            print(f"{m['match_id']:<22} {m['mode']:<12} {sum(diffs)/len(diffs):>5.1f}pt  {max(diffs):>5.1f}pt  {upset_str}")

    # ─── 3. mode別比較 ────────────────────────────────────────
    print("\n【3. ペアリングモード比較】")
    print(f"{'mode':<12} {'試合数':>6} {'平均チーム差':>10} {'番狂わせ率':>10}")
    print_separator()
    for mode, s in mode_stats.items():
        if s["diffs"]:
            avg_diff = sum(s["diffs"]) / len(s["diffs"])
            upset_rate = s["upsets"] / s["total"] * 100
            print(f"{mode:<12} {s['total']:>6}コート {avg_diff:>8.1f}pt  {upset_rate:>8.1f}%")

    # ─── 4. 選手別成績 ────────────────────────────────────────
    print("\n【4. 選手別成績（除外プレイヤー以外）】")
    player_stats = defaultdict(lambda: {"wins": 0, "loses": 0, "scores": []})
    for m in parsed:
        snap = m["snap"]
        for c in m["courts"]:
            for name, uid in c["team_a"]:
                if name in skip_names: continue
                result = "wins" if c["winner"] == "A" else "loses"
                player_stats[name][result] += 1
                if uid in snap:
                    player_stats[name]["scores"].append(float(snap[uid]["M"]["skill_score"]["S"]))
            for name, uid in c["team_b"]:
                if name in skip_names: continue
                result = "wins" if c["winner"] == "B" else "loses"
                player_stats[name][result] += 1
                if uid in snap:
                    player_stats[name]["scores"].append(float(snap[uid]["M"]["skill_score"]["S"]))

    print(f"{'名前':<18} {'勝':>4} {'負':>4} {'勝率':>7} {'スキル（最終）':>12}")
    print_separator()
    sorted_players = sorted(player_stats.items(), key=lambda x: -(x[1]["scores"][-1] if x[1]["scores"] else 0))
    for name, s in sorted_players:
        total = s["wins"] + s["loses"]
        win_rate = s["wins"] / total * 100 if total > 0 else 0
        last_score = s["scores"][-1] if s["scores"] else None
        score_str = f"{last_score:.1f}pt" if last_score else "?"
        print(f"{name:<18} {s['wins']:>4} {s['loses']:>4} {win_rate:>6.1f}%  {score_str:>10}")

    # ─── 5. スキルスコア変動 ──────────────────────────────────
    print("\n【5. スキルスコア変動（初回→最終）】")
    player_score_history = defaultdict(list)
    for m in parsed:
        snap = m["snap"]
        for c in m["courts"]:
            for name, uid in c["team_a"] + c["team_b"]:
                if name in skip_names: continue
                if uid in snap:
                    player_score_history[name].append(float(snap[uid]["M"]["skill_score"]["S"]))

    print(f"{'名前':<18} {'初回':>8} {'最終':>8} {'変動':>8}")
    print_separator()
    for name, scores in sorted(player_score_history.items(), key=lambda x: -(x[1][-1] - x[1][0])):
        if len(scores) >= 2:
            delta = scores[-1] - scores[0]
            sign = "+" if delta >= 0 else ""
            print(f"{name:<18} {scores[0]:>7.1f}pt {scores[-1]:>7.1f}pt {sign}{delta:>6.1f}pt")

    print_separator("═")
    print("  分析完了")
    print_separator("═")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="bad-game-results 分析")
    parser.add_argument("dates", nargs="*", help="日付プレフィックス (例: 20260305)")
    parser.add_argument("--skip", nargs="*", default=DEFAULT_SKIP, help="除外プレイヤー名")
    parser.add_argument("--all", action="store_true", help="テストデータを含む全履歴を対象にする")
    args = parser.parse_args()

    if not args.dates:
        if args.all:
            print("全履歴を取得中（テストデータ含む）...")
            items = fetch_results()
        else:
            print(f"本番データを取得中（{PRODUCTION_START}以降）...")
            all_items = fetch_results()
            items = [i for i in all_items if i["match_id"]["S"][:8] >= PRODUCTION_START]
    elif len(args.dates) == 1:
        print(f"{args.dates[0]} のデータを取得中...")
        items = fetch_results(args.dates[0])
    else:
        print(f"{args.dates[0]}〜{args.dates[1]} のデータを取得中...")
        all_items = fetch_results()
        items = [i for i in all_items if args.dates[0] <= i["match_id"]["S"][:8] <= args.dates[1]]

    analyze(items, args.skip)
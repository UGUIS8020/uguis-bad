from flask import Blueprint, request, jsonify, abort, render_template  # ← render_template 追加
from flask_login import login_required, current_user
from datetime import datetime, date, timedelta
from dateutil import tz
import logging
from collections import Counter, defaultdict 
from utils.db import get_schedules_with_formatting_all

# 追加インポート
import os
import boto3
from dateutil import parser as dtparser  # ← isoparse 用

from .dynamo import DynamoDB
db = DynamoDB()

analytics = Blueprint("analytics", __name__)
logger = logging.getLogger(__name__)

def _parse_date(s, default=None):
    if not s:
        return default
    return datetime.strptime(s[:10], "%Y-%m-%d").date()

def _group_key(dt: date, group: str):
    if group == "day":
        return dt.strftime("%Y-%m-%d")
    if group == "week":
        iso = dt.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"
    return dt.strftime("%Y-%m")

def _event_start_hour(sched: dict) -> int | None:
    """スケジュールの start_time ('HH:MM') から hour を返す"""
    st = (sched.get("start_time") or "").strip()
    if ":" in st:
        try:
            h = int(st.split(":")[0])
            return h if 0 <= h <= 23 else None
        except Exception:
            return None
    return None

def _hour_from_event_start(schedule_id: str, target_tz="Asia/Tokyo"):
    """
    bad_schedules から start_time('HH:MM') を取り出して hour を返す。
    schedule_id が MANUALPOINT のように実スケジュールでない場合は None を返す。
    """
    if not schedule_id or schedule_id.startswith("MANUALPOINT#"):
        return None

    table_name = os.getenv("DYNAMODB_TABLE_NAME", "bad_schedules")
    region = os.getenv("AWS_REGION", "ap-northeast-1")
    ddb = boto3.resource("dynamodb", region_name=region)
    tbl = ddb.Table(table_name)

    try:
        resp = tbl.get_item(Key={"schedule_id": schedule_id})
        item = resp.get("Item") or {}
    except Exception:
        return None

    start_time = (item.get("start_time") or "").strip()  # 例: "19:00"
    if not start_time or ":" not in start_time:
        return None

    try:
        hh = int(start_time.split(":")[0])
        if 0 <= hh <= 23:
            return hh
    except Exception:
        pass
    return None

def _to_hour_with_debug(reg, target_tz="Asia/Tokyo"):
    """
    registered_at を指定タイムゾーンの時刻に変換して hour を返す
    （isoparse で Z / +09:00 を厳密解釈）
    """
    debug_info = {"original": str(reg), "type": type(reg).__name__}
    try:
        if isinstance(reg, datetime):
            dt = reg
        elif isinstance(reg, str):
            dt = dtparser.isoparse(reg.replace(" ", "T"))
            debug_info["parsed"] = True
        else:
            debug_info["success"] = False
            return None, debug_info

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz.UTC)  # UTC とみなす
            debug_info["assumed_utc"] = True

        dt_local = dt.astimezone(tz.gettz(target_tz))
        hour = int(dt_local.hour)
        debug_info["converted_hour"] = hour
        debug_info["success"] = True
        return hour, debug_info

    except Exception as e:
        debug_info["error"] = str(e)
        debug_info["success"] = False
        return None, debug_info

def _to_hour(reg, target_tz="Asia/Tokyo"):
    hour, _ = _to_hour_with_debug(reg, target_tz)
    return hour

def _calc_streaks(sorted_dates: list[date]) -> tuple[int, int]:
    """14日超でストリーク断絶とみなす簡易定義"""
    if not sorted_dates:
        return 0, 0
    cur = best = 1
    for i in range(1, len(sorted_dates)):
        gap = (sorted_dates[i] - sorted_dates[i-1]).days
        if gap <= 14:
            cur += 1
            best = max(best, cur)
        else:
            cur = 1
    return cur, best

@analytics.route("/admin/analytics/retention", methods=["GET"])
@login_required
def analytics_retention():
    if not getattr(current_user, "administrator", False):
        abort(403)

    tzname = request.args.get("tz", "Asia/Tokyo")
    group = (request.args.get("group") or "month").lower()
    if group not in ("month", "week"):
        group = "month"

    today = datetime.now(tz=tz.gettz(tzname)).date()
    default_start = (today.replace(day=1) - timedelta(days=120))  # 過去4ヶ月目頭
    start = _parse_date(request.args.get("start"), default=default_start)
    end   = _parse_date(request.args.get("end"),   default=today)
    if start and start > end:
        start, end = end, start

    # 期間内の全参加（キャンセル除外）
    rows = db.get_all_participations_with_timestamp(start, end) or []

    # userごとの初参加日を特定するために、広い期間での「初回日」も必要ならdb側で取得してもOK。
    # ここでは簡便に、[過去分も含めた初参加日] を別メソッドで取れたと仮定（なければrowsのみで「期間内での初回」を新規と定義）
    # first_map = db.get_users_first_participation_dates()  # {user_id: date} などを想定

    # 集計
    def grpkey(d: date) -> str:
        if group == "week":
            iso = d.isocalendar()
            return f"{iso.year}-W{iso.week:02d}"
        return d.strftime("%Y-%m")

    active_by_group = defaultdict(set)  # グループごとの参加者集合
    for r in rows:
        try:
            st = str(r.get("status", "active")).lower()
            if st.startswith("cancel"):   # 'cancel', 'canceled', 'cancelled' すべて弾く
                continue
            d = datetime.strptime(r["event_date"], "%Y-%m-%d").date()
            if d < start or d > end:
                continue
            g = grpkey(d)
            uid = str(r["user_id"])
            active_by_group[g].add(uid)
        except Exception:
            continue

    # 並び順を一度だけ決めて以降もそれを使用
    groups_sorted = sorted(active_by_group.keys())

    # 期間内「新規/継続」判定（期間内累積で初登場=新規）
    new_vs_returning = {}
    seen_so_far = set()
    for g in groups_sorted:
        users_g = active_by_group[g]
        new_count = sum(1 for u in users_g if u not in seen_so_far)
        returning_count = len(users_g) - new_count
        new_vs_returning[g] = {
            "new": new_count,
            "returning": returning_count,
            "total_active": len(users_g),
        }
        seen_so_far |= users_g

    # Retention（前グループ→当グループ）
    retention = {}
    for i in range(1, len(groups_sorted)):
        prev_g = groups_sorted[i-1]
        cur_g  = groups_sorted[i]
        prev_users = active_by_group[prev_g]
        cur_users  = active_by_group[cur_g]
        if not prev_users:
            x = 0
            rate = None
        else:
            x = len(prev_users & cur_users)
            rate = round(x / len(prev_users), 3)
        retention[cur_g] = {
            "from_prev": x,
            "prev_active": len(prev_users),
            "rate": rate,
        }

    return jsonify({
        "range": {"start": start.strftime("%Y-%m-%d"), "end": end.strftime("%Y-%m-%d")},
        "group": group,
        "actives": {g: len(active_by_group[g]) for g in groups_sorted},
        "new_vs_returning": new_vs_returning,
        "retention": retention,
    })


@analytics.route("/admin/analytics/user/<user_id>/participation", methods=["GET"])
@login_required
def analytics_user_participation(user_id):
    if not getattr(current_user, "administrator", False):
        abort(403)

    tzname = request.args.get("tz", "Asia/Tokyo")
    group = (request.args.get("group") or "month").lower()
    if group not in ("day", "week", "month"):
        group = "month"

    # auto: イベント開始時刻を優先、取れなければ登録時刻で補完
    hour_source = (request.args.get("hour_source") or "auto").lower()
    if hour_source not in ("auto", "event", "registered"):
        hour_source = "auto"

    debug = request.args.get("debug", "").lower() == "true"

    today = datetime.now(tz=tz.gettz(tzname)).date()
    default_start = today - timedelta(days=180)
    start = _parse_date(request.args.get("start"), default=default_start)
    end   = _parse_date(request.args.get("end"),   default=today)
    if start and start > end:
        start, end = end, start

    records = db.get_user_participation_history_with_timestamp(user_id) or []

    dates: list[date] = []
    # 3本用意：イベント開始時刻での分布、登録時刻での分布、最終採用分布
    hours_event     = Counter({f"{h:02d}": 0 for h in range(24)})
    hours_registered= Counter({f"{h:02d}": 0 for h in range(24)})
    hours_final     = Counter({f"{h:02d}": 0 for h in range(24)})

    debug_samples = []

    for idx, r in enumerate(records):
        try:
            ev = r["event_date"]
            d = datetime.strptime(ev, "%Y-%m-%d").date()
            if d < start or d > end:
                continue
            if str(r.get("status", "active")).lower() == "cancelled":
                continue

            dates.append(d)

            # 個別に両方求める
            event_h = _hour_from_event_start(r.get("schedule_id"), tzname)
            reg_h   = None
            ra = r.get("registered_at")
            if debug and idx < 10:
                reg_h, dbg = _to_hour_with_debug(ra, tzname)
                debug_samples.append({"index": idx, "event_date": ev, "registered_at": str(ra), "debug_info": dbg})
            else:
                reg_h = _to_hour(ra, tzname)

            # カウント（存在するものだけ）
            if isinstance(event_h, int) and 0 <= event_h <= 23:
                hours_event[f"{event_h:02d}"] += 1
            if isinstance(reg_h, int) and 0 <= reg_h <= 23:
                hours_registered[f"{reg_h:02d}"] += 1

            # 最終採用（auto/event/registered）
            chosen = None
            if hour_source == "event":
                chosen = event_h
            elif hour_source == "registered":
                chosen = reg_h
            else:  # auto
                chosen = event_h if isinstance(event_h, int) else reg_h

            if isinstance(chosen, int) and 0 <= chosen <= 23:
                hours_final[f"{chosen:02d}"] += 1

        except Exception as e:
            logger.error(f"Error processing record: {e}, record: {r}")
            continue

    # 去重・ソート
    dates = sorted(set(dates))
    total = len(dates)

    # 期間統計
    if total:
        first_date = dates[0].strftime("%Y-%m-%d")
        last_date  = dates[-1].strftime("%Y-%m-%d")
        if total >= 2:
            gaps = [(dates[i] - dates[i-1]).days for i in range(1, total)]
            avg_gap = sum(gaps) / len(gaps)
            max_gap = max(gaps)
        else:
            avg_gap = None
            max_gap = None
    else:
        first_date = last_date = None
        avg_gap = max_gap = None

    # 曜日分布
    youbi = ["月","火","水","木","金","土","日"]
    by_weekday = Counter()
    for d in dates:
        by_weekday[youbi[d.weekday()]] += 1

    # 粒度集計
    by_group = Counter()
    for d in dates:
        by_group[_group_key(d, group)] += 1

    # ストリーク
    current_streak, max_streak = _calc_streaks(dates)

    response_data = {
        "user_id": user_id,
        "range": {
            "start": (start or (dates[0] if dates else None)).strftime("%Y-%m-%d") if (start or dates) else None,
            "end": end.strftime("%Y-%m-%d") if end else None
        },
        "totals": {
            "participations": total,
            "first_date": first_date,
            "last_date": last_date,
            "avg_interval_days": round(avg_gap, 2) if avg_gap is not None else None,
            "max_gap_days": int(max_gap) if max_gap is not None else None
        },
        "distributions": {
            # 採用（フロントは従来通り by_hour を読むだけでOK）
            "by_hour": dict(hours_final),
            # 参考: どちらで集計しても見たい時用
            "by_hour_event": dict(hours_event),
            "by_hour_registered": dict(hours_registered),
            "by_weekday": dict(by_weekday),
            "by_group": [{"group": k, "count": v} for k, v in sorted(by_group.items())]
        },
        "streaks": {
            "current_streak": current_streak,
            "max_streak": max_streak
        },
        "dates": [d.strftime("%Y-%m-%d") for d in dates],
        "meta": {
            "hour_source": hour_source  # auto / event / registered
        }
    }

    if debug:
        response_data["debug"] = {"samples": debug_samples, "total_records": len(records)}

    return jsonify(response_data)

@analytics.route("/admin/analytics/overall", methods=["GET"])
@login_required
def analytics_overall():
    # 管理者のみ
    if not getattr(current_user, "administrator", False):
        abort(403)

    group = (request.args.get("group") or "month").lower()
    if group not in ("day", "week", "month"):
        group = "month"

    # 期間（デフォルト：直近180日）
    today = date.today()
    default_start = today - timedelta(days=180)
    start = _parse_date(request.args.get("start"), default=default_start)
    end   = _parse_date(request.args.get("end"),   default=today)
    if start and start > end:
        start, end = end, start

    # すべてのアクティブ・スケジュール
    schedules = get_schedules_with_formatting_all() or []

    # まず全期間（end まで）の「初参加日」を作る（新規/リピーター判定用）
    first_seen: dict[str, date] = {}
    for s in schedules:
        if s.get("status", "active") != "active":
            continue
        try:
            d = datetime.strptime(s["date"], "%Y-%m-%d").date()
        except Exception:
            continue
        if d > end:
            continue
        # 参加者リスト（通常参加 + タラ参加もあれば加算）
        uids = list(s.get("participants", [])) + list(s.get("tara_participants", []))
        for uid in uids:
            fd = first_seen.get(uid)
            if fd is None or d < fd:
                first_seen[uid] = d

    # 集計用
    total_participations = 0          # 参加延べ数
    unique_users: set[str] = set()    # 一意ユーザ
    events_count = 0                  # 対象期間のイベント数
    by_weekday = Counter()            # 曜日別（延べ）
    by_group = Counter()              # 月/週/日別（延べ）
    by_hour  = Counter({f"{h:02d}": 0 for h in range(24)})  # 開始時刻ベース
    participation_dates = Counter()   # 日別の延べ参加数（ヒートマップ等で使える）

    new_users_in_range = set()        # 期間中に初参加（=新規）
    returning_users_in_range = set()  # 期間中に参加し、初参加日は期間より前（=リピーター）

    # 期間内だけを本集計
    for s in schedules:
        if s.get("status", "active") != "active":
            continue
        try:
            d = datetime.strptime(s["date"], "%Y-%m-%d").date()
        except Exception:
            continue
        if d < start or d > end:
            continue

        events_count += 1

        # 参加者
        uids = list(s.get("participants", [])) + list(s.get("tara_participants", []))
        count_here = len(uids)
        total_participations += count_here
        participation_dates[d] += count_here

        # 一意ユーザ
        for uid in uids:
            unique_users.add(uid)
            # 新規 / リピーター
            fd = first_seen.get(uid)
            if fd is not None:
                if start <= fd <= end:
                    new_users_in_range.add(uid)
                elif fd < start:
                    returning_users_in_range.add(uid)

        # 曜日別（延べ）
        youbi = ["月","火","水","木","金","土","日"]
        by_weekday[youbi[d.weekday()]] += count_here

        # 粒度別（延べ）
        by_group[_group_key(d, group)] += count_here

        # 開始時刻→時間帯（延べ）
        hh = _event_start_hour(s)
        if hh is not None:
            by_hour[f"{hh:02d}"] += count_here

    # KPI
    unique_users_count = len(unique_users)
    new_users_count = len(new_users_in_range)
    returning_users_count = len(returning_users_in_range)

    # 返却
    return jsonify({
        "range": {
            "start": start.strftime("%Y-%m-%d") if start else None,
            "end":   end.strftime("%Y-%m-%d") if end else None,
            "events_count": events_count
        },
        "totals": {
            "participations": total_participations,         # 延べ参加数
            "unique_users": unique_users_count,             # 一意参加者数
            "avg_participants_per_event": (
                round(total_participations / events_count, 2) if events_count else None
            )
        },
        "cohorts": {
            "new_users_in_range": new_users_count,          # 期間中に初参加
            "returning_users_in_range": returning_users_count
        },
        "distributions": {
            "by_weekday": dict(by_weekday),                 # 曜日別（延べ）
            "by_hour": dict(by_hour),                       # 開始時刻ベース
            "by_group": [{"group": k, "count": v} for k, v in sorted(by_group.items())],
            "by_date": [{"date": d.strftime("%Y-%m-%d"), "count": c}
                        for d, c in sorted(participation_dates.items())]
        }
    })

# ★ ルート装飾子を付与して公開
@analytics.route("/analytics/<user_id>", methods=["GET"])
@login_required
def analytics_dashboard(user_id):
    if not getattr(current_user, "administrator", False):
        abort(403)
    return render_template("uguu/analytics_dashboard.html", user_id=user_id)
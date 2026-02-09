from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Set
from utils.timezone import JST

@dataclass(frozen=True)
class ParticipationRecord:
    event_date: datetime
    registered_at: datetime
    status: str

@dataclass(frozen=True)
class PointRules:
    reset_days: int = 50
    first_participation_points: int = 200
    streak_per_participation_after_2: int = 50
    # ※ ここにルールを集約しておくと仕様変更が楽

def normalize_participation_history(raw_history: List[Dict[str, Any]]) -> List[ParticipationRecord]:
    records: List[ParticipationRecord] = []
    for r in raw_history:
        status = (r.get("status") or "registered").lower()
        if status == "cancelled":
            continue
        try:
            event_date = datetime.strptime(r["event_date"], "%Y-%m-%d")
            registered_at = datetime.strptime(r["registered_at"], "%Y-%m-%d %H:%M:%S")
        except (KeyError, ValueError):
            continue
        records.append(ParticipationRecord(event_date=event_date, registered_at=registered_at, status=status))
    records.sort(key=lambda x: x.event_date)
    return records

def calc_reset_index(records: List[ParticipationRecord], reset_days: int) -> Tuple[int, bool]:
    """
    参加記録をチェックして、50日ルールでリセットされているか判定
    - 参加記録間の間隔が50日超 → その地点でリセット（ポイント計算範囲を制限）
    - 最後の参加日から現在まで50日超 → 現在リセット中（is_reset=True）
    """
    from datetime import datetime
    
    last_reset_index = 0
    
    # 参加記録間の間隔をチェック（ポイント計算範囲の制限のみ）
    for i in range(1, len(records)):
        diff = (records[i].event_date - records[i-1].event_date).days
        if diff > reset_days:
            last_reset_index = i
    
    # ★最後の参加日から現在までの日数だけでis_resetを判定
    is_reset = False
    if records:
        now = datetime.now()
        days_since_last = (now - records[-1].event_date).days
        if days_since_last > reset_days:
            is_reset = True
    
    return last_reset_index, is_reset

def slice_records_for_points(records: List[ParticipationRecord], last_reset_index: int) -> List[ParticipationRecord]:
    return records[last_reset_index:] if last_reset_index > 0 else records

def build_participated_date_set(records_for_points: List[ParticipationRecord]) -> Set[str]:
    return {r.event_date.strftime("%Y-%m-%d") for r in records_for_points}

def calc_registration_counts(records: List[ParticipationRecord], is_early_registration_fn) -> Tuple[int, int]:
    early = 0
    direct = 0
    for r in records:
        base = is_early_registration_fn({"event_date": r.event_date, "registered_at": r.registered_at})
        if base == 100:
            early += 1
        elif base == 50:
            direct += 1
    return early, direct

def calc_participation_and_cumulative(
    records_all: List[ParticipationRecord],
    records_for_points: List[ParticipationRecord],
    rules: PointRules,
    point_multiplier: float,
    is_early_registration_fn,
) -> Dict[str, Any]:
    participation_points = 0
    cumulative_count = 0
    cumulative_bonus_points = 0
    early_count = 0
    direct_count = 0

    if not records_all:
        return {
            "participation_points": 0,
            "cumulative_count": 0,
            "cumulative_bonus_points": 0,
            "early_registration_count": 0,
            "direct_registration_count": 0,
        }

    first_ever_date = records_all[0].event_date

    for i, rec in enumerate(records_for_points):
        base = is_early_registration_fn({"event_date": rec.event_date, "registered_at": rec.registered_at})
        
        if base not in (100, 50):
            base = 10
        
        if base == 100:
            early_count += 1
        elif base == 50:
            direct_count += 1

        # ★全履歴の初回 OR カムバック参加（records_for_pointsの最初）
        if rec.event_date == first_ever_date or i == 0:
            pts = int(rules.first_participation_points * point_multiplier)
        else:
            pts = int(base * point_multiplier)
        participation_points += pts

        cumulative_count += 1
        if cumulative_count % 5 == 0:
            # ここは「500にしたい」なら 500 に変更し rules 化推奨
            bonus = int(500 * point_multiplier)
            cumulative_bonus_points += bonus

    return {
        "participation_points": participation_points,
        "cumulative_count": cumulative_count,
        "cumulative_bonus_points": cumulative_bonus_points,
        "early_registration_count": early_count,
        "direct_registration_count": direct_count,
    }


def calc_monthly_bonus(records_for_points: List[ParticipationRecord], point_multiplier: float) -> Tuple[int, Dict[str, Any]]:
    monthly_participation = defaultdict(int)
    for r in records_for_points:
        monthly_participation[r.event_date.strftime("%Y-%m")] += 1
    
    print("[DBG monthly counts]", dict(sorted(monthly_participation.items())))

    monthly_bonus_points = 0
    monthly_bonuses: Dict[str, Any] = {}

    for month, count in sorted(monthly_participation.items()):
        base_bonus = 0 if count < 4 else 500 + (count - 4) * 200
        bonus = int(base_bonus * point_multiplier)

        monthly_bonuses[month] = {"participation_count": count, "bonus_points": bonus}
        monthly_bonus_points += bonus

    return monthly_bonus_points, monthly_bonuses

def calc_days_until_reset(last_participation_dt: datetime, reset_days: int, now_dt: Optional[datetime] = None) -> int:
    now_dt = now_dt or datetime.now()
    days_since = (now_dt - last_participation_dt).days
    return reset_days - days_since

EXCLUDE_ACTIONS = {"admin_manual_point"}  # 参加に混ぜないもの
TARA_ACTION = "tara_join"

def classify(rec) -> str:
    """
    日付確定用の分類
    - official: 正規参加（参加ボタン）
    - tara    : たら
    - cancelled: キャンセル
    - other   : 参加に混ぜない（例: 手動ポイントなど）
    """
    status = (rec.get("status") or "registered").lower()
    action = rec.get("action")

    if status == "cancelled":
        return "cancelled"

    if action in EXCLUDE_ACTIONS:
        return "other"

    if action == TARA_ACTION:
        return "tara"

    # それ以外（action無し or join系など）は「正規参加」とみなす
    return "official"

PRIORITY = {
    "official": 3,
    "tara": 2,
    "cancelled": 1,
    "other": 0,
}


def calc_streak_points(
    records_for_points: List[ParticipationRecord],
    all_schedules: List[Dict[str, Any]],
    rules: PointRules,
    point_multiplier: float,
) -> Tuple[int, int, int, str]:
    """
    連続参加ボーナス計算
    Returns:
        (streak_points, current_streak, max_streak, streak_start_date)
    """
    import os

    DEBUG = os.getenv("POINT_LOG", "1") == "1"               # サマリー/重要イベント
    DEBUG_DETAIL = os.getenv("POINT_LOG_DETAIL", "0") == "1" # 逐次ログ（重い）

    def dbg(msg: str):
        if DEBUG:
            print(msg)

    def dbg_detail(msg: str):
        if DEBUG_DETAIL:
            print(msg)

    # 参加日のセット
    user_participated_dates = {r.event_date.strftime("%Y-%m-%d") for r in records_for_points}

    # サマリーは最初に1回だけ
    dbg(f"[STREAK] schedules={len(all_schedules)} participated={len(user_participated_dates)}")

    streak_points = 0
    current_streak = 0
    max_streak = 0
    streak_start = None

    # マイルストーン管理
    milestone_values = {5: 500, 10: 1000, 15: 1500, 20: 2000, 25: 2500}
    milestones = {k: False for k in milestone_values.keys()}

    resets = 0
    reset_events = []   # DETAIL用（多いので通常表示しない）
    bonus_events = []   # DETAIL用（必要なら）

    for schedule in all_schedules:
        schedule_date = schedule["date"]
        is_participated = schedule_date in user_participated_dates

        if is_participated:
            current_streak += 1
            if streak_start is None:
                streak_start = schedule_date

            # 連続2回目以降は毎回50P
            if current_streak >= 2:
                sp = int(rules.streak_per_participation_after_2 * point_multiplier)
                streak_points += sp
                # ← これが大量ログの原因：DETAIL の時だけ
                dbg_detail(f"[STREAK][step] {schedule_date} +{sp} (streak={current_streak})")

            # マイルストーンボーナス（ここは残してOK）
            if current_streak in milestone_values and not milestones[current_streak]:
                bonus = int(milestone_values[current_streak] * point_multiplier)
                streak_points += bonus
                milestones[current_streak] = True

                # 通常ログでも「達成」は出してOK（分析に効く）
                dbg(f"[STREAK][bonus] date={schedule_date} streak={current_streak} bonus=+{bonus}")

                if DEBUG_DETAIL:
                    bonus_events.append((schedule_date, current_streak, bonus))

            max_streak = max(max_streak, current_streak)

        else:
            # 欠席 → リセット（ここも残してOK。ただし多ければDETAIL化しても良い）
            if current_streak > 0:
                resets += 1
                # resetは分析に効くので通常は残す（静かにしたければ dbg_detail に落とす）
                dbg(f"[STREAK][reset] date={schedule_date} streak_was={current_streak}")
                if DEBUG_DETAIL:
                    reset_events.append((schedule_date, current_streak))

            current_streak = 0
            streak_start = None
            milestones = {k: False for k in milestone_values.keys()}

    # 最後に合計を1回だけ
    dbg(f"[STREAK][sum] points={streak_points} current_streak={current_streak} max_streak={max_streak} resets={resets}")

    # DETAIL時だけサンプル表示（必要なら）
    if DEBUG_DETAIL:
        dbg_detail(f"[STREAK][detail] reset_events(sample up to 10)={reset_events[:10]}")
        dbg_detail(f"[STREAK][detail] bonus_events(sample up to 10)={bonus_events[:10]}")

    return streak_points, current_streak, max_streak, streak_start or ""


def better(a, b) -> bool:
    """a を採用すべきなら True（b より良い）"""
    ca, cb = classify(a), classify(b)

    # 優先順位が高い方を採用
    if PRIORITY[ca] != PRIORITY[cb]:
        return PRIORITY[ca] > PRIORITY[cb]

    # 同分類なら、登録時刻が遅い方（registered_at）を採用
    ta = a.get("_registered_at")  # datetime を入れておく想定
    tb = b.get("_registered_at")
    if ta and tb:
        return ta > tb

    # 片方しか時刻が無い場合は、時刻がある方を優先
    if ta and not tb:
        return True
    if tb and not ta:
        return False

    # どちらも無ければ現状維持
    return False

def _is_junior_high_student(self, user_info):
        """
        生年月日から中学生以下かどうかを判定
        日本の学年は4月1日基準
        生年月日がない場合は対象外とみなす
        """
        # ユーザー情報がない、または生年月日がない場合は対象外
        if not user_info or not user_info.get('birth_date'):
            print(f"[DEBUG] 生年月日なし → ポイント半減対象外として扱う")
            return False
        
        try:
            from datetime import datetime
            
            birth_date = user_info['birth_date']
            
            # 文字列の場合はdatetimeに変換
            if isinstance(birth_date, str):
                birth_date = datetime.strptime(birth_date, '%Y-%m-%d')
            
            today = datetime.now(JST).date()
            
            # 年齢を計算
            age = today.year - birth_date.year
            
            # 誕生日前なら-1
            if (today.month, today.day) < (birth_date.month, birth_date.day):
                age -= 1
            
            # 学年を計算（4月1日基準）
            # 4月1日以前なら、前の学年
            if today.month < 4 or (today.month == 4 and today.day == 1):
                school_year_age = age - 1
            else:
                school_year_age = age
            
            # 中学生以下は14歳以下
            # 小学生：6～11歳、中学生：12～14歳
            is_junior_high_or_below = school_year_age <= 14
            
            grade_info = ""
            if 6 <= school_year_age <= 11:
                grade_info = f"(小学{school_year_age - 5}年相当)"
            elif 12 <= school_year_age <= 14:
                grade_info = f"(中学{school_year_age - 11}年相当)"
            elif school_year_age < 6:
                grade_info = "(未就学)"
            
            print(f"[DEBUG] 生年月日: {birth_date.strftime('%Y-%m-%d')}, 年齢: {age}歳, 学年年齢: {school_year_age}歳{grade_info}, 中学生以下: {is_junior_high_or_below}")
            
            return is_junior_high_or_below
            
        except Exception as e:
            print(f"[WARN] 中学生以下判定エラー: {str(e)} → ポイント半減対象外として扱う")
            return False


def _is_junior_high_or_below(user_info):  # ← selfを削除
    """
    生年月日から中学生以下（小学生＋中学生）かどうかを判定
    日本の学年は4月1日基準
    生年月日がない場合は対象外とみなす
    """
    if not user_info or not user_info.get('birth_date'):
        print(f"[DEBUG] 生年月日なし → ポイント半減対象外として扱う")
        return False
    
    try:
        birth_date = user_info['birth_date']
        
        if isinstance(birth_date, str):
            birth_date = datetime.strptime(birth_date, '%Y-%m-%d')
        
        today = datetime.now(JST).date()
        
        # 現在の年度を計算（4月1日は前年度扱い）
        if today.month < 4 or (today.month == 4 and today.day == 1):
            current_fiscal_year = today.year - 1
        else:
            current_fiscal_year = today.year
        
        # 生まれた年度を計算
        if birth_date.month < 4 or (birth_date.month == 4 and birth_date.day == 1):
            birth_fiscal_year = birth_date.year - 1
        else:
            birth_fiscal_year = birth_date.year
        
        # 学年を計算（小1=0, 中3=8, 高1=9）
        school_grade = current_fiscal_year - birth_fiscal_year - 6
        
        # 中学生以下は学年0〜8
        is_target = 0 <= school_grade <= 8
        
        age = today.year - birth_date.year
        if (today.month, today.day) < (birth_date.month, birth_date.day):
            age -= 1
        
        print(f"[DEBUG] 生年月日: {birth_date.strftime('%Y-%m-%d')}, 年齢: {age}歳, 中学生以下: {is_target}")
        
        return is_target
        
    except Exception as e:
        print(f"[WARN] 中学生以下判定エラー: {str(e)} → ポイント半減対象外として扱う")
        return False
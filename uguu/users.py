from flask import Blueprint, render_template, flash, redirect, url_for, jsonify, request, current_app
from flask_login import login_required, current_user
from datetime import datetime
import os
import boto3
from typing import Any, cast
from game.game_utils import normalize_user_pk

users = Blueprint('users', __name__)

from .dynamo import db
from utils.points import record_earn
from uguu.point import get_current_points_hybrid

print("[DEBUG] AWS_REGION =", os.getenv("AWS_REGION"))
print("[DEBUG] DYNAMO_UGU_POINTS_TABLE =", os.getenv("DYNAMO_UGU_POINTS_TABLE", "ugu_points"))

# ❸ 定数
UGU_POINTS_TABLE = os.getenv("DYNAMO_UGU_POINTS_TABLE", "ugu_points")
UGU_PARTICIPATION_TABLE = "bad-users-history"


def _parse_ymd10(s):
    """
    '2026-03-04' のような10文字の文字列を datetime オブジェクトに変換する
    """
    if not s or not isinstance(s, str):
        return None
    try:
        # 最初の10文字（YYYY-MM-DD）を抽出してパース
        return datetime.strptime(s[:10], '%Y-%m-%d')
    except (ValueError, TypeError):
        return None

# ❹ ルート定義（ここから下で @users.route を使う）
@users.route("/admin/users/<user_id>/add-point", methods=["POST"])
@login_required
def add_point(user_id):
    if not getattr(current_user, "administrator", False):
        flash("権限がありません", "danger")
        return redirect(url_for("users.user_profile", user_id=user_id))

    try:
        points = int(request.form.get("points", "0") or 0)
    except ValueError:
        points = 0

    reason = (request.form.get("reason") or "管理人付与").strip()
    effective_at_str = (request.form.get("effective_at") or "").strip()

    if effective_at_str:
        try:
            dt = datetime.strptime(effective_at_str, "%Y-%m-%d %H:%M")
        except ValueError:
            dt = datetime.strptime(effective_at_str, "%Y-%m-%dT%H:%M")
    else:
        dt = datetime.utcnow()

    event_date = dt.strftime("%Y-%m-%d")

    dynamodb = cast(Any, boto3.resource("dynamodb", region_name=os.getenv("AWS_REGION")))
    history_table = dynamodb.Table(UGU_PARTICIPATION_TABLE)

    try:
        record_earn(
            history_table,
            user_id=user_id,
            points=points,
            event_date=event_date,
            reason=reason,
            source="admin_manual",
            created_by=getattr(current_user, "email", "admin"),
        )
        flash("ポイントを付与しました。", "success")
    except Exception:
        current_app.logger.exception("ポイント付与の保存に失敗")
        flash("ポイント付与の保存に失敗しました。", "danger")

    return redirect(url_for("users.user_profile", user_id=user_id))


@users.route('/user/<user_id>')
def user_profile(user_id):
    try:
        # 1. ユーザー基本情報取得
        user = db.get_user_by_id(user_id)
        if not user:
            flash('ユーザーが見つかりませんでした。', 'error')
            return redirect(url_for('index'))

        # 2. 投稿取得
        user_posts, _next_cursor = db.get_posts_by_user(user_id)
        user_posts = sorted(user_posts, key=lambda x: x.get('created_at', ''), reverse=True) if user_posts else []

        # 3. 参加履歴・ポイント履歴取得
        raw_history = db.get_user_participation_history_with_timestamp(user_id) or []
        point_spends = db.list_point_spends(user_id, limit=1000) or []

        # 4. 統計計算（旧ルール計算結果を取得）
        user_stats = db.get_user_stats(user_id, raw_history=raw_history, spends=point_spends) or {}

        # 5. 支出計算
        def _pick_spend_amount(s: dict) -> int:
            for key in ["points_used", "delta_points", "amount"]:
                v = s.get(key)
                if v is not None:
                    try: return abs(int(v))
                    except: pass
            return 0
        point_total_spent_recent = sum(_pick_spend_amount(s) for s in point_spends)

        # 6. 参加判定ヘルパー
        def _is_registered(rec: dict) -> bool:
            st = (rec.get("status") or "").lower().strip()
            if not st:
                act = (rec.get("action") or "").lower().strip()
                if act in ("tara_join", "join", "register", "registered"): st = "registered"
                elif act in ("tara_cancel", "cancel", "cancelled", "canceled"): st = "cancelled"
            return st == "registered"

        # 7. 参加回数カウント
        official_count = sum(1 for r in raw_history if isinstance(r, dict) and _is_registered(r))

        # ==========================================================
        # ★ 厳格な60日ルール適用（バイパスモード + 自動没収）
        # ==========================================================
        from datetime import datetime, date

        # 全履歴から計算された合計ポイント（暫定値）
        calculated_total = int(user_stats.get('uguu_points', 0))

        # 最後に「参加(registered)」した日を特定
        last_participation_date = None
        sorted_history = sorted(raw_history, key=lambda x: str(x.get('date') or x.get('event_date') or ''), reverse=True)
        
        for rec in sorted_history:
            if _is_registered(rec):
                last_participation_date = rec.get('date') or rec.get('event_date') or rec.get('eventDay')
                break

        # 没収判定
        is_expired = False
        days_since_last = 0
        if last_participation_date:
            try:
                last_dt = datetime.strptime(str(last_participation_date)[:10], '%Y-%m-%d').date()
                days_since_last = (date.today() - last_dt).days
                if days_since_last > 60:
                    is_expired = True
            except Exception as e:
                print(f"[WARN] Failed to parse last_participation_date: {e}")
        else:
            # 一度も参加したことがない場合は日数計算不能
            days_since_last = 999 

        # ポイントの最終確定
        if is_expired:
            participation_points = 0
            print(f"[EXPIRED] user={user_id} Last={last_participation_date} Days={days_since_last} -> 0P")
        else:
            participation_points = calculated_total
            print(f"[ACTIVE] user={user_id} Last={last_participation_date} Days={days_since_last} -> {participation_points}P")

        # テンプレートに渡すポイント情報
        points_info = {
            "current_points": participation_points,
            "base_current_points": 0,
            "snapshot_date": None,
            "is_expired": is_expired,
            "days_since_last": days_since_last
        }

        # 8. 管理者用・スキルスコア・参加日リスト
        is_admin = bool(getattr(current_user, "administrator", False))
        bad_users_table = current_app.dynamodb.Table("bad-users")
        resp = bad_users_table.get_item(Key={"user#user_id": user_id}, ConsistentRead=True)
        bad_user = resp.get("Item") or {}
        
        def _to_float_or_none(v):
            try: return float(v) if v is not None else None
            except: return None
        skill_score = _to_float_or_none(bad_user.get("skill_score"))

        admin_participation_dates = []
        if is_admin:
            try:
                youbi = ['月', '火', '水', '木', '金', '土', '日']
                formatted = []
                from uguu.users import _parse_ymd10 as _parse_ymd10
                for r in raw_history:
                    if not isinstance(r, dict) or not _is_registered(r): continue
                    date_raw = r.get("date") or r.get("event_date") or r.get("eventDay")
                    dt = datetime.strptime(str(date_raw)[:10], '%Y-%m-%d') if date_raw else None
                    if dt:
                        formatted.append(f"{dt.strftime('%Y年%m月%d日')}（{youbi[dt.weekday()]}）")
                admin_participation_dates = sorted(list(set(formatted)))
            except Exception as e:
                print(f"[WARN] failed to build admin_participation_dates: {e}")

        plain_user_id = user.get('user#user_id', '').replace('user#', '')

        return render_template(
            'uguu/users.html',
            user=user,
            plain_user_id=plain_user_id,
            posts=user_posts,
            posts_count=len(user_posts) if user_posts else 0,
            past_participation_count=int(official_count),
            participation_points=participation_points,
            followers_count=0,
            following_count=0,
            is_admin=is_admin,
            admin_participation_dates=admin_participation_dates,
            days_until_reset=60 - days_since_last if not is_expired else 0, # 残り日数表示用
            upcoming_schedules=db.get_upcoming_schedules(),
            point_spends=point_spends,
            point_total_spent_recent=point_total_spent_recent,
            skill_score=skill_score,
            points_info=points_info,
        )

    except Exception as e:
        print(f"[ERROR] Error in user_profile: {str(e)}")
        import traceback
        traceback.print_exc()
        flash('プロフィールの読み込み中にエラーが発生しました。', 'error')
        return redirect(url_for('index'))
    
    
@users.route('/point-participation', methods=['POST'])
@login_required
def point_participation():
    """ポイント支払い処理"""
    try:
        # ---- ここからトレース（必ず残すと原因切り分けが速い）----
        print("[TRACE] /point-participation hit")
        data = request.get_json(silent=True) or {}
        print(f"[TRACE] payload={data}")
        user_id = str(data.get('user_id') or "")
        event_date = data.get('event_date')
        points_cost = int(data.get('points_cost') or 600)
        print(f"[TRACE] current_user.id={current_user.id} / user_id={user_id} / event_date={event_date} / cost={points_cost}")

        # 本人確認（型ずれ防止のため文字列比較）
        if str(current_user.id) != user_id:
            print("[TRACE] blocked: not owner")
            return jsonify({'error': '本人のみ実行できます'}), 403        

        # event_date の形式チェック（YYYY-MM-DDに正規化）
        if not event_date:
            return jsonify({'error': 'イベント日が指定されていません'}), 400
        try:
            # 余計な時刻が来ても日付だけへ丸める
            event_date = str(event_date)[:10]
            datetime.strptime(event_date, "%Y-%m-%d")
        except Exception:
            return jsonify({'error': 'イベント日の形式が不正です（YYYY-MM-DD）'}), 400

        # 現在ポイント
        stats = db.get_user_stats(user_id) or {}
        current_points = int(stats.get('uguu_points', 0))
        print(f"[TRACE] current_points={current_points}")

        # 残高チェック
        if current_points < points_cost:
            return jsonify({'error': f'ポイント不足（現在: {current_points}P / 必要: {points_cost}P）'}), 400

        # 支払い記録（bad-users-history に points#spend#... を作成）
        ok = db.record_payment(
            user_id=user_id,
            event_date=event_date,
            points_used=points_cost
        )
        print(f"[TRACE] record_payment -> {ok}")

        if ok:
            # ここで ledger から再計算して返したい場合は db.calc_total_points_spent を呼ぶ
            return jsonify({
                'message': 'ポイント支払いが完了しました',
                'remaining_points': current_points - points_cost
            }), 200

        return jsonify({'error': 'ポイント支払いに失敗しました'}), 500

    except Exception as e:
        print(f"[ERROR] ポイント支払いエラー: {e}")
        import traceback; traceback.print_exc()
        return jsonify({'error': '内部エラー'}), 500
    

    

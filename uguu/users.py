from flask import Blueprint, render_template, flash, redirect, url_for, jsonify, request, current_app
from flask_login import login_required, current_user
from datetime import datetime
import os
import boto3
from typing import Any, cast

# ❶ Blueprint は一番最初に作る（route より前）
users = Blueprint('users', __name__)

# ❷ 以降に他の import（循環回避のためなるべく上に集約しすぎない）
from .dynamo import db
from utils.points import record_earn

print("[DEBUG] AWS_REGION =", os.getenv("AWS_REGION"))
print("[DEBUG] DYNAMO_UGU_POINTS_TABLE =", os.getenv("DYNAMO_UGU_POINTS_TABLE", "ugu_points"))

# ❸ 定数
UGU_POINTS_TABLE = os.getenv("DYNAMO_UGU_POINTS_TABLE", "ugu_points")
UGU_PARTICIPATION_TABLE = "bad-users-history"

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
        print(f"[DEBUG] Start loading profile for user_id: {user_id}")

        # ユーザー情報
        user = db.get_user_by_id(user_id)
        if not user:
            flash('ユーザーが見つかりませんでした。', 'error')
            return redirect(url_for('index'))

        # 投稿
        user_posts, _next_cursor = db.get_posts_by_user(user_id)
        if user_posts is None:  # ← None チェックを追加
            user_posts = []
        if user_posts:
            user_posts = sorted(user_posts, key=lambda x: x.get('created_at', ''), reverse=True)

        # 統計（ポイント/参加回数）
        user_stats = db.get_user_stats(user_id)

        point_spends = db.list_point_spends(user_id, limit=20)  # 新規に実装したメソッド
        point_total_spent_recent = sum(s.get("amount", 0) for s in point_spends)

        # 今後の予定を取得（ポイント参加用）
        upcoming_schedules = db.get_upcoming_schedules()
        print(f"[DEBUG] upcoming_schedules: {upcoming_schedules}")
        print(f"[DEBUG] upcoming_schedules count: {len(upcoming_schedules) if upcoming_schedules else 0}")

        # 管理者用：参加日一覧（「YYYY年MM月DD日（曜）」で整形してテンプレに渡す）
        is_admin = bool(getattr(current_user, "administrator", False))
        admin_participation_dates = []
        if is_admin:
            try:
                raw_dates = db.get_user_participation_history(user_id)
                youbi = ['月', '火', '水', '木', '金', '土', '日']
                formatted = []
                for d in raw_dates:
                    if isinstance(d, datetime):
                        dt = d
                    else:
                        s = str(d)[:10]
                        dt = datetime.strptime(s, "%Y-%m-%d")
                    formatted.append(f"{dt.strftime('%Y年%m月%d日')}（{youbi[dt.weekday()]}）")
                admin_participation_dates = sorted(formatted)
                print(f"[DEBUG] admin_participation_dates(formatted): {admin_participation_dates}")
            except Exception as e:
                print(f"[WARN] failed to format participation dates: {e}")
                admin_participation_dates = []

        return render_template(
            'uguu/users.html',
            user=user,
            posts=user_posts,
            posts_count=len(user_posts) if user_posts else 0,
            past_participation_count=int(user.get("practice_count") or 0),
            participation_points=user_stats['uguu_points'],  # ←「うぐポイント」残高を表示するならこのままでOK
            followers_count=0,
            following_count=0,
            is_admin=is_admin,
            admin_participation_dates=admin_participation_dates,
            days_until_reset=user_stats.get('days_until_reset'),
            upcoming_schedules=upcoming_schedules,

            # ▼ 追加（テンプレで表示用）
            point_spends=point_spends,
            point_total_spent_recent=point_total_spent_recent,
        )

    except Exception as e:
        print(f"[ERROR] Error in user_profile: {str(e)}")
        import traceback
        traceback.print_exc()  # ← 詳細なスタックトレースを表示
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
    

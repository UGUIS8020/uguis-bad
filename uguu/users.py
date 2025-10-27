from flask import Blueprint, render_template, flash, redirect, url_for
from flask_login import login_required, current_user
from .dynamo import db
from datetime import datetime, date

# Blueprintの作成
users = Blueprint('users', __name__)

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
        user_posts = db.get_posts_by_user(user_id)
        if user_posts:
            user_posts = sorted(user_posts, key=lambda x: x.get('created_at', ''), reverse=True)

        # 統計（ポイント/参加回数）
        user_stats = db.get_user_stats(user_id)

        # 管理者用：参加日一覧（「YYYY年MM月DD日（曜）」で整形してテンプレに渡す）
        is_admin = bool(getattr(current_user, "administrator", False))
        admin_participation_dates = []
        if is_admin:
            try:
                raw_dates = db.get_user_participation_history(user_id)  # datetime の配列 or 'YYYY-MM-DD' の配列
                youbi = ['月', '火', '水', '木', '金', '土', '日']
                formatted = []
                for d in raw_dates:
                    if isinstance(d, datetime):
                        dt = d
                    else:
                        # 文字列や date が来ても安全にパース
                        s = str(d)[:10]
                        dt = datetime.strptime(s, "%Y-%m-%d")
                    formatted.append(f"{dt.strftime('%Y年%m月%d日')}（{youbi[dt.weekday()]}）")
                # 昇順整列
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
            past_participation_count=user_stats['total_participation'],
            participation_points=user_stats['uguu_points'],
            followers_count=0,
            following_count=0,
            is_admin=is_admin,
            admin_participation_dates=admin_participation_dates
        )

    except Exception as e:
        print(f"[ERROR] Error in user_profile: {str(e)}")
        flash('プロフィールの読み込み中にエラーが発生しました。', 'error')
        return redirect(url_for('index'))
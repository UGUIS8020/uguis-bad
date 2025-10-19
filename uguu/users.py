from flask import Blueprint, render_template, flash, redirect, url_for
from flask_login import login_required, current_user
from .dynamo import db

# Blueprintの作成
users = Blueprint('users', __name__)

@users.route('/user/<user_id>')
def user_profile(user_id):
    try:
        print(f"[DEBUG] Start loading profile for user_id: {user_id}")
        
        # ユーザー情報の取得
        user = db.get_user_by_id(user_id)
        print(f"[DEBUG] Retrieved user data: {user}")
        
        if not user:
            print(f"[DEBUG] No user found for user_id: {user_id}")
            flash('ユーザーが見つかりませんでした。', 'error')
            return redirect(url_for('index'))
            
        # ユーザーの投稿を取得
        user_posts = db.get_posts_by_user(user_id)
        print(f"[DEBUG] Retrieved {len(user_posts) if user_posts else 0} posts")
        
        # 統計情報を計算
        posts_count = len(user_posts) if user_posts else 0
        followers_count = 0  # 将来実装
        following_count = 0  # 将来実装
        
        # 投稿を新しい順にソート
        if user_posts:
            user_posts = sorted(
                user_posts,
                key=lambda x: x.get('created_at', ''),
                reverse=True
            )
        
        print(f"[DEBUG] Rendering profile template")
        return render_template(
            'users/profile.html',
            user=user,
            posts=user_posts,
            posts_count=posts_count,
            followers_count=followers_count,
            following_count=following_count
        )
        
    except Exception as e:
        print(f"[ERROR] Error in user_profile: {str(e)}")
        import traceback
        print(traceback.format_exc())
        flash('プロフィールの読み込み中にエラーが発生しました。', 'error')
        return redirect(url_for('index'))
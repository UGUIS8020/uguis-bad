from flask import Blueprint, render_template, flash, redirect, url_for
from flask_login import login_required, current_user
from .dynamo import db

# Blueprintの作成
users = Blueprint('users', __name__)

@users.route('/user/<user_id>')
@login_required
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
            
        # 投稿を取得する前のログ
        print(f"[DEBUG] Attempting to get posts for user_id: {user_id}")
            
        # ユーザーの投稿を取得
        user_posts = db.get_posts_by_user(user_id)
        print(f"[DEBUG] Retrieved {len(user_posts) if user_posts else 0} posts")
        
        print(f"[DEBUG] Rendering profile template")
        return render_template(
            'users/profile.html',
            user=user,
            posts=user_posts
        )
        
    except Exception as e:
        print(f"[ERROR] Error in user_profile: {str(e)}")
        import traceback
        print(traceback.format_exc())
        flash('プロフィールの読み込み中にエラーが発生しました。', 'error')
        return redirect(url_for('index'))
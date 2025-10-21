from flask import Blueprint, render_template, flash, redirect, url_for
from flask_login import login_required, current_user
from .dynamo import db
from decimal import Decimal

# Blueprintの作成
users = Blueprint('users', __name__)

# @users.route('/user/<user_id>')
# def user_profile(user_id):
#     try:
#         print(f"[DEBUG] Start loading profile for user_id: {user_id}")
        
#         # ユーザー情報の取得
#         user = db.get_user_by_id(user_id)
#         print(f"[DEBUG] Retrieved user data: {user}")
        
#         if not user:
#             print(f"[DEBUG] No user found for user_id: {user_id}")
#             flash('ユーザーが見つかりませんでした。', 'error')
#             return redirect(url_for('index'))
            
#         # ユーザーの投稿を取得
#         user_posts = db.get_posts_by_user(user_id) or []
#         print(f"[DEBUG] Retrieved {len(user_posts)} posts")
        
#         # === ここで過去の参加回数を取得 ===
#         raw_count = user.get("practice_count", 0)
#         if isinstance(raw_count, Decimal):
#             past_participation_count = int(raw_count)
#         else:
#             try:
#                 past_participation_count = int(raw_count)
#             except Exception:
#                 past_participation_count = 0
#         print(f"[DEBUG] Past participation count: {past_participation_count}")
        
#         # 投稿を新しい順にソート
#         user_posts = sorted(
#             user_posts,
#             key=lambda x: x.get('created_at', ''),
#             reverse=True
#         )
        
#         print(f"[DEBUG] Rendering profile template")
#         return render_template(
#             'uguu/users.html',
#             user=user,
#             posts=user_posts,
#             posts_count=len(user_posts),
#             followers_count=0,
#             following_count=0,
#             past_participation_count=past_participation_count  # ← ここでテンプレに渡す
#         )
        
#     except Exception as e:
#         print(f"[ERROR] Error in user_profile: {str(e)}")
#         import traceback
#         print(traceback.format_exc())
#         flash('プロフィールの読み込み中にエラーが発生しました。', 'error')
#         return redirect(url_for('index'))
    

@users.route('/user/<user_id>')
def user_profile(user_id):
    try:
        print(f"[DEBUG] Start loading profile for user_id: {user_id}")
        
        # ユーザー情報を取得
        user = db.get_user_by_id(user_id)
        
        if not user:
            flash('ユーザーが見つかりませんでした。', 'error')
            return redirect(url_for('index'))
        
        # ユーザーの投稿を取得
        user_posts = db.get_posts_by_user(user_id)
        
        # うぐポイントと統計情報を取得
        user_stats = db.get_user_stats(user_id)
        
        # 投稿を新しい順にソート
        if user_posts:
            user_posts = sorted(
                user_posts,
                key=lambda x: x.get('created_at', ''),
                reverse=True
            )
        
        return render_template(
            'uguu/users.html',
            user=user,
            posts=user_posts,
            posts_count=len(user_posts) if user_posts else 0,
            past_participation_count=user_stats['total_participation'],  # 総参加回数
            participation_points=user_stats['uguu_points'],  # うぐポイント
            followers_count=0,
            following_count=0
        )
        
    except Exception as e:
        print(f"[ERROR] Error in user_profile: {str(e)}")
        flash('プロフィールの読み込み中にエラーが発生しました。', 'error')
        return redirect(url_for('index'))
from flask import Blueprint, render_template, redirect, url_for, flash
from .dynamo import db
from flask_login import current_user, login_required

# Blueprintの作成
uguu = Blueprint('uguu', __name__)

@uguu.route('/')
def show_timeline():
    """タイムラインを表示"""
    try:
        print("Starting timeline display process")
        
        # ログイン状態をチェック
        if current_user.is_authenticated:
            print(f"Current user ID: {current_user.id}")
            user_id = current_user.id
        else:
            print("Anonymous user accessing timeline")
            user_id = None
        
        posts = db.get_posts()
        print(f"Retrieved {len(posts) if posts else 0} posts")
        
        if posts:
            # いいね状態の確認（ログインユーザーのみ）
            for post in posts:
                try:
                    if user_id:  # ログインユーザーの場合のみいいね状態をチェック
                        post['is_liked_by_user'] = db.check_if_liked(
                            post['post_id'], 
                            user_id
                        )
                        print(f"Like status checked for post {post['post_id']}")
                    else:  # 匿名ユーザーの場合はFalse
                        post['is_liked_by_user'] = False
                        
                except Exception as e:
                    print(f"Error checking like status: {str(e)}")
                    post['is_liked_by_user'] = False
            
            # 時系列順にソート
            posts = sorted(
                posts,
                key=lambda x: x.get('updated_at', x.get('created_at', '')),
                reverse=True
            )
            print("Posts sorted successfully")
            
        return render_template('uguu/timeline.html', 
                             posts=posts,
                             is_authenticated=current_user.is_authenticated)
        
    except Exception as e:
        print(f"Timeline Error: {str(e)}")
        import traceback
        print(traceback.format_exc())
        flash('タイムラインの取得中にエラーが発生しました。', 'danger')
        return render_template('uguu/timeline.html', 
                             posts=[],
                             is_authenticated=current_user.is_authenticated)
        
    except Exception as e:
        print(f"Timeline Error: {str(e)}")
        import traceback
        print(traceback.format_exc())  # スタックトレース出力を追加
        flash('タイムラインの取得中にエラーが発生しました。', 'danger')
        return render_template('uguu/timeline.html', posts=[])

@uguu.route('/my_posts')

def show_my_posts():
    """自分の投稿のみを表示"""
    try:
        # ユーザーの投稿を取得
        posts = db.get_user_posts(current_user.id)
        
        if posts:
            posts = sorted(posts, key=lambda x: x['updated_at'], reverse=True)
            
        return render_template(
            'uguu/timeline.html',
            posts=posts,
            show_my_posts=True
        )
        
    except Exception as e:
        print(f"My Posts Error: {e}")
        flash('投稿の取得中にエラーが発生しました。', 'danger')
        return redirect(url_for('timeline.show_timeline'))
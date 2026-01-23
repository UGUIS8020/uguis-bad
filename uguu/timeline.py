from flask import Blueprint, render_template, redirect, url_for, flash, current_app
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
            # DynamoDB ユーザーテーブル（current_appで統一）
            user_table = current_app.dynamodb.Table(current_app.table_name)
            
            for post in posts:
                try:
                    # DynamoDBからユーザー情報を取得してプロフィール画像を追加
                    post_user_id = post.get('user_id')
                    if post_user_id:
                        res = user_table.get_item(Key={"user#user_id": post_user_id})
                        user = res.get("Item")
                        
                        if user:
                            # 画像URLは複数候補からフォールバック
                            url = (user.get("profile_image_url")
                                   or user.get("profileImageUrl")
                                   or user.get("large_image_url")
                                   or "")
                            url = url.strip() if isinstance(url, str) else None
                            
                            # 投稿データにプロフィール画像URLを追加
                            post['profile_image_url'] = url if url and url.lower() != "none" else None
                            
                            # display_nameも更新（念のため）
                            if 'display_name' not in post or not post['display_name']:
                                post['display_name'] = user.get("display_name", "不明")
                    
                    # 返信を取得
                    post['replies'] = db.get_post_replies(post['post_id'])
                    post['replies_count'] = len(post['replies'])
                    print(f"Post {post['post_id']}: {post['replies_count']}件の返信")
                    
                    # いいね状態の確認（ログインユーザーのみ）
                    if user_id:
                        post['is_liked_by_user'] = db.check_if_liked(
                            post['post_id'], 
                            user_id
                        )
                        print(f"Like status checked for post {post['post_id']}")
                    else:
                        post['is_liked_by_user'] = False
                        
                except Exception as e:
                    print(f"Error processing post {post.get('post_id')}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    post['is_liked_by_user'] = False
                    post['replies'] = []  # ★ 追加
                    post['replies_count'] = 0  # ★ 追加
                    if 'profile_image_url' not in post:
                        post['profile_image_url'] = None
            
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


@uguu.route('/my_posts')

def show_my_posts():
    """自分の投稿のみを表示"""
    try:
        # ユーザーの投稿を取得
        posts = db.get_user_posts(current_user.id)
        
        if posts:
            # ★★★ ここに追加 ★★★
            for post in posts:
                post['replies'] = db.get_post_replies(post['post_id'])
                post['replies_count'] = len(post['replies'])
            # ★★★ ここまで ★★★
            
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
    


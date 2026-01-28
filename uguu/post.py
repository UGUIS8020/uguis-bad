from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session, g
from flask_login import current_user, login_required
from .dynamo import db
from utils.s3 import upload_image_to_s3, delete_image_from_s3
from uuid import uuid4
from datetime import datetime, timezone



post = Blueprint('post', __name__)

@post.route('/post', methods=['GET', 'POST'])
@login_required 
def create_post():
    if request.method == 'POST':       
        content = request.form.get('content', '').strip()
        youtube_url = request.form.get('youtube_url', '').strip()  # YouTube URL取得
        image = request.files.get('image')
        print(f"投稿内容: {repr(content)}")      
        
        # バリデーションを追加
        if not content:
            flash('投稿内容を入力してください', 'warning')
            return render_template('uguu/create_post.html')
        
        if len(content) > 1000:
            flash('投稿内容は1000文字以内で入力してください', 'warning')
            return render_template('uguu/create_post.html')
        
        try:
            # 画像がアップロードされた場合、S3にアップロード
            image_url = None
            if image and image.filename:
                # 画像ファイル形式チェック
                allowed_extensions = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
                file_ext = image.filename.lower().split('.')[-1]
                
                if file_ext not in allowed_extensions:
                    flash('サポートされていない画像形式です', 'warning')
                    return render_template('uguu/create_post.html')
                
                image_url = upload_image_to_s3(image)
                print(f"画像アップロード成功: {image_url}")
            
            # YouTube URLを埋め込み形式に変換
            embed_url = None
            if youtube_url:
                embed_url = convert_youtube_url(youtube_url)
                if not embed_url:
                    flash('有効なYouTube URLを入力してください', 'warning')
                    return render_template('uguu/create_post.html')
                print(f"YouTube URL変換成功: {embed_url}")
            
            # DynamoDBに保存（youtube_urlを追加）
            db.create_post(current_user.id, content, image_url, embed_url)
            print("投稿作成成功")
            
            flash('投稿が完了しました', 'success')
            return redirect(url_for('uguu.show_timeline'))
            
        except Exception as e:
            print(f"投稿作成エラー: {str(e)}")
            import traceback
            print(traceback.format_exc())
            flash('投稿の作成に失敗しました', 'error')
            return render_template('uguu/create_post.html')
    
    return render_template('uguu/create_post.html')

def convert_youtube_url(url):
    """
    YouTube URLを埋め込み形式に変換
    
    対応形式:
    - https://www.youtube.com/watch?v=VIDEO_ID
    - https://youtu.be/VIDEO_ID
    - https://www.youtube.com/shorts/VIDEO_ID (Shorts対応)
    - https://www.youtube.com/embed/VIDEO_ID (既に埋め込み形式)
    """
    import re
    
    if not url:
        return None
    
    # 既に埋め込み形式の場合
    if 'youtube.com/embed/' in url:
        return url
    
    # YouTube Shorts の場合
    if 'youtube.com/shorts/' in url:
        match = re.search(r'youtube\.com/shorts/([a-zA-Z0-9_-]{11})', url)
        if match:
            video_id = match.group(1)
            return f'https://www.youtube.com/embed/{video_id}'
    
    # 通常のYouTube URL (watch?v=) または短縮URL (youtu.be)
    match = re.search(r'(?:youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]{11})', url)
    if match:
        video_id = match.group(1)
        return f'https://www.youtube.com/embed/{video_id}'
    
    # マッチしない場合はNoneを返す
    return None

@post.route('/post/<post_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_post(post_id):
    post_data = db.get_post(post_id)
    if not post_data or post_data['user_id'] != current_user.id:
        flash('投稿が見つからないか、編集権限がありません')
        return redirect(url_for('uguu.show_timeline'))

    if request.method == 'POST':
        content = (request.form.get('content') or "").strip()
        if not content:
            flash('投稿内容を入力してください')
            return redirect(url_for('post.edit_post', post_id=post_id))

        remove_image = request.form.get('remove_image') == "1"
        file = request.files.get('image')

        try:
            new_image_url = None
            image_url_to_set = None
            image_url_specified = False

            # 画像削除（チェックONなら image_url=None をセット）
            if remove_image:
                image_url_to_set = None
                image_url_specified = True

            # 画像差し替え（ファイルが来たら優先）
            if file and file.filename:
                # ここをあなたのS3アップロード処理に置き換え
                new_image_url = upload_image_to_s3(file, user_id=current_user.id, post_id=post_id)
                image_url_to_set = new_image_url
                image_url_specified = True

            # DB更新：contentは必須、image_urlは指定があった時だけ
            if image_url_specified:
                db.update_post_fields(post_id, content=content, image_url=image_url_to_set)
            else:
                db.update_post_fields(post_id, content=content)

            flash('投稿を更新しました')
            return redirect(url_for('uguu.show_timeline'))

        except Exception as e:
            print(f"Error: {e}")
            flash('更新に失敗しました')
            return redirect(url_for('post.edit_post', post_id=post_id))

    return render_template('uguu/edit_post.html', post=post_data)

@post.route('/like/<post_id>', methods=['POST'])
@login_required  # login_requiredを追加
def like_post(post_id):
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        try:
            is_liked = db.like_post(post_id, current_user.id)
            likes_count = db.get_likes_count(post_id)
            return jsonify({
                'is_liked': is_liked,
                'likes_count': likes_count
            })
        except Exception as e:
            print(f"Error in like_post route: {e}")
            return jsonify({'error': 'いいねの処理に失敗しました'}), 500
    else:
        # 通常のフォーム送信の場合
        try:
            is_liked = db.like_post(post_id, current_user.id)
            return redirect(url_for('uguu.show_timeline'))
        except Exception as e:
            print(f"Error in like_post route: {e}")
            flash('いいねの処理に失敗しました', 'error')
            return redirect(url_for('uguu.show_timeline'))

@post.route('/reply/<post_id>', methods=['POST'])
@login_required
def create_reply(post_id):
    content = (request.form.get('content') or "").strip()
    if not content:
        flash('内容を入力してください。', 'warning')
        return redirect(url_for('uguu.show_timeline'))

    try:
        reply_id = str(uuid4())
        now = datetime.now(timezone.utc).isoformat()

        item = {
            "post_id": post_id,                 
            "sk": f"REPLY#{reply_id}",          
            "reply_id": reply_id,
            "user_id": str(current_user.id),
            "content": content,
            "created_at": now,
            "display_name": getattr(current_user, "display_name", "不明"),
        }

        db.replies_table.put_item(Item=item)
        flash('返信を投稿しました', 'success')

    except Exception as e:
        print(f"返信作成エラー: {e}")
        flash('返信の投稿に失敗しました', 'error')

    return redirect(url_for('uguu.show_timeline'))


@post.route('/post/<post_id>/delete', methods=['POST'])
@login_required
def delete_post(post_id):
    try:
        post_data = db.get_post(post_id)
        if not post_data:
            flash('投稿が見つかりません', 'error')
            return redirect(url_for('uguu.show_timeline'))

        if str(post_data.get('user_id')) != str(current_user.id):
            flash('投稿を削除する権限がありません', 'error')
            return redirect(url_for('uguu.show_timeline'))

        # 投稿本体削除（uguu_post）
        db.delete_post(post_id)

        # 返信削除（post-replies）
        db.delete_post_replies(post_id)

        # いいね削除（あるなら）
        db.delete_post_likes(post_id)

        flash('投稿を削除しました', 'success')
        return redirect(url_for('uguu.show_timeline'))

    except Exception as e:
        print(f"[delete_post route] error: {e}")
        flash('投稿の削除に失敗しました', 'error')
        return redirect(url_for('uguu.show_timeline')) 



# def delete_post(post_id):
#     """投稿を削除"""
#     try:
#         # 投稿を取得して存在確認
#         post_data = db.get_post(post_id)
#         if not post_data:
#             flash('投稿が見つかりません', 'error')
#             return redirect(url_for('uguu.show_timeline'))
        
#         # 投稿者本人か確認
#         if str(post_data['user_id']) != str(current_user.id):
#             flash('投稿を削除する権限がありません', 'error')
#             return redirect(url_for('uguu.show_timeline'))
        
#         # S3から画像を削除（もし画像がある場合）
#         if post_data.get('image_url'):
#             try:
#                 delete_image_from_s3(post_data['image_url'])
#             except Exception as e:
#                 pass  # S3削除に失敗してもDynamoDB削除は続行
        
#         # DynamoDBから投稿を削除
#         db.delete_post(post_id)
        
#         # 関連するいいねや返信も削除
#         try:
#             db.delete_post_likes(post_id)
#             db.delete_post_replies(post_id)
#         except Exception as e:
#             pass  # 関連データ削除に失敗しても続行
        
#         flash('投稿を削除しました', 'success')
        
#     except Exception as e:
#         flash('投稿の削除に失敗しました', 'error')
    
#     return redirect(url_for('uguu.show_timeline'))


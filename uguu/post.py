from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session, g
from flask_login import current_user, login_required
from .dynamo import db
from utils.s3 import upload_image_to_s3, delete_image_from_s3
import uuid
from datetime import datetime

post = Blueprint('post', __name__)

@post.route('/post', methods=['GET', 'POST'])
@login_required 
def create_post():
    if request.method == 'POST':       
        content = request.form.get('content', '').strip()
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
            
            # DynamoDBに保存
            db.create_post(current_user.id, content, image_url)
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

@post.route('/post/<post_id>/edit', methods=['GET', 'POST'])
@login_required  # login_requiredを追加
def edit_post(post_id):
    """投稿を編集"""
    # 投稿を取得
    post_data = db.get_post(post_id)
    if not post_data or post_data['user_id'] != current_user.id:  # current_userを使用
        flash('投稿が見つからないか、編集権限がありません')
        return redirect(url_for('uguu.show_timeline'))  # 正しいURLに修正
        
    if request.method == 'POST':
        content = request.form.get('content')
        if not content:
            flash('投稿内容を入力してください')
            return redirect(url_for('uguu.show_timeline'))
            
        try:
            # 投稿を更新
            db.update_post(post_id, content)
            flash('投稿を更新しました')
        except Exception as e:
            print(f"Error: {e}")
            flash('更新に失敗しました')
            
        return redirect(url_for('uguu.show_timeline'))
        
    # GET リクエストの場合は編集フォームを表示
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
@login_required  # login_requiredを追加
def create_reply(post_id):
    """返信を作成する"""
    content = request.form.get('content')
    
    if not content:
        flash('内容を入力してください。')
        return redirect(url_for('uguu.show_timeline'))
    
    try:
        # 返信をDynamoDBに保存（PK/SK構造に合わせる）
        reply_id = str(uuid.uuid4())
        reply_data = {
            'PK': f"POST#{post_id}",
            'SK': f"REPLY#{reply_id}",
            'reply_id': reply_id,
            'post_id': post_id,
            'user_id': current_user.id,
            'content': content,
            'created_at': datetime.now().isoformat(),
            'display_name': current_user.display_name  # current_userから取得
        }
        
        db.posts_table.put_item(Item=reply_data)
        flash('返信を投稿しました', 'success')
        
    except Exception as e:
        print(f"返信作成エラー: {str(e)}")
        flash('返信の投稿に失敗しました', 'error')

    return redirect(url_for('uguu.show_timeline'))

@post.route('/post/<post_id>/delete', methods=['POST'])
@login_required
def delete_post(post_id):
    """投稿を削除"""
    try:
        # 投稿を取得して存在確認
        post_data = db.get_post(post_id)
        if not post_data:
            flash('投稿が見つかりません', 'error')
            return redirect(url_for('uguu.show_timeline'))
        
        # 投稿者本人か確認
        if str(post_data['user_id']) != str(current_user.id):
            flash('投稿を削除する権限がありません', 'error')
            return redirect(url_for('uguu.show_timeline'))
        
        # S3から画像を削除（もし画像がある場合）
        if post_data.get('image_url'):
            try:
                delete_image_from_s3(post_data['image_url'])
            except Exception as e:
                pass  # S3削除に失敗してもDynamoDB削除は続行
        
        # DynamoDBから投稿を削除
        db.delete_post(post_id)
        
        # 関連するいいねや返信も削除
        try:
            db.delete_post_likes(post_id)
            db.delete_post_replies(post_id)
        except Exception as e:
            pass  # 関連データ削除に失敗しても続行
        
        flash('投稿を削除しました', 'success')
        
    except Exception as e:
        flash('投稿の削除に失敗しました', 'error')
    
    return redirect(url_for('uguu.show_timeline'))
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import current_user, login_required
from .dynamo import db
from utils.s3 import upload_image_to_s3
import uuid
from datetime import datetime
from boto3.dynamodb.conditions import Key


post = Blueprint('post', __name__)

@post.route('/post', methods=['GET', 'POST'])
@login_required
def create_post():
    if request.method == 'POST':       
        content = request.form.get('content')
        image = request.files.get('image')  # 画像ファイルを取得        
        
        try:
            # 画像がアップロードされた場合、S3にアップロード
            image_url = None
            if image:
                image_url = upload_image_to_s3(image)                
            
            # DynamoDBに保存（画像URLも含める）
            db.create_post(current_user.id, content, image_url)            
            
            flash('投稿が完了しました', 'success')
            return redirect(url_for('uguu.show_timeline'))
            
        except Exception as e:            
            flash('投稿の作成に失敗しました', 'error')
            return redirect(url_for('post.create_post'))
    
    return render_template('uguu/create_post.html')

@post.route('/post/<post_id>/edit', methods=['GET', 'POST'])
def edit_post(post_id):
    """投稿を編集"""
    if 'user_id' not in session:
        return redirect(url_for('login'))
        
    # 投稿を取得
    post = db.get_post(post_id)
    if not post or post['user_id'] != session['user_id']:
        flash('投稿が見つからないか、編集権限がありません')
        return redirect(url_for('timeline.show_timeline'))
        
    if request.method == 'POST':
        content = request.form.get('content')
        if not content:
            flash('投稿内容を入力してください')
            return redirect(url_for('timeline.show_timeline'))
            
        try:
            # 投稿を更新
            db.update_post(post_id, content)
            flash('投稿を更新しました')
        except Exception as e:
            print(f"Error: {e}")
            flash('更新に失敗しました')
            
        return redirect(url_for('timeline.show_timeline'))
        
    # GET リクエストの場合は編集フォームを表示
    return render_template('uguu/edit_post.html', post=post)

@post.route('/like/<post_id>', methods=['POST'])
@login_required
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
        
@post.route('/reply/<post_id>', methods=['POST'])  # intの型指定を削除
@login_required
def create_reply(post_id):
    """返信を作成する"""
    if request.method == 'POST':
        content = request.form['content']
        error = None

        if not content:
            error = '内容を入力してください。'

        if error is not None:
            flash(error)
        else:
            db = get_db()
            # DynamoDBのテーブルに返信を保存
            response = db.Table('replies').put_item(
                Item={
                    'reply_id': str(uuid.uuid4()),  # 新しいUUIDを生成
                    'post_id': post_id,
                    'user_id': g.user['user_id'],
                    'content': content,
                    'created_at': datetime.now().isoformat(),
                    'display_name': g.user['display_name']
                }
            )
            return redirect(url_for('uguu.timeline'))

    return redirect(url_for('uguu.timeline'))
from flask import Blueprint, render_template, redirect, url_for, flash, current_app
from .dynamo import db
from flask_login import current_user, login_required

from typing import Any, cast

# Blueprintの作成
uguu = Blueprint('uguu', __name__)

@uguu.route('/')
def show_timeline():
    try:
        print("Starting timeline display process")

        viewer_id = current_user.id if current_user.is_authenticated else None

        posts, next_cursor = db.get_posts_page(limit=5, cursor=None)

        print(f"Retrieved {len(posts) if posts else 0} posts, next_cursor={bool(next_cursor)}")

        if posts:
            app = cast(Any, current_app)
            user_table = app.dynamodb.Table(app.table_name)

            for post in posts:
                try:
                    post_user_id = post.get('user_id')

                    # ✅ 編集可否（本人だけ true）
                    post['can_edit'] = bool(viewer_id and post_user_id and viewer_id == post_user_id)

                    if post_user_id:
                        res = user_table.get_item(Key={"user#user_id": post_user_id})
                        user = res.get("Item") or {}

                        url = (user.get("profile_image_url")
                               or user.get("profileImageUrl")
                               or user.get("large_image_url")
                               or "")
                        url = url.strip() if isinstance(url, str) else ""

                        post['profile_image_url'] = url if url and url.lower() != "none" else None
                        post['display_name'] = post.get('display_name') or user.get("display_name", "不明")

                    post.setdefault('replies', [])
                    post.setdefault('replies_count', 0)

                    # ✅ 管理者判定（属性名は必要なら後で合わせます）
                    is_admin = bool(
                        getattr(current_user, "administrator", False)
                        or getattr(current_user, "is_admin", False)
                    )

                    # ✅ 返信ごとの削除可否（返信者本人 or 管理者）
                    for r in post.get("replies", []):
                        r_user_id = (r.get("user_id") or "").strip()
                        r["can_delete"] = bool(viewer_id and (viewer_id == r_user_id or is_admin))

                    if viewer_id:
                        post['is_liked_by_user'] = db.check_if_liked(post['post_id'], viewer_id)
                    else:
                        post['is_liked_by_user'] = False

                except Exception as e:
                    print(f"Error processing post {post.get('post_id')}: {str(e)}")
                    post.setdefault('profile_image_url', None)
                    post.setdefault('display_name', "不明")
                    post['replies'] = []
                    post['replies_count'] = 0
                    post['is_liked_by_user'] = False
                    post['can_edit'] = False  # ✅ 念のため

            posts = sorted(posts, key=lambda x: x.get('updated_at', x.get('created_at', '')), reverse=True)

        return render_template(
            'uguu/timeline.html',
            posts=posts,
            next_cursor=next_cursor,
            is_authenticated=current_user.is_authenticated
        )

    except Exception as e:
        print(f"Timeline Error: {str(e)}")
        flash('タイムラインの取得中にエラーが発生しました。', 'danger')
        return render_template(
            'uguu/timeline.html',
            posts=[],
            next_cursor="",
            is_authenticated=current_user.is_authenticated
        ) 
    
@uguu.route("/reply/<post_id>/<reply_id>/delete", methods=["POST"])
@login_required
def delete_reply(post_id, reply_id):
    viewer_id = current_user.id
    is_admin = bool(getattr(current_user, "administrator", False) or getattr(current_user, "is_admin", False))

    # 所有者チェック（安全）
    sk = f"REPLY#{reply_id}"
    res = db.replies_table.get_item(Key={"post_id": str(post_id), "sk": sk})
    item = res.get("Item")
    if not item:
        flash("返信が見つかりませんでした", "warning")
        return redirect(url_for("uguu.show_timeline"))

    if not (viewer_id == (item.get("user_id") or "") or is_admin):
        abort(403)

    ok = db.delete_reply(post_id, reply_id)
    flash("返信を削除しました" if ok else "削除に失敗しました", "success" if ok else "danger")
    return redirect(url_for("uguu.show_timeline"))
    
    
import base64, json

def encode_cursor(lek):
    if not lek:
        return ""
    raw = json.dumps(lek, ensure_ascii=False).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")

def decode_cursor(token):
    if not token:
        return None
    raw = base64.urlsafe_b64decode(token.encode("ascii"))
    return json.loads(raw.decode("utf-8"))

from flask import request, jsonify
from flask_login import current_user

@uguu.route("/api/feed")
def api_feed():
    cursor = request.args.get("cursor") or ""     # トークン文字列のまま
    limit = int(request.args.get("limit", 5))

    posts, next_cursor = db.get_posts_page(limit=limit, cursor=cursor)  # get_posts_pageがdecodeする

    viewer_id = current_user.id if getattr(current_user, "is_authenticated", False) else None

    for p in posts:
        uid = p.get("user_id")
        p["can_edit"] = bool(viewer_id and uid and viewer_id == uid)
        if uid:
            try:
                u = db.users_table.get_item(Key={"user#user_id": uid}).get("Item") or {}
                p["display_name"] = p.get("display_name") or u.get("display_name") or "不明"
                p["user_name"] = p.get("user_name") or u.get("user_name") or ""

                url = (u.get("profile_image_url")
                       or u.get("profileImageUrl")
                       or u.get("large_image_url")
                       or "")
                url = url.strip() if isinstance(url, str) else ""
                p["profile_image_url"] = url if url and url.lower() != "none" else None
            except Exception:
                p.setdefault("display_name", "不明")
                p.setdefault("user_name", "")
                p.setdefault("profile_image_url", None)
        else:
            p.setdefault("display_name", "不明")
            p.setdefault("user_name", "")
            p.setdefault("profile_image_url", None)

        p.setdefault("replies_count", 0)
        p.setdefault("replies", [])

        if viewer_id and p.get("post_id"):
            try:
                p["is_liked_by_user"] = db.check_if_liked(p["post_id"], viewer_id)
            except Exception:
                p["is_liked_by_user"] = False
        else:
            p["is_liked_by_user"] = False

    # next_cursor はすでにトークン文字列
    return jsonify({"posts": posts, "next_cursor": next_cursor})     


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
    


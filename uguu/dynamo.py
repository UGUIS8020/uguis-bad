import boto3
import os
from datetime import datetime
import uuid
from dotenv import load_dotenv
from boto3.dynamodb.conditions import Key, Attr

load_dotenv()

class DynamoDB:
    def __init__(self):
        self.dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION")
        )
        self.posts_table = self.dynamodb.Table('posts')
        self.users_table = self.dynamodb.Table('bad-users')
        print("DynamoDB tables initialized")

    def get_posts(self, limit=20):
        """投稿一覧を取得（PK/SK構造対応版）"""
        try:
            print("Starting to fetch posts...")
            
            # PK/SK構造に対応したクエリ
            response = self.posts_table.scan(
                FilterExpression="begins_with(SK, :metadata)",
                ExpressionAttributeValues={
                    ':metadata': 'METADATA#'
                }
            )
            posts = response.get('Items', [])
            print(f"Found {len(posts)} posts")
            
            enriched_posts = []
            for post in posts:
                try:
                    user_id = post.get('user_id')
                    if user_id:
                        print(f"Processing post for user: {user_id}")
                        
                        try:
                            user_response = self.users_table.get_item(
                                Key={
                                    'user#user_id': user_id
                                }
                            )
                            user = user_response.get('Item', {})
                            
                            enriched_post = {
                                'post_id': post.get('post_id'),
                                'content': post.get('content'),
                                'image_url': post.get('image_url'),
                                'youtube_url': post.get('youtube_url'),  # ← 追加
                                'created_at': post.get('created_at'),
                                'updated_at': post.get('updated_at', post.get('created_at')),
                                'user_id': user_id,
                                'display_name': user.get('display_name', 'Unknown User'),
                                'user_name': user.get('user_name', 'Unknown')
                            }
                            enriched_posts.append(enriched_post)
                            print(f"Successfully processed post: {post.get('post_id')}")
                        except Exception as e:
                            print(f"Error fetching user: {str(e)}")
                            # エラー時にもポストは表示する
                            enriched_post = {
                                'post_id': post.get('post_id'),
                                'content': post.get('content'),
                                'image_url': post.get('image_url'),
                                'youtube_url': post.get('youtube_url'),  # ← 追加
                                'created_at': post.get('created_at'),
                                'updated_at': post.get('updated_at', post.get('created_at')),
                                'user_id': user_id,
                                'display_name': '不明なユーザー',
                                'user_name': 'Unknown'
                            }
                            enriched_posts.append(enriched_post)
                except Exception as e:
                    print(f"Error processing post: {str(e)}")
                    continue
            
            return sorted(enriched_posts, 
                        key=lambda x: x.get('updated_at', x.get('created_at', '')), 
                        reverse=True)[:limit]
            
        except Exception as e:
            print(f"Error in get_posts: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return []

    def get_post(self, post_id):
        """特定の投稿を取得（PK/SK構造対応）"""
        try:
            print(f"Fetching post: {post_id}")
            
            # PK/SK構造に合わせて投稿を取得
            response = self.posts_table.get_item(
                Key={
                    'PK': f"POST#{post_id}",
                    'SK': f"METADATA#{post_id}"
                }
            )
            
            print(f"DynamoDB response: {response}")
            
            if 'Item' not in response:
                print(f"Post not found: {post_id}")
                return None
                
            post = response['Item']
            print(f"Raw post data: {post}")
            
            # ユーザー情報を取得して投稿を充実させる
            user_id = post.get('user_id')
            if user_id:
                try:
                    user_response = self.users_table.get_item(
                        Key={'user#user_id': user_id}
                    )
                    user = user_response.get('Item', {})
                    
                    enriched_post = {
                        'post_id': post.get('post_id', post_id),
                        'content': post.get('content'),
                        'image_url': post.get('image_url'),
                        'created_at': post.get('created_at'),
                        'updated_at': post.get('updated_at', post.get('created_at')),
                        'user_id': user_id,
                        'display_name': user.get('display_name', 'Unknown User'),
                        'user_name': user.get('user_name', 'Unknown')
                    }
                    return enriched_post
                    
                except Exception as e:
                    print(f"Error fetching user: {str(e)}")
                    # ユーザー情報取得失敗時のフォールバック
                    fallback_post = {
                        'post_id': post.get('post_id', post_id),
                        'content': post.get('content'),
                        'image_url': post.get('image_url'),
                        'created_at': post.get('created_at'),
                        'updated_at': post.get('updated_at', post.get('created_at')),
                        'user_id': user_id,
                        'display_name': '不明なユーザー',
                        'user_name': 'Unknown'
                    }
                    return fallback_post
            
            # user_idがない場合
            post['post_id'] = post.get('post_id', post_id)
            return post
                
        except Exception as e:
            print(f"投稿取得エラー: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return None

    def create_post(self, user_id, content, image_url=None, youtube_url=None):
        """新規投稿を作成"""
        try:
            post_id = str(uuid.uuid4())
            timestamp = datetime.now().isoformat()
            
            post = {
                'PK': f"POST#{post_id}",
                'SK': f"METADATA#{post_id}",
                'post_id': post_id,
                'user_id': user_id,
                'content': content,
                'image_url': image_url,
                'youtube_url': youtube_url,  # YouTube URL追加
                'created_at': timestamp,
                'updated_at': timestamp
            }
            print(f"Post data: {post}")
            
            self.posts_table.put_item(Item=post)
            print("Post created successfully in DynamoDB")
            return post
            
        except Exception as e:
            print(f"DynamoDB Error: {str(e)}")
            raise

    def update_post(self, post_id, content):
        """投稿を更新"""
        try:
            timestamp = datetime.now().isoformat()
            self.posts_table.update_item(
                Key={
                    'PK': f"POST#{post_id}",
                    'SK': f"METADATA#{post_id}"
                },
                UpdateExpression='SET content = :content, updated_at = :updated_at',
                ExpressionAttributeValues={
                    ':content': content,
                    ':updated_at': timestamp
                }
            )
            return True
        except Exception as e:
            print(f"Error updating post: {e}")
            raise

    def delete_post(self, post_id):
        """投稿を削除"""
        try:
            # 投稿を検索して実際のキー構造を確認
            response = self.posts_table.scan()
            posts = response.get('Items', [])
            
            target_post = None
            for post in posts:
                if str(post.get('post_id')) == str(post_id):
                    target_post = post
                    break
            
            if not target_post:
                raise Exception(f"Post {post_id} not found")
            
            # 実際のキー構造に基づいて削除
            if 'PK' in target_post and 'SK' in target_post:
                # PK/SK構造の場合
                delete_key = {
                    'PK': target_post['PK'],
                    'SK': target_post['SK']
                }
            else:
                # post_id直接の場合
                delete_key = {
                    'post_id': post_id
                }
            
            response = self.posts_table.delete_item(Key=delete_key)
            return response
            
        except Exception as e:
            raise Exception(f"投稿削除エラー: {str(e)}")

    def delete_post_likes(self, post_id):
        """投稿に関連するいいねを削除"""
        try:
            print(f"Deleting likes for post: {post_id}")
            
            response = self.posts_table.query(
                KeyConditionExpression="PK = :pk AND begins_with(SK, :like)",
                ExpressionAttributeValues={
                    ':pk': f"POST#{post_id}",
                    ':like': 'LIKE#'
                }
            )
            
            for like in response.get('Items', []):
                self.posts_table.delete_item(
                    Key={
                        'PK': like['PK'],
                        'SK': like['SK']
                    }
                )
            
            print(f"Deleted {len(response.get('Items', []))} likes")
            return True
        except Exception as e:
            print(f"いいね削除エラー: {str(e)}")
            return False

    def delete_post_replies(self, post_id):
        """投稿に関連する返信を削除"""
        try:
            print(f"Deleting replies for post: {post_id}")
            
            response = self.posts_table.query(
                KeyConditionExpression="PK = :pk AND begins_with(SK, :reply)",
                ExpressionAttributeValues={
                    ':pk': f"POST#{post_id}",
                    ':reply': 'REPLY#'
                }
            )
            
            for reply in response.get('Items', []):
                self.posts_table.delete_item(
                    Key={
                        'PK': reply['PK'],
                        'SK': reply['SK']
                    }
                )
            
            print(f"Deleted {len(response.get('Items', []))} replies")
            return True
        except Exception as e:
            print(f"返信削除エラー: {str(e)}")
            return False

    def like_post(self, post_id, user_id):
        """投稿にいいねを追加/削除"""
        try:
            # いいねの状態を確認
            like_key = {
                'PK': f"POST#{post_id}",
                'SK': f"LIKE#{user_id}"
            }
            
            response = self.posts_table.get_item(Key=like_key)
            
            if 'Item' in response:
                # いいねを削除
                self.posts_table.delete_item(Key=like_key)
                self.update_likes_count(post_id, -1)
                return False
            else:
                # いいねを追加
                like_data = {
                    'PK': f"POST#{post_id}",
                    'SK': f"LIKE#{user_id}",
                    'user_id': user_id,
                    'created_at': datetime.now().isoformat()
                }
                self.posts_table.put_item(Item=like_data)
                self.update_likes_count(post_id, 1)
                return True
                
        except Exception as e:
            print(f"Error in like_post: {e}")
            raise

    def update_likes_count(self, post_id, increment):
        """いいね数を更新"""
        try:
            self.posts_table.update_item(
                Key={
                    'PK': f"POST#{post_id}",
                    'SK': f"METADATA#{post_id}"
                },
                UpdateExpression='ADD likes_count :inc',
                ExpressionAttributeValues={
                    ':inc': increment
                }
            )
        except Exception as e:
            print(f"Error updating likes count: {e}")
            raise

    def get_likes_count(self, post_id):
        """投稿のいいね数を取得"""
        try:
            response = self.posts_table.query(
                KeyConditionExpression="PK = :pk AND begins_with(SK, :like)",
                ExpressionAttributeValues={
                    ':pk': f"POST#{post_id}",
                    ':like': 'LIKE#'
                }
            )
            return len(response.get('Items', []))
        except Exception as e:
            print(f"Error getting likes count: {e}")
            return 0

    def check_if_liked(self, post_id, user_id):
        """ユーザーが投稿をいいねしているか確認"""
        try:
            key = {
                'PK': f"POST#{post_id}",
                'SK': f"LIKE#{user_id}"
            }
            response = self.posts_table.get_item(Key=key)
            return 'Item' in response
        except Exception as e:
            print(f"Error checking like status: {e}")
            return False
    
    def get_user_by_id(self, user_id):
        """ユーザー情報を取得"""
        try:
            print(f"Fetching user: {user_id}")
            response = self.users_table.get_item(
                Key={'user#user_id': user_id}
            )
            user = response.get('Item')
            print(f"User found: {user is not None}")
            return user
        except Exception as e:
            print(f"Error getting user by id: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return None

    def get_posts_by_user(self, user_id):
        """特定ユーザーの投稿を取得"""
        try:
            print(f"Fetching posts for user: {user_id}")
            
            # postsテーブルから該当ユーザーの投稿を検索
            response = self.posts_table.scan(
                FilterExpression="begins_with(SK, :metadata) AND user_id = :user_id",
                ExpressionAttributeValues={
                    ':metadata': 'METADATA#',
                    ':user_id': user_id
                }
            )
            
            posts = response.get('Items', [])
            print(f"Found {len(posts)} posts for user {user_id}")
            
            # 投稿データを整形
            enriched_posts = []
            for post in posts:
                enriched_post = {
                    'post_id': post.get('post_id'),
                    'content': post.get('content'),
                    'image_url': post.get('image_url'),
                    'youtube_url': post.get('youtube_url'),
                    'created_at': post.get('created_at'),
                    'updated_at': post.get('updated_at', post.get('created_at')),
                    'user_id': user_id,
                    'likes_count': post.get('likes_count', 0),
                    'replies_count': post.get('replies_count', 0)
                }
                enriched_posts.append(enriched_post)
            
            return enriched_posts
            
        except Exception as e:
            print(f"Error getting posts by user: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return []

# 重要: インスタンスを作成
db = DynamoDB()
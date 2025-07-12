import boto3
import os
from datetime import datetime
import uuid
from dotenv import load_dotenv

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


    def initialize_tables(self):
        """テーブル構造を確認・表示する"""
        try:
            # Usersテーブルの構造を確認
            users_desc = self.users_table.meta.client.describe_table(
                TableName=self.users_table.name
            )
            print("Users table structure:")
            print(f"Key Schema: {users_desc['Table']['KeySchema']}")
            
            # Postsテーブルの構造を確認
            posts_desc = self.posts_table.meta.client.describe_table(
                TableName=self.posts_table.name
            )
            print("Posts table structure:")
            print(f"Key Schema: {posts_desc['Table']['KeySchema']}")
            
        except Exception as e:
            print(f"Error checking table structure: {e}")

    def get_posts(self, limit=20):
        try:
            print("Starting to fetch posts...")
            
            response = self.posts_table.scan()
            posts = response.get('Items', [])
            print(f"Found {len(posts)} posts")
            
            enriched_posts = []
            for post in posts:
                try:
                    user_id = post.get('user_id')
                    if user_id:
                        print(f"Processing post for user: {user_id}")
                        
                        try:
                            # user#user_id 形式で検索
                            user_response = self.users_table.get_item(
                                Key={
                                    'user#user_id': user_id  # このキーで直接検索
                                }
                            )
                            user = user_response.get('Item', {})
                            
                            print(f"Found user data: {user}")  # デバッグ用
                            
                            enriched_post = {
                                'post_id': post.get('post_id'),
                                'content': post.get('content'),
                                'image_url': post.get('image_url'),
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
        
    def inspect_tables(self):
        """テーブル構造を確認するヘルパー関数"""
        try:
            # usersテーブルの構造確認
            users_desc = self.users_table.meta.client.describe_table(
                TableName=self.users_table.name
            )
            print("Users table structure:")
            print(users_desc['Table']['KeySchema'])

            # postsテーブルの構造確認
            posts_desc = self.posts_table.meta.client.describe_table(
                TableName=self.posts_table.name
            )
            print("Posts table structure:")
            print(posts_desc['Table']['KeySchema'])
            
        except Exception as e:
            print(f"Error inspecting tables: {e}")
        
    def check_user_table_schema(self):
        """ユーザーテーブルのスキーマを確認"""
        try:
            table_description = self.users_table.meta.client.describe_table(
                TableName=self.users_table.name
            )
            print("User table schema:")
            print(table_description)
            
            # テストユーザーで取得を試みる
            test_response = self.users_table.get_item(
                Key={'id': '8ffbb8ef-5870-47f5-a91d-296b464ee005'}
            )
            print("Test user response:")
            print(test_response)
            
        except Exception as e:
            print(f"Error checking user table schema: {str(e)}")

    def get_user_by_id(self, user_id):
        """ユーザー情報を取得する補助メソッド"""
        try:
            print(f"Attempting to fetch user with user#user_id: {user_id}")  # デバッグ追加
            
            response = self.users_table.get_item(
                Key={'user#user_id': user_id}
            )
            
            print(f"DynamoDB response: {response}")  # デバッグ追加
            
            if 'Item' in response:
                user = response['Item']
                print(f"Found user data: {user}")  # デバッグ追加
                return user
            
            print(f"User not found with ID: {user_id}")  # デバッグ追加
            return None
                
        except Exception as e:
            print(f"Error in get_user_by_id: {str(e)}")
            import traceback
            print(traceback.format_exc())  # スタックトレース出力
            return None

    def create_post(self, user_id, content, image_url=None):
        """新規投稿を作成"""
        try:
            post_id = str(uuid.uuid4())
            timestamp = datetime.now().isoformat()
            
            post = {
                'PK': f"POST#{post_id}",  # パーティションキー
                'SK': f"METADATA#{post_id}",  # ソートキー
                'post_id': post_id,
                'user_id': user_id,
                'content': content,
                'image_url': image_url,
                'created_at': timestamp,
                'updated_at': timestamp
            }
            print(f"Post data: {post}")  # デバッグログ
            
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
                Key={'post_id': post_id},
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

    def create_posts_table(self):
        """postsテーブルが存在しない場合は作成"""
        try:
            existing_tables = self.dynamodb.meta.client.list_tables()['TableNames']
            if 'post' not in existing_tables:
                table = self.dynamodb.create_table(
                    TableName='post',
                    KeySchema=[
                        {
                            'AttributeName': 'PK',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'SK',
                            'KeyType': 'RANGE'
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'PK',
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': 'SK',
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': 'GSI1PK',
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': 'GSI1SK',
                            'AttributeType': 'S'
                        }
                    ],
                    GlobalSecondaryIndexes=[
                        {
                            'IndexName': 'GSI1',
                            'KeySchema': [
                                {'AttributeName': 'GSI1PK', 'KeyType': 'HASH'},
                                {'AttributeName': 'GSI1SK', 'KeyType': 'RANGE'}
                            ],
                            'Projection': {'ProjectionType': 'ALL'}
                        }
                    ],
                    BillingMode='PAY_PER_REQUEST'
                )
                table.meta.client.get_waiter('table_exists').wait(TableName='posts')
                print("posts table created successfully")
                return True
        except Exception as e:
            print(f"Error creating posts table: {e}")
            raise

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

# インスタンスを作成
db = DynamoDB()
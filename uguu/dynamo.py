import boto3
import os
from datetime import datetime
import uuid
from dotenv import load_dotenv
from boto3.dynamodb.conditions import Key, Attr
from collections import defaultdict

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
        self.schedule_table = self.dynamodb.Table('bad_schedules')
        self.part_history    = self.dynamodb.Table("bad-users-history")
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
        
    def get_user_participation_history(self, user_id: str):
        """
        bad-users-history からユーザーの参加日を昇順で返す
        PK=user_id, SK=date (YYYY-MM-DD) を想定
        キャンセルされた参加は除外
        """
        from datetime import datetime
        table = self.part_history
        items = []

        # user_id に対する全件取得
        resp = table.query(
            KeyConditionExpression=Key("user_id").eq(user_id),
            ProjectionExpression="#d, #s",  # dateとstatusを取得
            ExpressionAttributeNames={
                "#d": "date", 
                "#s": "status"  # statusも取得するように追加
            },
            ScanIndexForward=True
        )
        items.extend(resp.get("Items", []))

        while "LastEvaluatedKey" in resp:
            resp = table.query(
                KeyConditionExpression=Key("user_id").eq(user_id),
                ProjectionExpression="#d, #s",
                ExpressionAttributeNames={
                    "#d": "date",
                    "#s": "status"
                },
                ExclusiveStartKey=resp["LastEvaluatedKey"],
                ScanIndexForward=True
            )
            items.extend(resp.get("Items", []))

        # 現在の日付（未来の参加を除外するため）
        today = datetime.now().date()
        
        dates = []
        for it in items:
            try:
                # キャンセルされた参加は除外
                if "status" in it and it["status"] == 'cancelled':
                    print(f"[DEBUG] キャンセル済みの参加をスキップ: {it['date']}")
                    continue
                    
                # 日付文字列をdatetimeオブジェクトに変換
                date_obj = datetime.strptime(it["date"], "%Y-%m-%d")
                
                # 未来の日付は除外
                if date_obj.date() > today:
                    print(f"[DEBUG] 未来の参加日をスキップ: {it['date']}")
                    continue
                    
                dates.append(date_obj)
            except Exception as e:
                print(f"[WARN] 日付変換エラー: {it.get('date')} - {str(e)}")
                pass

        # 日付順にソート
        dates.sort()
        print(f"[DEBUG] 有効な参加履歴 - user_id: {user_id}, 件数: {len(dates)}")
        return dates
    
    def cancel_participation(self, user_id: str, date: str):
        """参加をキャンセル"""
        try:
            # 参加レコードを取得して更新
            self.part_history.update_item(
                Key={
                    'user_id': user_id,
                    'date': date
                },
                UpdateExpression='SET status = :s',
                ExpressionAttributeValues={
                    ':s': 'cancelled'
                }
            )
            return True
        except Exception as e:
            print(f"[ERROR] 参加キャンセルエラー: {str(e)}")
            return False
        
    def get_user_participation_history_with_timestamp(self, user_id):
        """
        参加履歴をタイムスタンプ付きで取得
        
        Returns:
            [
                {
                    'event_date': '2025-10-28',  # イベント日
                    'registered_at': '2025-10-26 15:30:00'  # 参加ボタンを押した日時
                },
                ...
            ]
        """
        # データベースから取得する実装
        pass
    
    def _is_early_registration(self, record):
        """
        事前参加の判定とポイント計算
        
        Returns:
            int: 参加ポイント (0, 100, or 200)
        """
        event_date = record['event_date']
        registered_at = record['registered_at']
        
        # 7日前の23:59:59を計算
        seven_days_before = event_date - timedelta(days=7)
        seven_days_deadline = seven_days_before.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        # 前々日の23:59:59を計算
        two_days_before = event_date - timedelta(days=2)
        two_days_deadline = two_days_before.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        # ポイント判定
        if registered_at <= seven_days_deadline:
            points = 200
            registration_type = "✓✓ 超早期登録(7日前)"
        elif registered_at <= two_days_deadline:
            points = 100
            registration_type = "✓ 事前登録(前々日)"
        else:
            points = 0
            registration_type = "✗ 当日登録"
        
        print(f"    参加ポイント判定 - イベント日: {event_date.strftime('%Y-%m-%d')}, "
            f"登録日時: {registered_at.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"      7日前締切: {seven_days_deadline.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"      前々日締切: {two_days_deadline.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"      → {registration_type}: +{points}P")
        
        return points

    def calculate_uguu_points(self, user_id):
        """
        うぐポイントを計算
        - 参加ポイント: 
        - 7日前までの登録: 200ポイント
        - 前々日の23:59までの登録: 100ポイント
        - それ以降: 0ポイント
        - 連続ポイント: 60日以内の連続参加で100ポイント
        - 月間ボーナス: 5/10/15/20回参加ごとにボーナス
        """
        try:
            from collections import defaultdict
            from datetime import datetime, timedelta
            
            print(f"\n[DEBUG] うぐポイント計算開始 - user_id: {user_id}")
            print("=" * 80)
            
            # 参加履歴を取得
            participation_history = self.get_user_participation_history_with_timestamp(user_id)
            
            if not participation_history:
                print(f"[DEBUG] 参加履歴なし - user_id: {user_id}")
                return {
                    'uguu_points': 0,
                    'participation_points': 0,
                    'streak_points': 0,
                    'monthly_bonus_points': 0,
                    'total_participation': 0,
                    'last_participation_date': None,
                    'current_streak_start': None,
                    'current_streak_count': 0,
                    'monthly_bonuses': {},
                    'early_registration_count': 0,
                    'super_early_registration_count': 0  # 7日前登録の回数
                }
            
            # 参加履歴を処理
            participation_records = []
            for record in participation_history:
                try:
                    event_date = datetime.strptime(record['event_date'], '%Y-%m-%d')
                    registered_at = datetime.strptime(record['registered_at'], '%Y-%m-%d %H:%M:%S')
                    
                    participation_records.append({
                        'event_date': event_date,
                        'registered_at': registered_at
                    })
                except (ValueError, KeyError) as e:
                    print(f"[WARN] 不正なレコード形式: {record}, エラー: {e}")
            
            # イベント日付順にソート
            participation_records.sort(key=lambda x: x['event_date'])
            
            # 総参加回数
            total_participation = len(participation_records)
            
            # ポイント内訳の初期化
            participation_points = 0
            streak_points = 0
            early_registration_count = 0  # 前々日までの登録回数
            super_early_registration_count = 0  # 7日前までの登録回数
            
            # 連続参加カウンター
            current_streak_start = participation_records[0]['event_date']
            current_streak_count = 1
            
            # 最初の参加の処理
            first_record = participation_records[0]
            first_points = self._is_early_registration(first_record)
            participation_points += first_points
            if first_points == 200:
                super_early_registration_count += 1
                early_registration_count += 1
            elif first_points == 100:
                early_registration_count += 1
            print(f"[DEBUG] 初回参加 - 参加ポイント: +{first_points}P")
            
            # 2回目以降の参加を処理
            for i in range(1, len(participation_records)):
                previous_record = participation_records[i - 1]
                current_record = participation_records[i]
                
                # 前回の参加日からの日数差を計算
                days_diff = (current_record['event_date'] - previous_record['event_date']).days
                
                print(f"\n[DEBUG] === 参加 {i+1}回目 ===")
                print(f"[DEBUG] 連続チェック - 前回: {previous_record['event_date'].strftime('%Y-%m-%d')}, "
                    f"今回: {current_record['event_date'].strftime('%Y-%m-%d')}, 日数差: {days_diff}日")
                
                # 参加ポイントの判定
                current_points = self._is_early_registration(current_record)
                participation_points += current_points
                if current_points == 200:
                    super_early_registration_count += 1
                    early_registration_count += 1
                elif current_points == 100:
                    early_registration_count += 1
                
                # 連続ポイントの判定
                if days_diff <= 60:
                    current_streak_count += 1
                    streak_points += 100
                    print(f"[DEBUG] 連続参加 - カウント: {current_streak_count}, 連続ポイント: +100P")
                else:
                    print(f"[DEBUG] 連続リセット - 60日超過: {days_diff}日, 連続ポイント: +0P")
                    current_streak_count = 1
                    current_streak_start = current_record['event_date']
            
            # 月間参加ボーナス計算
            monthly_participation = defaultdict(int)
            monthly_bonuses = {}
            monthly_bonus_points = 0
            
            for record in participation_records:
                month_key = record['event_date'].strftime('%Y-%m')
                monthly_participation[month_key] += 1
            
            print(f"\n[DEBUG] === 月間ボーナス計算 ===")
            for month, count in sorted(monthly_participation.items()):
                print(f"[DEBUG] 月別参加回数 - {month}: {count}回")
            
            for month, count in monthly_participation.items():
                monthly_bonuses[month] = {
                    'participation_count': count,
                    'bonus_points': 0
                }
                
                if count >= 5:
                    monthly_bonuses[month]['bonus_points'] += 500
                if count >= 10:
                    monthly_bonuses[month]['bonus_points'] += 1000
                if count >= 15:
                    monthly_bonuses[month]['bonus_points'] += 1500
                if count >= 20:
                    monthly_bonuses[month]['bonus_points'] += 2000
                
                if monthly_bonuses[month]['bonus_points'] > 0:
                    print(f"[DEBUG] {month} - {count}回参加 → ボーナス: {monthly_bonuses[month]['bonus_points']}P")
                
                monthly_bonus_points += monthly_bonuses[month]['bonus_points']
            
            # 総ポイント計算
            uguu_points = participation_points + streak_points + monthly_bonus_points
            
            result = {
                'uguu_points': uguu_points,
                'participation_points': participation_points,
                'streak_points': streak_points,
                'monthly_bonus_points': monthly_bonus_points,
                'total_participation': total_participation,
                'early_registration_count': early_registration_count,  # 前々日以前の登録
                'super_early_registration_count': super_early_registration_count,  # 7日前以前の登録
                'last_participation_date': participation_records[-1]['event_date'].strftime('%Y-%m-%d') if participation_records else None,
                'current_streak_start': current_streak_start.strftime('%Y-%m-%d') if current_streak_start else None,
                'current_streak_count': current_streak_count,
                'monthly_bonuses': monthly_bonuses
            }
            
            print(f"\n[DEBUG] === 最終結果 ===")
            print(f"  参加ポイント: {participation_points}P")
            print(f"    └ 7日前登録: {super_early_registration_count}回")
            print(f"    └ 前々日登録: {early_registration_count - super_early_registration_count}回")
            print(f"    └ 当日登録: {total_participation - early_registration_count}回")
            print(f"  連続ポイント: {streak_points}P")
            print(f"  月間ボーナス: {monthly_bonus_points}P")
            print(f"  ━━━━━━━━━━━━━━━━━━")
            print(f"  合計: {uguu_points}P")
            print("=" * 80)
            
            return result
            
        except Exception as e:
            print(f"[ERROR] うぐポイント計算エラー: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'uguu_points': 0,
                'participation_points': 0,
                'streak_points': 0,
                'monthly_bonus_points': 0,
                'total_participation': 0,
                'early_registration_count': 0,
                'super_early_registration_count': 0,
                'last_participation_date': None,
                'current_streak_start': None,
                'current_streak_count': 0,
                'monthly_bonuses': {}
            }
    
    # 参加履歴の書き込み（登録/更新時に呼ぶ。ビューからは呼ばない）
    def record_participation(self, date_str: str, schedule_id: str, participants: list[str]):
        tbl = self.dynamodb.Table("bad-users-history")
        for uid in set(participants or []):
            # すでに同日の記録があるか確認
            resp = tbl.query(
                KeyConditionExpression=Key("user_id").eq(uid),
                FilterExpression="#d = :d",
                ExpressionAttributeNames={"#d": "date"},
                ExpressionAttributeValues={":d": date_str}
            )
            if resp.get("Count", 0) > 0:
                continue  # 同日データがあればスキップ

            tbl.put_item(Item={
                "user_id": uid,
                "joined_at": datetime.utcnow().isoformat() + "Z",
                "date": date_str,
                "schedule_id": schedule_id
            })

    def get_user_participation_history(self, user_id: str):
        from datetime import datetime
        
        # 現在の日付
        today = datetime.now().strftime('%Y-%m-%d')
        
        items, resp = [], self.part_history.query(
            KeyConditionExpression=Key("user_id").eq(user_id)
        )
        items.extend(resp.get("Items", []))
        while "LastEvaluatedKey" in resp:
            resp = self.part_history.query(
                KeyConditionExpression=Key("user_id").eq(user_id),
                ExclusiveStartKey=resp["LastEvaluatedKey"]
            )
            items.extend(resp.get("Items", []))
        
        # 日付のセットを作成し、未来の日付を除外
        dates = sorted({it["date"] for it in items if "date" in it and it["date"] <= today})
        
        print(f"[DEBUG] 参加履歴取得完了 - user_id: {user_id}, 件数: {len(dates)}")
        for i, date in enumerate(dates):
            print(f"[DEBUG] 参加日 {i+1}: {date}")
        
        return dates  # 'YYYY-MM-DD' の文字列配列

    # プロフィール表示用：うぐポイント等の集計（履歴テーブルのみで計算）
    def get_user_stats(self, user_id: str):
        dates = self.get_user_participation_history(user_id)  # 昇順
        if not dates:
            return {
                "uguu_points": 0,
                "total_participation": 0,
                "last_participation_date": None,
                "current_streak_start": None,
                "current_streak_count": 0,
                "monthly_bonuses": {}
            }

        # 文字列→date
        d_objs = [datetime.strptime(d, "%Y-%m-%d").date() for d in dates]
        total = len(d_objs)

        # 連続ポイント（60日以内継続を同一ストリーク）
        points = 100
        streak_start = d_objs[0]
        streak_count = 1
        for prev, cur in zip(d_objs, d_objs[1:]):
            diff = (cur - prev).days
            if diff <= 60:
                streak_count += 1
                points += 200 if streak_count >= 2 else 100
            else:
                points += 100
                streak_start = cur
                streak_count = 1

        # 月間ボーナス
        monthly = defaultdict(int)
        for d in d_objs:
            monthly[d.strftime("%Y-%m")] += 1

        monthly_bonuses = {}
        for m, cnt in monthly.items():
            bonus = 1000 * sum(cnt >= t for t in (5, 10, 15, 20))
            monthly_bonuses[m] = {"participation_count": cnt, "bonus_points": bonus}
            points += bonus

        return {
            "uguu_points": points,
            "total_participation": total,
            "last_participation_date": d_objs[-1].strftime("%Y-%m-%d"),
            "current_streak_start": streak_start.strftime("%Y-%m-%d"),
            "current_streak_count": streak_count,
            "monthly_bonuses": dict(monthly_bonuses)
        }

# 重要: インスタンスを作成
db = DynamoDB()
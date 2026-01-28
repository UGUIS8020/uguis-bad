from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from uuid import uuid4
from dotenv import load_dotenv
from boto3.dynamodb.conditions import Key
from utils.points import record_spend
from typing import Any, Dict, List, Optional, Tuple
import json, base64
from decimal import Decimal
from botocore.exceptions import ClientError

load_dotenv()

import boto3
import os

def _encode_cursor(last_key: Optional[Dict[str, Any]]) -> Optional[str]:
    if not last_key:
        return None

    def conv(v):
        if isinstance(v, Decimal):
            return int(v) if v % 1 == 0 else float(v)
        return v

    safe = {k: conv(v) for k, v in last_key.items()}
    raw = json.dumps(safe, ensure_ascii=False).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")

def _decode_cursor(cursor: Optional[str]) -> Optional[Dict[str, Any]]:
    if not cursor:
        return None
    raw = base64.urlsafe_b64decode(cursor.encode("ascii"))
    return json.loads(raw.decode("utf-8"))


class DynamoDB:
    ALLOWED_EARN_TYPES = {"manual"}

    def __init__(self):
        self.dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION")
        )
        self.posts_table    = self.dynamodb.Table('uguu_post')
        self.users_table    = self.dynamodb.Table('bad-users')
        self.schedule_table = self.dynamodb.Table('bad_schedules')
        self.part_history   = self.dynamodb.Table("bad-users-history")
        
        self.replies_table  = self.dynamodb.Table("post-replies")

        print("DynamoDB tables initialized")

    def get_posts_page(
        self,
        limit: int = 10,
        cursor: Optional[str] = None,
        replies_limit: int = 5,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:

        eks = _decode_cursor(cursor)

        kwargs = dict(
            IndexName="gsi_feed",
            KeyConditionExpression=Key("feed_pk").eq("FEED"),
            ScanIndexForward=False,
            Limit=limit,
        )
        if eks:
            kwargs["ExclusiveStartKey"] = eks

        res = self.posts_table.query(**kwargs)
        items = res.get("Items", [])
        next_cursor = _encode_cursor(res.get("LastEvaluatedKey"))

        enriched_posts: List[Dict[str, Any]] = []

        for post in items:
            # post_id の決定
            post_id = post.get("post_id")
            if not post_id:
                pk = post.get("PK", "")
                if isinstance(pk, str) and pk.startswith("POST#"):
                    post_id = pk.split("#", 1)[1]

            user_id = post.get("user_id")

            p: Dict[str, Any] = {
                "post_id": post_id,
                "content": post.get("content"),
                "image_url": post.get("image_url"),
                "youtube_url": post.get("youtube_url"),
                "created_at": post.get("created_at"),
                "updated_at": post.get("updated_at", post.get("created_at")),
                "user_id": user_id,
                "display_name": post.get("display_name"),
                "user_name": post.get("user_name"),
            }

            # 返信
            if post_id:
                try:
                    rr = self.replies_table.query(
                        KeyConditionExpression=Key("post_id").eq(str(post_id)) & Key("sk").begins_with("REPLY#"),
                        ScanIndexForward=False,
                        Limit=replies_limit,
                    )
                    replies_items = rr.get("Items", [])

                    cr = self.replies_table.query(
                        KeyConditionExpression=Key("post_id").eq(str(post_id)) & Key("sk").begins_with("REPLY#"),
                        Select="COUNT",
                    )
                    replies_count = int(cr.get("Count", 0))

                    p["replies"] = [
                        {
                            "reply_id": it.get("reply_id") or (
                                it.get("sk", "").split("#", 1)[1]
                                if str(it.get("sk", "")).startswith("REPLY#")
                                else None
                            ),
                            "post_id": str(post_id),
                            "user_id": it.get("user_id"),
                            "content": it.get("content"),
                            "created_at": it.get("created_at"),
                            "display_name": it.get("display_name"),
                            "profile_image_url": it.get("profile_image_url"),
                        }
                        for it in replies_items
                    ]
                    p["replies_count"] = replies_count

                except Exception as e:
                    print("[DEBUG replies] error:", e)
                    p["replies"] = []
                    p["replies_count"] = 0
            else:
                p["replies"] = []
                p["replies_count"] = 0

            # ★これが必要（作ったpをリストに入れる）
            enriched_posts.append(p)

        # ★これが必要（必ずタプルで返す）
        return enriched_posts, next_cursor


    def get_post(self, post_id: str) -> Optional[Dict[str, Any]]:
        """投稿メタデータ1件を取得（uguu_post から）"""
        res = self.posts_table.get_item(
            Key={"PK": f"POST#{post_id}", "SK": f"METADATA#{post_id}"}
        )
        return res.get("Item")
    

    def create_post(self, user_id, content, image_url=None, youtube_url=None):
        post_id = str(uuid4())
        now = datetime.now(timezone.utc).isoformat()

        post = {
            "PK": f"POST#{post_id}",
            "SK": f"METADATA#{post_id}",
            "post_id": post_id,
            "user_id": user_id,
            "content": content,
            "image_url": image_url,
            "youtube_url": youtube_url,
            "created_at": now,
            "updated_at": now,            
            "feed_pk": "FEED",
            "feed_sk": f"TS#{now}#POST#{post_id}",
        }

        self.posts_table.put_item(Item=post)
        return post

    def update_post_fields(self, post_id: str, content: str = None, image_url=None):
        now = datetime.now().isoformat()

        key = {"PK": f"POST#{post_id}", "SK": f"METADATA#{post_id}"}

        updates = []
        values = {}
        names = {}

        # content 更新
        if content is not None:
            updates.append("#content = :content")
            names["#content"] = "content"
            values[":content"] = content

            # ✅ content編集したらフィード順も更新（上に上げる）
            updates.append("#updated_at = :now")
            updates.append("#feed_ts = :now")
            names["#updated_at"] = "updated_at"
            names["#feed_ts"] = "feed_ts"
            values[":now"] = now

        # image_url 更新（None も許容：削除）
        if image_url is not None:
            updates.append("#image_url = :image_url")
            names["#image_url"] = "image_url"
            values[":image_url"] = image_url

            # 画像変更でも上げたいならここでも feed_ts 更新してOK
            if ":now" not in values:
                updates.append("#updated_at = :now")
                updates.append("#feed_ts = :now")
                names["#updated_at"] = "updated_at"
                names["#feed_ts"] = "feed_ts"
                values[":now"] = now

        if not updates:
            return False

        self.posts_table.update_item(
            Key=key,
            UpdateExpression="SET " + ", ".join(updates),
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )
        return True


    def update_post(self, post_id, content):
        """投稿を更新（フィードの並びも更新＝上に上がる）"""
        timestamp = datetime.now(timezone.utc).isoformat()

        self.posts_table.update_item(
            Key={
                "PK": f"POST#{post_id}",
                "SK": f"METADATA#{post_id}",
            },
            UpdateExpression="SET content = :content, updated_at = :updated_at, feed_ts = :feed_ts",
            ExpressionAttributeValues={
                ":content": content,
                ":updated_at": timestamp,
                ":feed_ts": timestamp,
            },
            ReturnValues="NONE",
        )
        return True

    # def delete_post(self, post_id):
    #     """投稿を削除"""
    #     try:
    #         # 投稿を検索して実際のキー構造を確認
    #         response = self.posts_table.scan()
    #         posts = response.get('Items', [])
            
    #         target_post = None
    #         for post in posts:
    #             if str(post.get('post_id')) == str(post_id):
    #                 target_post = post
    #                 break
            
    #         if not target_post:
    #             raise Exception(f"Post {post_id} not found")
            
    #         # 実際のキー構造に基づいて削除
    #         if 'PK' in target_post and 'SK' in target_post:
    #             # PK/SK構造の場合
    #             delete_key = {
    #                 'PK': target_post['PK'],
    #                 'SK': target_post['SK']
    #             }
    #         else:
    #             # post_id直接の場合
    #             delete_key = {
    #                 'post_id': post_id
    #             }
            
    #         response = self.posts_table.delete_item(Key=delete_key)
    #         return response
            
    #     except Exception as e:
    #         raise Exception(f"投稿削除エラー: {str(e)}")
        
    def delete_post(self, post_id: str):
        """投稿(METADATA) + 返信(全件)を削除"""
        pk = f"POST#{post_id}"

        # 1) 返信を全削除（replies_table）
        try:
            last_evaluated_key = None
            with self.replies_table.batch_writer() as batch:
                while True:
                    kwargs = {
                        # ✅ replies は post_id / sk がキー
                        "KeyConditionExpression": Key("post_id").eq(str(post_id)) & Key("sk").begins_with("REPLY#"),
                        "ProjectionExpression": "post_id, sk",
                    }
                    if last_evaluated_key:
                        kwargs["ExclusiveStartKey"] = last_evaluated_key

                    res = self.replies_table.query(**kwargs)
                    for it in res.get("Items", []):
                        batch.delete_item(Key={"post_id": it["post_id"], "sk": it["sk"]})

                    last_evaluated_key = res.get("LastEvaluatedKey")
                    if not last_evaluated_key:
                        break
        except Exception as e:
            raise Exception(f"返信削除エラー: {e}")

        # 2) 投稿(METADATA)削除（posts_table）
        try:
            return self.posts_table.delete_item(
                Key={"PK": pk, "SK": f"METADATA#{post_id}"},
                ConditionExpression="attribute_exists(PK) AND attribute_exists(SK)",
            )
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code == "ConditionalCheckFailedException":
                raise Exception(f"Post {post_id} not found")
            raise Exception(f"投稿削除エラー: {e}")

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

    def delete_post_replies(self, post_id: str) -> bool:
        """投稿に関連する返信を削除（post_id + sk 版）"""
        try:
            print(f"Deleting replies for post: {post_id}")
            deleted = 0
            last_evaluated_key = None

            with self.replies_table.batch_writer() as batch:
                while True:
                    kwargs = {
                        "KeyConditionExpression": Key("post_id").eq(str(post_id)) & Key("sk").begins_with("REPLY#"),
                    }
                    if last_evaluated_key:
                        kwargs["ExclusiveStartKey"] = last_evaluated_key

                    res = self.replies_table.query(**kwargs)
                    items = res.get("Items", [])

                    for it in items:
                        batch.delete_item(Key={"post_id": it["post_id"], "sk": it["sk"]})
                        deleted += 1

                    last_evaluated_key = res.get("LastEvaluatedKey")
                    if not last_evaluated_key:
                        break

            print(f"Deleted {deleted} replies")
            return True

        except Exception as e:
            print(f"返信削除エラー: {str(e)}")
            return False
        
    def delete_reply(self, post_id: str, reply_id: str) -> bool:
        """返信1件を削除（sk = REPLY#<reply_id> 前提）"""
        try:
            sk = f"REPLY#{reply_id}"
            self.replies_table.delete_item(
                Key={"post_id": str(post_id), "sk": sk}
            )
            return True
        except Exception as e:
            print("[delete_reply] error:", e)
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
    
    def get_user_by_id(self, user_id: str) -> dict | None:
        """ユーザー情報を取得（表示用に最低限整形）"""
        try:
            if not user_id:
                return None

            res = self.users_table.get_item(Key={"user#user_id": user_id})
            u = res.get("Item")
            if not u:
                return None

            # 表示名のフォールバック
            display_name = (u.get("display_name") or "").strip() or "不明"
            user_name    = (u.get("user_name") or "").strip()

            # 画像URLのフォールバック（あなたの既存ロジックに合わせる）
            url = (
                u.get("profile_image_url")
                or u.get("profileImageUrl")
                or u.get("large_image_url")
                or ""
            )
            url = url.strip() if isinstance(url, str) else ""
            profile_image_url = url if url and url.lower() != "none" else None

            u["display_name"] = display_name
            u["user_name"] = user_name
            u["profile_image_url"] = profile_image_url

            return u

        except Exception as e:
            print(f"Error getting user by id: {e}")
            import traceback
            print(traceback.format_exc())
            return None

    def get_posts_by_user(
        self,
        user_id: str,
        limit: int = 20,
        cursor: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:

        eks = _decode_cursor(cursor)

        kwargs = dict(
            IndexName="gsi_user_posts",
            KeyConditionExpression=Key("user_id").eq(user_id),
            ScanIndexForward=False,  # 新しい順
            Limit=limit,
        )
        if eks:
            kwargs["ExclusiveStartKey"] = eks

        res = self.posts_table.query(**kwargs)
        items = res.get("Items", [])
        next_cursor = _encode_cursor(res.get("LastEvaluatedKey"))

        posts = []
        for post in items:
            post_id = post.get("post_id")
            if not post_id:
                pk = post.get("PK", "")
                if pk.startswith("POST#"):
                    post_id = pk.split("#", 1)[1]

            posts.append({
                "post_id": post_id,
                "content": post.get("content"),
                "image_url": post.get("image_url"),
                "youtube_url": post.get("youtube_url"),
                "created_at": post.get("created_at"),
                "updated_at": post.get("updated_at", post.get("created_at")),
                "user_id": post.get("user_id"),
                "likes_count": post.get("likes_count", 0),
                "replies_count": post.get("replies_count", 0),
            })

        return posts, next_cursor
        
    def get_user_posts(self, user_id):
        """特定ユーザーの投稿を取得（get_posts_by_userのエイリアス）"""
        return self.get_posts_by_user(user_id)
    
    def get_post_replies(self, post_id):
        """投稿の返信を取得"""
        try:
            print(f"[DEBUG] 返信取得: post_id={post_id}")
            
            response = self.posts_table.query(
                KeyConditionExpression="PK = :pk AND begins_with(SK, :reply)",
                ExpressionAttributeValues={
                    ':pk': f"POST#{post_id}",
                    ':reply': 'REPLY#'
                },
                ScanIndexForward=True  # 古い順（時系列）
            )
            
            replies = response.get('Items', [])
            print(f"[DEBUG] 取得した返信数: {len(replies)}件")
            
            # 作成日時でソート
            return sorted(replies, key=lambda x: x.get('created_at', ''))
            
        except Exception as e:
            print(f"[ERROR] 返信取得エラー: {str(e)}")
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
    
    def cancel_participation(self, user_id: str, date: str, schedule_id: str = None):
        """参加をキャンセル - 該当する全レコードを更新"""
        try:
            from boto3.dynamodb.conditions import Key
            
            # user_idで検索し、該当するすべてのレコードを取得
            response = self.part_history.query(
                KeyConditionExpression=Key('user_id').eq(user_id)
            )
            
            items = response.get('Items', [])
            updated_count = 0
            
            # dateが一致するレコードをすべて更新（schedule_idがあればそれも確認）
            for item in items:
                date_match = item.get('date') == date
                schedule_match = (schedule_id is None) or (item.get('schedule_id') == schedule_id)
                
                if date_match and schedule_match:
                    # joined_atをソートキーとして使用
                    self.part_history.update_item(
                        Key={
                            'user_id': user_id,
                            'joined_at': item['joined_at']  # ★ ソートキーを使用
                        },
                        UpdateExpression='SET #status = :s',
                        ExpressionAttributeNames={
                            '#status': 'status'
                        },
                        ExpressionAttributeValues={
                            ':s': 'cancelled'
                        }
                    )
                    updated_count += 1
                    print(f"[INFO] 更新: user_id={user_id}, joined_at={item['joined_at']}, date={date}")
            
            if updated_count > 0:
                print(f"[INFO] キャンセル成功: user_id={user_id}, date={date}, schedule_id={schedule_id}, 更新件数={updated_count}")
            else:
                print(f"[WARNING] 該当レコードなし: user_id={user_id}, date={date}, schedule_id={schedule_id}")
            
            return True
            
        except Exception as e:
            print(f"[ERROR] 参加キャンセルエラー: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
        
    def get_user_participation_history_with_timestamp(self, user_id):
        """
        参加履歴をタイムスタンプ付きで取得（重複除外）
        同じ日付が複数ある場合は、最も遅い登録時刻を採用（再参加を反映）
        """
        from datetime import datetime
        from boto3.dynamodb.conditions import Key
        
        try:
            print(f"\n[DEBUG] ========================================")
            print(f"[DEBUG] タイムスタンプ付き参加履歴取得開始")
            print(f"[DEBUG] user_id: {user_id}")
            print(f"[DEBUG] ========================================")
            
            items = []
            resp = self.part_history.query(
                KeyConditionExpression=Key("user_id").eq(user_id)
            )
            items.extend(resp.get("Items", []))
            
            while "LastEvaluatedKey" in resp:
                resp = self.part_history.query(
                    KeyConditionExpression=Key("user_id").eq(user_id),
                    ExclusiveStartKey=resp["LastEvaluatedKey"]
                )
                items.extend(resp.get("Items", []))
            
            print(f"[DEBUG] DynamoDBから取得した生レコード数: {len(items)}件")
            
            # 現在の日付（未来の参加を除外）
            today = datetime.now().date()
            print(f"[DEBUG] 今日の日付: {today}")
            
            # 日付ごとの最も遅い登録時刻を保持する辞書
            date_records = {}
            
            # 統計用カウンター
            cancelled_count = 0
            future_count = 0
            parse_error_count = 0
            processed_count = 0
            
            for idx, item in enumerate(items, 1):
                print(f"\n[DEBUG] --- レコード {idx}/{len(items)} ---")
                print(f"[DEBUG] 生データ: {item}")
                
                try:
                    # statusフィールドの確認
                    status = item.get("status", "未設定")
                    print(f"[DEBUG] status: {status}")
                    
                    # キャンセル済みは除外
                    if item.get("status") == "cancelled":
                        print(f"[DEBUG] ✗ キャンセル済みをスキップ: {item.get('date')}")
                        cancelled_count += 1
                        continue
                    
                    # 必要なフィールドの確認
                    if "date" not in item:
                        print(f"[DEBUG] ✗ dateフィールドなし")
                        parse_error_count += 1
                        continue
                        
                    if "joined_at" not in item:
                        print(f"[DEBUG] ✗ joined_atフィールドなし")
                        parse_error_count += 1
                        continue
                    
                    event_date_str = item["date"]
                    print(f"[DEBUG] event_date: {event_date_str}")
                    
                    event_date = datetime.strptime(event_date_str, "%Y-%m-%d").date()
                    
                    # 未来の日付は除外
                    if event_date > today:
                        print(f"[DEBUG] ✗ 未来の日付をスキップ: {event_date_str}")
                        future_count += 1
                        continue
                    
                    # joined_at のパース
                    joined_at_str = item["joined_at"]
                    print(f"[DEBUG] joined_at(元): {joined_at_str}")
                    
                    if joined_at_str.endswith("Z"):
                        joined_at_str = joined_at_str[:-1]
                    
                    try:
                        registered_at = datetime.fromisoformat(joined_at_str)
                        print(f"[DEBUG] registered_at(変換後): {registered_at.strftime('%Y-%m-%d %H:%M:%S')}")
                    except ValueError as ve:
                        print(f"[WARN] ✗ joined_at パースエラー: {joined_at_str} - {ve}")
                        parse_error_count += 1
                        continue
                    
                    # 同じ日付の場合、より遅い登録時刻を採用（再参加を反映）
                    if event_date_str not in date_records:
                        date_records[event_date_str] = {
                            'event_date': event_date_str,
                            'registered_at': registered_at.strftime('%Y-%m-%d %H:%M:%S')
                        }
                        print(f"[DEBUG] ✓ 新規参加記録として登録: {event_date_str} - {registered_at.strftime('%H:%M:%S')}")
                        processed_count += 1
                    else:
                        # 既存のレコードより遅い登録時刻なら更新（再参加）
                        existing_registered_at = datetime.strptime(
                            date_records[event_date_str]['registered_at'], 
                            '%Y-%m-%d %H:%M:%S'
                        )
                        print(f"[DEBUG] 既存レコードと比較: 既存={existing_registered_at.strftime('%H:%M:%S')}, 新={registered_at.strftime('%H:%M:%S')}")
                        
                        if registered_at > existing_registered_at:
                            print(f"[DEBUG] ✓ 再参加検出（より遅い時刻に更新）: {event_date_str}")
                            print(f"[DEBUG]   {existing_registered_at.strftime('%H:%M:%S')} → {registered_at.strftime('%H:%M:%S')}")
                            date_records[event_date_str]['registered_at'] = registered_at.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            print(f"[DEBUG] ✗ 古い参加記録をスキップ: {event_date_str} - {registered_at.strftime('%H:%M:%S')}")
                    
                except Exception as e:
                    print(f"[WARN] ✗ レコード処理エラー: {e}")
                    import traceback
                    traceback.print_exc()
                    parse_error_count += 1
                    continue
            
            # リストに変換してソート
            records = sorted(date_records.values(), key=lambda x: x['event_date'])
            
            print(f"\n[DEBUG] ========================================")
            print(f"[DEBUG] 処理結果サマリー")
            print(f"[DEBUG] ========================================")
            print(f"[DEBUG] 生レコード総数: {len(items)}件")
            print(f"[DEBUG] キャンセル済み: {cancelled_count}件")
            print(f"[DEBUG] 未来の日付: {future_count}件")
            print(f"[DEBUG] パースエラー: {parse_error_count}件")
            print(f"[DEBUG] 新規登録: {processed_count}件")
            print(f"[DEBUG] 重複除外後: {len(records)}件")
            print(f"[DEBUG] ========================================\n")
            
            return records
            
        except Exception as e:
            print(f"[ERROR] get_user_participation_history_with_timestamp エラー: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
    
    def _is_early_registration(self, record):
        """
        事前参加の判定とポイント計算（2パターン版）

        新ルール:
        - 〜3日前まで: 100点（3日前 23:59:59 まで）
        - 2日前〜前日まで: 50点（前日 23:59:59 まで）
        - 当日: 0点（当日 0:00:00 以降）

        Returns:
            int: 参加ポイント (0, 50, 100)
        """
        from datetime import timedelta

        event_date = record["event_date"]        # datetime
        registered_at = record["registered_at"]  # datetime

        # 3日前の23:59:59
        three_days_before = event_date - timedelta(days=3)
        three_days_deadline = three_days_before.replace(
            hour=23, minute=59, second=59, microsecond=999999
        )

        # 前日の23:59:59（= 2日前〜前日までの締切）
        one_day_before = event_date - timedelta(days=1)
        one_day_deadline = one_day_before.replace(
            hour=23, minute=59, second=59, microsecond=999999
        )

        # ポイント判定（2段階）
        if registered_at <= three_days_deadline:
            points = 100
            registration_type = "✓ 早期登録(〜3日前)"
        elif registered_at <= one_day_deadline:
            points = 50
            registration_type = "✓ 直前登録(2日前〜前日)"
        else:
            points = 0
            registration_type = "✗ 当日登録"

        print(
            f"    参加ポイント判定 - イベント日: {event_date:%Y-%m-%d}, "
            f"登録日時: {registered_at:%Y-%m-%d %H:%M:%S}"
        )
        print(f"      3日前締切: {three_days_deadline:%Y-%m-%d %H:%M:%S}")
        print(f"      前日締切: {one_day_deadline:%Y-%m-%d %H:%M:%S}")
        print(f"      → {registration_type}: +{points}P")

        return points   
    
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
        
        # ↓↓↓ ここを修正：キャンセル済みを除外 ↓↓↓
        valid_dates = []
        for it in items:
            if "date" not in it:
                continue
            
            # キャンセル済みは除外
            if it.get("status") == "cancelled":
                print(f"[DEBUG] キャンセル済みの参加をスキップ: {it['date']}")
                continue
            
            # 未来の日付は除外
            if it["date"] <= today:
                valid_dates.append(it["date"])
        
        # 重複を削除してソート
        dates = sorted(set(valid_dates))
        # ↑↑↑ ここまで修正 ↑↑↑
        
        print(f"[DEBUG] 参加履歴取得完了 - user_id: {user_id}, 件数: {len(dates)}")
        for i, date in enumerate(dates):
            print(f"[DEBUG] 参加日 {i+1}: {date}")
        
        return dates  # 'YYYY-MM-DD' の文字列配列
    
    def get_all_past_schedules(self, until_date):
        """
        過去の全スケジュールを日付順に取得
        
        Args:
            until_date: この日付までのスケジュールを取得（datetime.date）
        
        Returns:
            [{'date': 'YYYY-MM-DD', 'participants': [...]}, ...]
        """
        try:
            from datetime import datetime
            
            response = self.schedule_table.scan()
            schedules = response.get('Items', [])
            
            while 'LastEvaluatedKey' in response:
                response = self.schedule_table.scan(
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                schedules.extend(response.get('Items', []))
            
            # 過去のスケジュールのみフィルタしてソート
            past_schedules = []
            for schedule in schedules:
                try:
                    schedule_date = datetime.strptime(schedule['date'], '%Y-%m-%d').date()
                    if schedule_date <= until_date:
                        past_schedules.append({
                            'date': schedule['date'],
                            'participants': schedule.get('participants', [])
                        })
                except Exception as e:
                    print(f"[WARN] スケジュール処理エラー: {schedule} - {e}")
                    continue
            
            # 日付順にソート
            past_schedules.sort(key=lambda x: x['date'])
            
            print(f"[DEBUG] 過去のスケジュール取得: {len(past_schedules)}件")
            return past_schedules
            
        except Exception as e:
            print(f"[ERROR] スケジュール取得エラー: {str(e)}")
            return []
        
    def _is_junior_high_student(self, user_info):
        """
        生年月日から中学生以下かどうかを判定
        日本の学年は4月1日基準
        生年月日がない場合は対象外とみなす
        """
        # ユーザー情報がない、または生年月日がない場合は対象外
        if not user_info or not user_info.get('birth_date'):
            print(f"[DEBUG] 生年月日なし → ポイント半減対象外として扱う")
            return False
        
        try:
            from datetime import datetime
            
            birth_date = user_info['birth_date']
            
            # 文字列の場合はdatetimeに変換
            if isinstance(birth_date, str):
                birth_date = datetime.strptime(birth_date, '%Y-%m-%d')
            
            today = datetime.now()
            
            # 年齢を計算
            age = today.year - birth_date.year
            
            # 誕生日前なら-1
            if (today.month, today.day) < (birth_date.month, birth_date.day):
                age -= 1
            
            # 学年を計算（4月1日基準）
            # 4月1日以前なら、前の学年
            if today.month < 4 or (today.month == 4 and today.day == 1):
                school_year_age = age - 1
            else:
                school_year_age = age
            
            # 中学生以下は14歳以下
            # 小学生：6～11歳、中学生：12～14歳
            is_junior_high_or_below = school_year_age <= 14
            
            grade_info = ""
            if 6 <= school_year_age <= 11:
                grade_info = f"(小学{school_year_age - 5}年相当)"
            elif 12 <= school_year_age <= 14:
                grade_info = f"(中学{school_year_age - 11}年相当)"
            elif school_year_age < 6:
                grade_info = "(未就学)"
            
            print(f"[DEBUG] 生年月日: {birth_date.strftime('%Y-%m-%d')}, 年齢: {age}歳, 学年年齢: {school_year_age}歳{grade_info}, 中学生以下: {is_junior_high_or_below}")
            
            return is_junior_high_or_below
            
        except Exception as e:
            print(f"[WARN] 中学生以下判定エラー: {str(e)} → ポイント半減対象外として扱う")
            return False
        
    def get_user_info(self, user_id: str):
        """
        ユーザー情報を取得（生年月日を含む）
        """
        try:
            response = self.users_table.get_item(  # self.table → self.users_table に変更
                Key={'user#user_id': user_id}
            )
            
            if 'Item' not in response:
                print(f"[WARN] ユーザー情報が見つかりません - user_id: {user_id}")
                return None
            
            item = response['Item']
            birth_date = item.get('date_of_birth', None)
            
            user_info = {
                'user_id': user_id,
                'birth_date': birth_date,
                'display_name': item.get('display_name', ''),
                'skill_score': item.get('skill_score', 0)
            }
            
            print(f"[DEBUG] ユーザー情報取得 - user_id: {user_id}, birth_date: {birth_date}")
            return user_info
            
        except Exception as e:
            print(f"[ERROR] ユーザー情報取得エラー: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
            

    def get_upcoming_schedules(self, limit: int = 10, today_only: bool = False):
        """今後の予定を取得（today_only=True で当日の最初の1件だけ）"""
        try:
            # === タイムゾーン設定 ===
            JST = ZoneInfo("Asia/Tokyo")
            today_date = datetime.now(JST).date()
            today_str = today_date.strftime('%Y-%m-%d')
            print(f"[DEBUG] get_upcoming_schedules - today(JST): {today_str}")

            # === スケジュール取得 ===
            response = self.schedule_table.scan()
            schedules = response.get('Items', [])

            while 'LastEvaluatedKey' in response:
                response = self.schedule_table.scan(
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                schedules.extend(response.get('Items', []))

            print(f"[DEBUG] 全スケジュール取得: {len(schedules)}件")

            upcoming = []
            for schedule in schedules:
                try:
                    s_date = datetime.strptime(schedule['date'], '%Y-%m-%d').date()

                    cond = (s_date == today_date) if today_only else (s_date >= today_date)
                    if cond:
                        item = {
                            'schedule_id': schedule.get('schedule_id'),
                            'date': schedule['date'],
                            'day_of_week': schedule.get('day_of_week', ''),
                            'start_time': schedule.get('start_time', ''),
                            'end_time': schedule.get('end_time', '')
                        }
                        upcoming.append(item)
                        print(f"[DEBUG] 対象の予定: {item['date']} {item.get('start_time','')}")
                except Exception as e:
                    print(f"[WARN] スケジュール処理エラー: {schedule} - {e}")
                    continue

            # === 当日のみ ===
            if today_only:
                if not upcoming:
                    print("[DEBUG] 本日の予定なし")
                    return []
                upcoming.sort(key=lambda x: x.get('start_time', '00:00'))
                result = [upcoming[0]]
                print(f"[DEBUG] 本日の予定（1件）: {result[0]['date']} {result[0].get('start_time','')}")
                return result

            # === 未来分 ===
            upcoming.sort(key=lambda x: (x['date'], x.get('start_time', '')))
            result = upcoming[:limit]
            print(f"[DEBUG] 今後の予定（{limit}件まで）: {len(result)}件")
            return result

        except Exception as e:
            print(f"[ERROR] 今後の予定取得エラー: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
        

    def get_user_payment_history(self, user_id: str):
        """
        ユーザーのポイント支払い履歴を取得
        """
        return self.get_point_transactions(user_id, transaction_type='payment') 

        
    # def record_spend(history_table, *, user_id: str, points_used: int,
    #              event_date: str, payment_type: str = "event_participation",
    #              reason: str | None = None, created_by: str | None = None):
    #     """
    #     bad-users-history に消費(spend)トランザクションを1件保存する。
    #     """
    #     now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    #     tx_id = str(uuid4())
    #     joined_at = f"points#spend#{now}#{tx_id}"

    #     item = {
    #         "user_id": user_id,
    #         "joined_at": joined_at,
    #         "tx_id": tx_id,
    #         "kind": "spend",                     # earn|spend|adjust
    #         "delta_points": -int(points_used),   # 残高計算用（負）
    #         "points_used": int(points_used),     # 互換のため（正）
    #         "payment_type": payment_type,
    #         "event_date": event_date,
    #         "reason": reason or f"{event_date}の参加費",
    #         "created_at": now,
    #         "entity_type": "point_transaction",
    #         "version": 1,
    #     }

    #     # 一意化（同じPK/SKの重複防止）
    #     history_table.put_item(
    #         Item=item,
    #         ConditionExpression="attribute_not_exists(user_id) AND attribute_not_exists(joined_at)"
    #     )
    #     return item
        
    # def record_point_earned(self, user_id: str, event_date: str,
    #                     points: int, earn_type: str,
    #                     details: dict = None, description: str = None):
    #     """ポイント獲得を bad-users-history に記録（earn系は正の delta_points）"""
    #     try:
    #         now = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
    #         tx_id = str(uuid4())
    #         joined_at = f'points#earn#{now}#{tx_id}'

    #         item = {
    #             'user_id': user_id,          # PK
    #             'joined_at': joined_at,      # SK
    #             'tx_id': tx_id,
    #             'kind': 'earn',              # earn|spend|adjust
    #             'delta_points': int(points), # 正の値
    #             'points': int(points),       # 互換フィールド（任意）
    #             'earn_type': earn_type,      # participation/streak/milestone/monthly 等
    #             'event_date': event_date,
    #             'details': details or {},
    #             'reason': description or "ポイント獲得",
    #             'created_at': now,
    #             'entity_type': 'point_transaction',
    #             'version': 1,
    #         }

    #         self.part_history.put_item(
    #             Item=item,
    #             ConditionExpression='attribute_not_exists(user_id) AND attribute_not_exists(joined_at)'
    #         )
    #         print(f"[SUCCESS] 獲得記録保存 - user_id={user_id}, +{points}P, type={earn_type}")
    #         return True

    #     except Exception as e:
    #         print(f"[ERROR] 獲得記録エラー: {e}")
    #         import traceback; traceback.print_exc()
    #         return False       
    

    def record_point_earned(
        self,
        user_id: str,
        event_date: str,
        points: int,
        earn_type: str = "manual",
        details: Optional[Dict[str, Any]] = None,
        description: Optional[str] = None,
        *,
        source: str = "admin_manual",
        created_by: Optional[str] = None,
    ) -> bool:
        """
        ポイント獲得を bad-users-history に記録する。

        運用方針（重要）:
        - 仕様が固まるまで「参加/連続/月間など」は get_user_stats() で再計算する。
        - ledger には「管理人付与(manual)」など、外部入力だけを記録する。
            → 二重計上防止のため、earn_type が manual 以外は保存しない。

        保存形式:
        - joined_at: points#earn#{ISO8601}#{uuid}
        - kind: earn
        - delta_points: 正の値
        - source: admin_manual（get_manual_points が拾えるように）
        """
        try:
            # ---- 入力バリデーション ----
            if not user_id or not str(user_id).strip():
                raise ValueError("user_id is required")

            earn_type_norm = (earn_type or "").strip().lower()
            if earn_type_norm not in self.ALLOWED_EARN_TYPES:
                raise ValueError(
                    f"earn_type='{earn_type}' is not allowed while running in recompute mode. "
                    f"Allowed: {sorted(self.ALLOWED_EARN_TYPES)}"
                )

            pts = int(points)
            if pts <= 0:
                raise ValueError("points must be a positive integer for earn")

            if not event_date:
                raise ValueError("event_date is required (YYYY-MM-DD)")
            event_date_10 = str(event_date).strip()[:10]
            # YYYY-MM-DD 形式チェック
            datetime.strptime(event_date_10, "%Y-%m-%d")

            now_iso = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            tx_id = str(uuid4())
            joined_at = f"points#earn#{now_iso}#{tx_id}"

            item: Dict[str, Any] = {
                "user_id": str(user_id),
                "joined_at": joined_at,
                "tx_id": tx_id,
                "kind": "earn",
                "delta_points": pts,       # 正
                "points": pts,             # 互換
                "earn_type": earn_type_norm,   # manual
                "source": source,              # admin_manual を推奨
                "created_by": created_by,
                "event_date": event_date_10,
                "details": details or {},
                "reason": (description or "管理人付与").strip(),
                "created_at": now_iso,
                "entity_type": "point_transaction",
                "version": 1,
            }

            self.part_history.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(user_id) AND attribute_not_exists(joined_at)",
            )

            print(f"[SUCCESS] earn保存 - user_id={user_id}, +{pts}P, earn_type={earn_type_norm}, source={source}")
            return True

        except Exception as e:
            print(f"[ERROR] earn記録エラー: {e}")
            import traceback
            traceback.print_exc()
            return False
        
    def record_payment(self, user_id: str, event_date: str, points_used: int,
                    payment_type: str = "event_participation", description: str = None):
        try:
            record_spend(
                self.part_history,
                user_id=user_id,
                points_used=points_used,
                event_date=event_date,
                payment_type=payment_type,
                reason=description,
                created_by=None
            )
            print(f"[SUCCESS] 支払い記録保存 - user_id={user_id}, event_date={event_date}, points={points_used}P")
            return True
        except Exception as e:
            print(f"[ERROR] 支払い記録エラー: {e}")
            import traceback; traceback.print_exc()
            return False
        
    # def get_point_transactions(self, user_id: str, limit: int = 50, transaction_type: str = None):
    #     try:
    #         prefix = 'points#spend#' if (transaction_type in (None, 'payment', 'spend')) else 'points#'
    #         resp = self.part_history.query(
    #             KeyConditionExpression=Key('user_id').eq(user_id) & Key('joined_at').begins_with(prefix),
    #             ScanIndexForward=False,
    #             Limit=limit,
    #             ConsistentRead=True,  # 直前の書き込みを確実に拾う
    #         )

    #         txs = []
    #         for it in resp.get('Items', []):
    #             if not it.get('joined_at','').startswith('points#spend#'):
    #                 continue
    #             used = int(it.get('points_used') or -int(it.get('delta_points', 0)) or 0)
    #             txs.append({
    #                 'date': it.get('created_at'),        # 支払日時
    #                 'type': 'payment',
    #                 'points_used': used,                  # ← 集計はこのキーに寄せる
    #                 'points': used,                       # 互換のため残す（UIで使用可）
    #                 'delta_points': int(it.get('delta_points', 0)),  # -800 など
    #                 'description': it.get('reason', ''),
    #                 'event_date': it.get('event_date'),
    #                 'payment_type': it.get('payment_type'),
    #                 'tx_id': it.get('tx_id'),
    #             })

    #         print(f"[DEBUG] 取引履歴取得 - user_id: {user_id}, 件数: {len(txs)}, type=payment")
    #         return txs

    #     except Exception as e:
    #         print(f"[ERROR] 取引履歴取得エラー: {e}")
    #         import traceback; traceback.print_exc()
    #         return []
        
    
    def get_point_transactions(
        self,
        user_id: str,
        limit: int = 50,
        transaction_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        bad-users-history からポイント取引履歴を取得して整形して返す。

        transaction_type:
        - None / "all"      : earn, spend, adjust を全部返す
        - "spend" / "payment": spend だけ返す
        - "earn"            : earn だけ返す
        - "adjust"          : adjust だけ返す
        """
        try:
            # ---- type 正規化 ----
            t = (transaction_type or "spend").strip().lower()
            if t in ("payment", "spend"):
                kind_filter = {"spend"}
                prefix = "points#spend#"
            elif t == "earn":
                kind_filter = {"earn"}
                prefix = "points#earn#"
            elif t == "adjust":
                kind_filter = {"adjust"}
                prefix = "points#adjust#"
            elif t in ("all", "*"):
                kind_filter = {"earn", "spend", "adjust"}
                prefix = "points#"  # まとめて取得して kind で絞る
            else:
                # 想定外は安全に spend 扱い
                kind_filter = {"spend"}
                prefix = "points#spend#"

            # ---- DynamoDB Query（ページネーション込み）----
            txs: List[Dict[str, Any]] = []
            last_evaluated_key = None

            while len(txs) < limit:
                q_kwargs = dict(
                    KeyConditionExpression=Key("user_id").eq(user_id) & Key("joined_at").begins_with(prefix),
                    ScanIndexForward=False,   # 新しい順
                    ConsistentRead=True,
                    Limit=min(100, limit - len(txs)),  # 余裕を持って取得
                )
                if last_evaluated_key:
                    q_kwargs["ExclusiveStartKey"] = last_evaluated_key

                resp = self.part_history.query(**q_kwargs)
                items = resp.get("Items", [])
                last_evaluated_key = resp.get("LastEvaluatedKey")

                for it in items:
                    joined_at = str(it.get("joined_at") or "")
                    kind = str(it.get("kind") or "")

                    # prefix="points#" のとき等に備えて joined_at/ kind 両方で絞る
                    if not joined_at.startswith("points#"):
                        continue
                    if kind not in kind_filter:
                        continue

                    # ---- 数値の取り扱いを安全に（Decimal -> int 等）----
                    def to_int(v, default=0) -> int:
                        try:
                            if v is None:
                                return default
                            if isinstance(v, Decimal):
                                return int(v)
                            return int(v)
                        except Exception:
                            return default

                    delta_points = to_int(it.get("delta_points"), 0)

                    # points_used は 0 を正しく扱う（or にしない）
                    pu_raw = it.get("points_used")
                    if pu_raw is not None:
                        points_used = to_int(pu_raw, 0)
                    else:
                        # spend の場合は delta_points が負なので反転して正に
                        points_used = -delta_points if kind == "spend" else 0

                    # 表示用 type
                    ui_type = "payment" if kind == "spend" else kind

                    txs.append({
                        "date": it.get("created_at") or it.get("transaction_date") or it.get("paid_at"),
                        "type": ui_type,
                        "kind": kind,  # 内部的に区別したい場合に便利
                        "points_used": points_used,        # 支払い(消費)は常に正
                        "points": points_used,             # 互換
                        "delta_points": delta_points,      # +/-
                        "description": it.get("reason", "") or "",
                        "event_date": it.get("event_date"),
                        "payment_type": it.get("payment_type"),
                        "tx_id": it.get("tx_id"),
                        "source": it.get("source"),
                        "schedule_id": it.get("schedule_id"),
                        "created_by": it.get("created_by"),
                    })

                    if len(txs) >= limit:
                        break

                if not last_evaluated_key:
                    break

            print(f"[DEBUG] 取引履歴取得 - user_id: {user_id}, 件数: {len(txs)}, type={t}")
            return txs

        except Exception as e:
            print(f"[ERROR] 取引履歴取得エラー: {e}")
            import traceback; traceback.print_exc()
            return []
    

    def get_point_balance_summary(self, user_id: str):
        try:
            # 消費
            total_spent = self.calc_total_points_spent(user_id)

            # 獲得（まずは記録ベース）
            earned_records = self.get_point_transactions(user_id, transaction_type='earned', limit=1000)
            if earned_records:
                total_earned = sum(int(r.get('points', 0)) for r in earned_records)
                using_calculated = False
            else:
                # フォールバック：既存の計算ロジック
                stats = self.get_user_stats(user_id)
                total_earned = (
                    int(stats.get('participation_points', 0)) +
                    int(stats.get('streak_points', 0)) +
                    int(stats.get('monthly_bonus_points', 0))
                )
                using_calculated = True

            return {
                'total_earned': total_earned,
                'total_spent': total_spent,
                'current_balance': total_earned - total_spent,
                'using_calculated_earned': using_calculated
            }
        except Exception as e:
            print(f"[ERROR] 収支サマリーエラー: {e}")
            return None
        
    def get_manual_points(self, user_id: str, reset_date: str | None = None) -> int:
        """
        管理人付与ポイント合計（ledger集計）
        - bad-users-history の "earn" かつ joined_at が "points#earn#" で始まる
        - earn_type=="manual" または source=="admin_manual" を対象
        - reset_date があれば event_date >= reset_date のみカウント
        ※ 旧: ugu_points は参照しない
        """
        table = self.part_history  # boto3.resource('dynamodb').Table('bad-users-history')
        total = 0

        resp = table.query(
            KeyConditionExpression=Key("user_id").eq(user_id) &
                                Key("joined_at").begins_with("points#earn#"),
            ScanIndexForward=True
        )
        items = resp.get("Items", [])
        while "LastEvaluatedKey" in resp:
            resp = table.query(
                KeyConditionExpression=Key("user_id").eq(user_id) &
                                    Key("joined_at").begins_with("points#earn#"),
                ScanIndexForward=True,
                ExclusiveStartKey=resp["LastEvaluatedKey"]
            )
            items.extend(resp.get("Items", []))

        for it in items:
            if it.get("kind") != "earn":
                continue
            if not (it.get("earn_type") == "manual" or it.get("source") == "admin_manual"):
                continue
            if reset_date and it.get("event_date", "") < reset_date:
                continue

            val = it.get("delta_points")
            if val is None:
                val = it.get("points", 0)  # 念のため互換
            try:
                total += int(val)
            except Exception:
                pass

        print(f"[DEBUG] 管理人付与(ledger) 合計: {total}P / user_id={user_id}, since={reset_date}")
        return total
    
    def list_point_spends(self, user_id: str, limit: int = 20):
        """
        bad-users-history から支払(消費)の直近履歴を返す。
        必ず list を返す（例外・0件時は []）。
        返す要素: {event_date, amount, reason, created_at, tx_id, joined_at}
        """
        try:
            resp = self.part_history.query(
                KeyConditionExpression=Key("user_id").eq(user_id) & Key("joined_at").begins_with("points#spend#"),
                ScanIndexForward=False,  # 新しい順
                Limit=limit
            )
            items = resp.get("Items", [])
            out = []
            for it in items:
                # amount は points_used 優先、なければ delta_points (負数) を反転して正数に
                amt = it.get("points_used")
                if amt is None:
                    dp = int(it.get("delta_points", 0))
                    amt = -dp if dp < 0 else 0
                out.append({
                    "event_date": it.get("event_date"),
                    "amount": int(amt or 0),
                    "reason": it.get("reason", "参加費"),
                    "created_at": it.get("created_at"),
                    "tx_id": it.get("tx_id"),
                    "joined_at": it.get("joined_at"),
                })
            return out
        except Exception as e:
            print(f"[WARN] list_point_spends failed: {e}")
            return []

    def debug_list_spend(self, user_id: str, since: str | None = None) -> int:
        """
        上の list_point_spends を使ってログ出力も行う簡易版（開発時に手早く確認用）。
        戻り値: 合計消費ポイント
        """
        result = self.list_point_spends(user_id, since=since)
        for row in result["items"]:
            print("[SPEND]", row["event_date"], f"-{row['used_points']}P", row.get("reason", ""), row["joined_at"])
        print("== 消費合計:", result["total_spent"], "P ==")
        return result["total_spent"]

    # プロフィール表示用：うぐポイント等の集計（履歴テーブルのみで計算）
    def get_user_stats(self, user_id: str):
        """
        うぐポイントを計算（中学生は半分のポイント）

        【重要な仕様】
        - 参加回数表示：45日ルール適用外（全履歴をカウント）
        - ポイント計算：45日ルール適用（45日以上空いたらリセット）
        - 初回参加だけ200Pを固定で付与する
        - 月内の参加回数に応じてボーナスを付与する（5,7,10,15,20回）
        - 5回ごとの累計ボーナス（+500P）は継続
        """
        try:
            from collections import defaultdict
            from datetime import datetime

            FIRST_PARTICIPATION_POINTS = 200  # 初回だけこれを付ける

            print(f"\n[DEBUG] うぐポイント計算開始 - user_id: {user_id}")
            print("=" * 80)

            # まず管理人付与分を必ず取る
            manual_points = self.get_manual_points(user_id)
            print(f"[DEBUG] 管理人付与ポイント: {manual_points}P")

            # ユーザー情報を取得（生年月日を含む）
            user_info = self.get_user_info(user_id)
            is_junior_high = self._is_junior_high_student(user_info)
            point_multiplier = 0.5 if is_junior_high else 1.0

            if is_junior_high:
                print(f"[DEBUG] 中学生判定 → ポイント係数: {point_multiplier}倍")

            # タイムスタンプ付き参加履歴を取得
            participation_history = self.get_user_participation_history_with_timestamp(user_id)

            # 参加履歴が一件もない場合は管理人付与分だけ返す
            if not participation_history:
                print(f"[DEBUG] 参加履歴なし - user_id: {user_id}")
                return {
                    'uguu_points': manual_points,
                    'participation_points': 0,
                    'streak_points': 0,
                    'monthly_bonus_points': 0,
                    'cumulative_bonus_points': 0,
                    'points_used': 0,
                    'total_participation': 0,
                    'last_participation_date': None,
                    'current_streak_start': None,
                    'current_streak_count': 0,
                    'cumulative_count': 0,
                    'monthly_bonuses': {},
                    'early_registration_count': 0,
                    'super_early_registration_count': 0,
                    'direct_registration_count': 0,
                    'is_junior_high': is_junior_high,
                    'is_reset': False,
                    'days_until_reset': None,
                    'manual_points': manual_points,
                    'base_points': 0,
                }

            # 参加履歴をdatetimeにしてソート
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

            participation_records.sort(key=lambda x: x['event_date'])

            # 表示用の全参加回数（40日ルール適用外）
            total_participation_all_time = len(participation_records)
            print(f"\n[DEBUG] 全参加回数（表示用）: {total_participation_all_time}回")

            # ===== 全履歴の早期登録カウント（統計用） =====
            all_time_early_count = 0   # 〜3日前(100)
            all_time_direct_count = 0  # 2日前〜前日(50)

            for record in participation_records:
                base_points = self._is_early_registration(record)
                if base_points == 100:
                    all_time_early_count += 1
                elif base_points == 50:
                    all_time_direct_count += 1

            print(f"[DEBUG] 全履歴の早期登録: 〜3日前(100)={all_time_early_count}回, 2日前〜前日(50)={all_time_direct_count}回")

            RESET_DAYS = 45  # 45日ルール（46日以上空いたらリセット）

            print(f"\n[DEBUG] {RESET_DAYS}日ルールチェック（ポイント計算用）")
            last_reset_index = 0
            is_reset = False

            for i in range(1, len(participation_records)):
                current_date = participation_records[i]["event_date"]
                previous_date = participation_records[i - 1]["event_date"]
                days_diff = (current_date - previous_date).days

                if days_diff > RESET_DAYS:
                    print(f"[DEBUG] {RESET_DAYS}日超の空白期間検出（{days_diff}日）")
                    print(f"[DEBUG] {previous_date:%Y-%m-%d} → {current_date:%Y-%m-%d}")
                    print(f"[DEBUG] → ポイントは {current_date:%Y-%m-%d} 以降のみ計算")
                    last_reset_index = i
                    is_reset = True

            # RESET_DAYS ルール適用後の履歴（これでポイントを計算する）
            if last_reset_index > 0:
                participation_records_for_points = participation_records[last_reset_index:]
                start_dt = participation_records_for_points[0]["event_date"]
                print(f"[DEBUG] ポイントリセット: {start_dt:%Y-%m-%d} 以降の {len(participation_records_for_points)} 回のみ計算")
                print(f"[DEBUG] 参加回数表示は全 {total_participation_all_time} 回")
            else:
                participation_records_for_points = participation_records
                print(f"[DEBUG] リセットなし: 全 {total_participation_all_time} 回が有効")

            # 連続参加チェック用に参加日セットを作る
            user_participated_dates = {
                r["event_date"].strftime("%Y-%m-%d") for r in participation_records_for_points
            }

            # ===== 参加ポイント（初回200P・それ以降は早期ポイント） =====
            participation_points = 0
            early_registration_count = 0   # 〜3日前(100)
            direct_registration_count = 0  # 2日前〜前日(50)

            cumulative_count = 0
            cumulative_bonus_points = 0
            cumulative_milestones = []

            for idx, record in enumerate(participation_records_for_points, start=1):
                base_points = self._is_early_registration(record)

                # 統計用カウント（リセット後の期間だけ）
                if base_points == 100:
                    early_registration_count += 1
                elif base_points == 50:
                    direct_registration_count += 1
                # 0点はカウントしない

                # 実際に付けるポイント
                if idx == 1:
                    pts = int(FIRST_PARTICIPATION_POINTS * point_multiplier)  # 初回200
                    participation_points += pts
                    print(f"[DEBUG] 初回参加 → +{pts}P")
                else:
                    pts = int(base_points * point_multiplier)
                    participation_points += pts
                    print(f"[DEBUG] 通常参加 → base:{base_points}P → +{pts}P")

                # 累計ボーナス（5回ごとに500P）
                cumulative_count += 1
                if cumulative_count % 5 == 0:
                    bonus = int(500 * point_multiplier)
                    cumulative_bonus_points += bonus
                    cumulative_milestones.append({
                        "date": record["event_date"].strftime("%Y-%m-%d"),
                        "count": cumulative_count,
                        "bonus": bonus,
                    })
                    print(f"[DEBUG] 累計{cumulative_count}回 → +{bonus}P")

                # 累計ボーナス（5回ごとに500P）
                cumulative_count += 1
                if cumulative_count % 5 == 0:
                    bonus = int(500 * point_multiplier)
                    cumulative_bonus_points += bonus
                    cumulative_milestones.append({
                        'date': record['event_date'].strftime('%Y-%m-%d'),
                        'count': cumulative_count,
                        'bonus': bonus,
                    })
                    print(f"[DEBUG] 累計{cumulative_count}回達成 → ボーナス +{bonus}P")

            print(f"[DEBUG] 参加ポイント合計: {participation_points}P")
            print(f"[DEBUG] 累計参加ボーナス合計: {cumulative_bonus_points}P")

            # ===== 連続参加ボーナス =====
            today = datetime.now().date()
            all_schedules = self.get_all_past_schedules(today)

            if last_reset_index > 0:
                reset_date = participation_records_for_points[0]['event_date'].strftime('%Y-%m-%d')
                all_schedules = [s for s in all_schedules if s['date'] >= reset_date]
                print(f"\n[DEBUG] 連続参加チェック（{reset_date} 以降のスケジュールのみ）")
            else:
                print(f"\n[DEBUG] 連続参加チェック（全スケジュール）")

            print(f"[DEBUG] 対象練習回数: {len(all_schedules)}回")
            print(f"[DEBUG] ユーザー参加: {len(user_participated_dates)}回")

            streak_points = 0
            current_streak = 0
            max_streak = 0
            streak_start = None

            milestone_5_achieved = False
            milestone_10_achieved = False
            milestone_15_achieved = False
            milestone_20_achieved = False
            milestone_25_achieved = False

            for schedule in all_schedules:
                schedule_date = schedule['date']
                is_participated = schedule_date in user_participated_dates

                if is_participated:
                    current_streak += 1
                    if streak_start is None:
                        streak_start = schedule_date

                    # 連続2回目以降は毎回50P
                    if current_streak >= 2:
                        sp = int(50 * point_multiplier)
                        streak_points += sp
                        print(f"[DEBUG] {schedule_date} 参加 → 連続{current_streak}回目 +{sp}P")

                    # 連続マイルストーン
                    milestone_bonus = 0
                    if current_streak == 5 and not milestone_5_achieved:
                        milestone_bonus = int(200 * point_multiplier)
                        milestone_5_achieved = True
                    elif current_streak == 10 and not milestone_10_achieved:
                        milestone_bonus = int(400 * point_multiplier)
                        milestone_10_achieved = True
                    elif current_streak == 15 and not milestone_15_achieved:
                        milestone_bonus = int(600 * point_multiplier)
                        milestone_15_achieved = True
                    elif current_streak == 20 and not milestone_20_achieved:
                        milestone_bonus = int(800 * point_multiplier)
                        milestone_20_achieved = True
                    elif current_streak == 25 and not milestone_25_achieved:
                        milestone_bonus = int(1000 * point_multiplier)
                        milestone_25_achieved = True

                    if milestone_bonus > 0:
                        streak_points += milestone_bonus
                        print(f"[DEBUG] 連続{current_streak}回達成ボーナス +{milestone_bonus}P")

                    max_streak = max(max_streak, current_streak)
                else:
                    if current_streak > 0:
                        print(f"[DEBUG] {schedule_date} 不参加 → 連続{current_streak}回でリセット")
                    current_streak = 0
                    streak_start = None
                    milestone_5_achieved = False
                    milestone_10_achieved = False
                    milestone_15_achieved = False
                    milestone_20_achieved = False
                    milestone_25_achieved = False

            print(f"[DEBUG] 連続ポイント合計: {streak_points}P")

            # ===== 月間ボーナス（係数適用、45日ルール適用後） =====
            monthly_participation = defaultdict(int)
            for record in participation_records_for_points:
                month_key = record['event_date'].strftime("%Y-%m")
                monthly_participation[month_key] += 1

            print(f"\n[DEBUG] 月間ボーナス計算（45日ルール適用後）")
            monthly_bonuses = {}
            monthly_bonus_points = 0

            for month, count in sorted(monthly_participation.items()):
                print(f"[DEBUG] 月別参加回数 - {month}: {count}回")

                base_bonus = 0
                if count >= 5:
                    base_bonus = 200
                if count >= 8:
                    base_bonus = 300
                if count >= 10:
                    base_bonus = 400
                if count >= 15:
                    base_bonus = 600
                if count >= 20:
                    base_bonus = 800

                bonus = int(base_bonus * point_multiplier)
                monthly_bonuses[month] = {
                    'participation_count': count,
                    'bonus_points': bonus,
                }

                if bonus > 0:
                    print(f"[DEBUG] {month} - {count}回参加 → 月間ボーナス: {bonus}P")

                monthly_bonus_points += bonus

            # ===== ポイント消費の集計 =====
            print(f"\n[DEBUG] ポイント消費チェック")
            total_points_used = 0
            payments = self.get_user_payment_history(user_id)

            if last_reset_index > 0:
                reset_date_str = participation_records_for_points[0]['event_date'].strftime('%Y-%m-%d')
                payments = [p for p in payments if p.get('event_date', '9999-99-99') >= reset_date_str]
                print(f"[DEBUG] {reset_date_str} 以降の支払いのみを集計")

            for payment in payments:
                points_used = payment.get('points_used', 0)
                if points_used > 0:
                    total_points_used += points_used
                    event_date = payment.get('event_date', '不明')
                    paid_at = payment.get('transaction_date', payment.get('paid_at', '不明'))
                    print(f"[DEBUG] {event_date} ポイント支払い: {points_used}P (支払日時: {paid_at})")

            print(f"[DEBUG] 合計ポイント消費: {total_points_used}P")

            # ===== 失効カウントダウン =====
            days_until_reset = None
            if participation_records:
                last_participation_date_obj = participation_records[-1]["event_date"]  # datetime
                today_datetime = datetime.now()

                days_since_last = (today_datetime - last_participation_date_obj).days
                days_until_reset = RESET_DAYS - days_since_last  # ★ ここを統一

                print(f"\n[DEBUG] 失効カウントダウン")
                print(f"[DEBUG] 最終参加日: {last_participation_date_obj:%Y-%m-%d}")
                print(f"[DEBUG] 経過日数: {days_since_last}日")
                print(f"[DEBUG] 残り日数: {days_until_reset}日")

                # ★ リセット条件と矛盾しない判定
                #   リセットは「46日以上空いたら」なので、失効状態は days_since_last > RESET_DAYS
                if days_since_last > RESET_DAYS:
                    print(f"[DEBUG] 失効状態です（{RESET_DAYS}日超過）")

            # ===== 総ポイント計算 =====
            uguu_points = (
                participation_points
                + streak_points
                + monthly_bonus_points
                + cumulative_bonus_points
                + manual_points
                - total_points_used
            )

            result = {
                'uguu_points': uguu_points,
                'participation_points': participation_points,
                'streak_points': streak_points,
                'monthly_bonus_points': monthly_bonus_points,
                'cumulative_bonus_points': cumulative_bonus_points,
                'points_used': total_points_used,

                'total_participation': total_participation_all_time,

                # リセット後期間の内訳
                'early_registration_count': early_registration_count,   # 〜3日前(100)
                'direct_registration_count': direct_registration_count, # 2日前〜前日(50)

                # 全期間の内訳
                'all_time_early_registration_count': all_time_early_count,
                'all_time_direct_registration_count': all_time_direct_count,

                'last_participation_date': participation_records[-1]['event_date'].strftime('%Y-%m-%d') if participation_records else None,
                'current_streak_start': streak_start if streak_start else None,
                'current_streak_count': current_streak,
                'cumulative_count': cumulative_count,
                'monthly_bonuses': monthly_bonuses,
                'is_junior_high': is_junior_high,
                'is_reset': is_reset,
                'days_until_reset': days_until_reset,
                'manual_points': manual_points,
            }

            print(f"\n[DEBUG] === 最終結果 ===")
            print(f"[DEBUG] 表示用参加回数: {total_participation_all_time}回")
            if is_reset:
                print(f"[DEBUG] ポイントは40日ルールにより途中から計算されています")
                print(f"[DEBUG] ポイント計算に使った参加回数: {len(participation_records_for_points)}回")
            if is_junior_high:
                print(f"[DEBUG] 中学生係数 {point_multiplier}倍 が適用されています")
            print(f"[DEBUG] 参加ポイント: {participation_points}P")
            print(f"[DEBUG] 連続ポイント: {streak_points}P")
            print(f"[DEBUG] 月間ボーナス: {monthly_bonus_points}P")
            print(f"[DEBUG] 累計ボーナス: {cumulative_bonus_points}P")
            print(f"[DEBUG] 管理人付与: {manual_points}P")
            print(f"[DEBUG] ポイント消費: -{total_points_used}P")
            print(f"[DEBUG] 合計うぐポイント: {uguu_points}P")
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
                'cumulative_bonus_points': 0,
                'points_used': 0,
                'total_participation': 0,
                'early_registration_count': 0,
                'super_early_registration_count': 0,
                'direct_registration_count': 0,
                'last_participation_date': None,
                'current_streak_start': None,
                'current_streak_count': 0,
                'cumulative_count': 0,
                'monthly_bonuses': {},
                'is_junior_high': False,
                'is_reset': False,
                'days_until_reset': None
            }
        
    def calc_total_points_spent(self, user_id: str, since: str | None = None) -> int:
        """
        消費合計（正の数: 800 など）。
        - kind=spend の台帳を直接読み、points_used が無ければ delta_points(<0) を反転して加算。
        - since = 'YYYY-MM-DD' を渡すと、その日付以降の支払いだけ合計。
        """
        t = self.part_history  # bad-users-history テーブル想定
        total = 0

        rs = t.query(
            KeyConditionExpression=Key("user_id").eq(user_id) &
                                Key("joined_at").begins_with("points#spend#"),
            ScanIndexForward=True,  # 昇順
        )
        items = rs.get("Items", [])
        while "LastEvaluatedKey" in rs:
            rs = t.query(
                KeyConditionExpression=Key("user_id").eq(user_id) &
                                    Key("joined_at").begins_with("points#spend#"),
                ScanIndexForward=True,
                ExclusiveStartKey=rs["LastEvaluatedKey"],
            )
            items += rs.get("Items", [])

        for it in items:
            # 期間フィルタ（40日リセット以降だけ数えたい時に使う）
            if since and it.get("event_date", "0000-00-00") < since:
                continue

            used = int(it.get("points_used", 0))
            if used <= 0:
                dp = int(it.get("delta_points", 0))
                if dp < 0:
                    used = -dp  # 例: delta_points=-600 → used=600

            if used > 0:
                total += used

        print(f"[DEBUG] 合計ポイント消費(ledger): {total}P / 件数: {len(items)}"
            + (f" / since={since}" if since else ""))
        return total   
    

def record_spend(history_table, *, user_id: str, points_used: int,
                 event_date: str, payment_type: str = "event_participation",
                 reason: str | None = None, created_by: str | None = None):
    """
    bad-users-history に消費(spend)トランザクションを1件保存する。
    """
    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    tx_id = str(uuid4())
    joined_at = f"points#spend#{now}#{tx_id}"

    item = {
        "user_id": user_id,
        "joined_at": joined_at,
        "tx_id": tx_id,
        "kind": "spend",
        "delta_points": -int(points_used),
        "points_used": int(points_used),
        "payment_type": payment_type,
        "event_date": event_date,
        "reason": reason or f"{event_date}の参加費",
        "created_at": now,
        "entity_type": "point_transaction",
        "version": 1,
    }

    history_table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(user_id) AND attribute_not_exists(joined_at)"
    )
    return item


class PointTransaction:
    """ポイント取引のデータ構造を定義"""
    
    # エンティティタイプ
    TYPE_EARNED = 'earned'      # ポイント獲得
    TYPE_PAYMENT = 'payment'    # ポイント支払い
    
    # 獲得種別
    EARN_PARTICIPATION = 'participation'  # 参加ポイント
    EARN_STREAK = 'streak'                # 連続ボーナス
    EARN_MILESTONE = 'milestone'          # マイルストーン達成
    EARN_MONTHLY = 'monthly'              # 月間ボーナス
    
    # 支払い種別
    PAYMENT_EVENT = 'event_participation' # イベント参加費
    
    

# 重要: インスタンスを作成
db = DynamoDB()
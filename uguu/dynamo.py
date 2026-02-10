from datetime import datetime, timezone
from uuid import uuid4
from dotenv import load_dotenv
from boto3.dynamodb.conditions import Key
from utils.points import record_spend
from typing import Any, Dict, List, Optional, Tuple, Set
import json, base64
from decimal import Decimal
from botocore.exceptions import ClientError
import re
from utils.timezone import JST

from uguu.point import (
    PointRules,
    normalize_participation_history,
    calc_reset_index,
    slice_records_for_points,
    build_participated_date_set,
    calc_registration_counts,
    calc_participation_and_cumulative,
    calc_monthly_bonus,
    calc_days_until_reset,
    classify,
    better,
    calc_streak_points,
    _is_junior_high_or_below
)

load_dotenv()

import boto3
import os

def get_today_jst():
    return datetime.now(JST).date()

    
ISO_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)"
)

def extract_iso_from_joined_at(s: str) -> str | None:
    if not s:
        return None
    m = ISO_RE.search(s)
    return m.group(1) if m else None

def parse_dt_safe(s: str | None, *, default_tz=JST) -> datetime | None:
    if not s:
        return None
    s = str(s).strip()

    # fromisoformat は "Z" を直接食えないので +00:00 にする
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    dt = None

    # 1) まずISO系を試す（"YYYY-MM-DDTHH:MM:SS" や tz付きもここでOK）
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        # 2) スペース区切り救済
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

    # 3) ここが重要：naive→tz付与、aware→JSTへ変換（返り値を統一）
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=default_tz)       # naiveは「そのTZのローカル時刻」として扱う
    else:
        dt = dt.astimezone(default_tz)           # awareはJSTに寄せる

    return dt
        
def iso_to_jst(s: str) -> str:
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return s

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
        today = datetime.now(JST).date()
        
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
        同じ日付が複数ある場合は、基本は「最も遅い登録時刻」を採用（再参加/再キャンセルを反映）
        ただし例外として、
        - "たら"(tara_join) は単体では参加ではない
        - 同日内に正式参加（参加ボタン）が1件でもあれば "たら" より正式参加を優先する
        - 管理人手動ポイント(admin_manual_point)は参加履歴に混ぜない（必要なら変更可）

        返却形式（従来通り）:
            [{"event_date":"YYYY-MM-DD","registered_at":"YYYY-MM-DD HH:MM:SS","status":"registered"}, ...]
        """
        import os
        from datetime import datetime, time
        from boto3.dynamodb.conditions import Key

        # ------------- logging switch -------------
        # 例:
        #   POINT_LOG=1 でサマリーを表示
        #   POINT_LOG_DETAIL=1 で詳細を表示
        DEBUG = os.getenv("POINT_LOG", "1") == "1"
        DEBUG_DETAIL = os.getenv("POINT_LOG_DETAIL", "0") == "1"

        def dbg(msg: str):
            if DEBUG:
                print(msg)

        def dbg_detail(msg: str):
            if DEBUG_DETAIL:
                print(msg)

        # --- 判定ポリシー（必要ならここを調整） ---
        TARA_ACTION = "tara_join"
        EXCLUDE_ACTIONS = {"admin_manual_point"}

        def classify_local(rec: dict) -> str:
            status = (rec.get("status") or "registered").lower()
            action = rec.get("action")
            source = rec.get("source")
            if status == "cancelled":
                return "cancelled"
            if action in EXCLUDE_ACTIONS or source == "admin_manual":  # ← 追加
                return "other"
            if action in EXCLUDE_ACTIONS:
                return "other"
            if action == TARA_ACTION:
                return "tara"
            return "official"

        def better_local(new: dict, old: dict) -> bool:
            cn, co = classify_local(new), classify_local(old)

            # other は参加履歴の採用対象外
            if cn == "other" and co != "other":
                return False
            if co == "other" and cn != "other":
                return True

            # official と tara の競合は official を優先（時刻に関わらず）
            if cn == "official" and co == "tara":
                return True
            if cn == "tara" and co == "official":
                return False

            # それ以外は時刻で比較
            tn = new.get("registered_at_dt")
            to = old.get("registered_at_dt")
            if tn and to:
                return tn > to
            if tn and not to:
                return True
            if to and not tn:
                return False
            return False

        try:
            # ---- DynamoDB query ----
            items = []
            resp = self.part_history.query(KeyConditionExpression=Key("user_id").eq(user_id))
            items.extend(resp.get("Items", []))
            while resp.get("LastEvaluatedKey"):
                resp = self.part_history.query(
                    KeyConditionExpression=Key("user_id").eq(user_id),
                    ExclusiveStartKey=resp["LastEvaluatedKey"],
                )
                items.extend(resp.get("Items", []))

            today = datetime.now(JST).date()

            # event_date_str -> 採用レコード
            date_records: dict[str, dict] = {}

            cancelled_count = 0
            future_count = 0
            parse_error_count = 0
            rescued_no_ts_count = 0
            processed_count = 0

            # 詳細時にだけ「問題のあった日」などを残す
            rescued_dates = []
            parse_error_dates = []

            for idx, item in enumerate(items, 1):
                try:
                    status = (item.get("status") or "registered").lower()
                    action = item.get("action")

                    event_date_str = (item.get("date") or item.get("event_date") or "").strip()
                    if not event_date_str:
                        parse_error_count += 1
                        if DEBUG_DETAIL:
                            parse_error_dates.append("(no date field)")
                        continue

                    event_date = datetime.strptime(event_date_str, "%Y-%m-%d").date()
                    if event_date > today:
                        future_count += 1
                        continue

                    joined_at_raw = item.get("joined_at")
                    joined_at_str = str(joined_at_raw or "").strip()

                    # timestamp 候補を拾う（優先キー）
                    iso_candidate = None
                    iso_source = None
                    for key in ("created_at", "registered_at", "transaction_date", "paid_at"):
                        v = item.get(key)
                        if v:
                            iso_candidate = v
                            iso_source = key
                            break

                    if not iso_candidate:
                        extracted = extract_iso_from_joined_at(joined_at_str)
                        if extracted:
                            iso_candidate = extracted
                            iso_source = "joined_at(extract_iso)"

                    registered_at = parse_dt_safe(iso_candidate, default_tz=JST)

                    # 救済：timestamp無しは「その日の最初」に置く（←ここは警告だけ残す）
                    if not registered_at:
                        registered_at = datetime.combine(event_date, time(0, 0, 0), tzinfo=JST)
                        rescued_no_ts_count += 1
                        if DEBUG:
                            dbg(f"[WARN] timestamp無しを救済(00:00:00): {event_date_str}")
                        if DEBUG_DETAIL:
                            rescued_dates.append(event_date_str)

                    if status == "cancelled":
                        cancelled_count += 1

                    new_rec = {
                        "event_date": event_date_str,
                        "registered_at_dt": registered_at,
                        "status": status,
                        "action": action,
                        "iso_source": iso_source,
                    }

                    if event_date_str not in date_records:
                        date_records[event_date_str] = new_rec
                        processed_count += 1
                    else:
                        existing = date_records[event_date_str]
                        if better_local(new_rec, existing):
                            date_records[event_date_str] = new_rec

                    # ここから下は詳細時のみ
                    if DEBUG_DETAIL:
                        dbg_detail(
                            f"[DETAIL] {idx}/{len(items)} "
                            f"date={event_date_str} status={status} action={action} "
                            f"ts={registered_at.strftime('%H:%M:%S')} src={iso_source}"
                        )

                except Exception as e:
                    parse_error_count += 1
                    if DEBUG:
                        dbg(f"[WARN] レコード処理エラー: date={item.get('date') or item.get('event_date')} err={e}")
                    if DEBUG_DETAIL:
                        parse_error_dates.append(str(item.get("date") or item.get("event_date") or "unknown"))
                        import traceback
                        traceback.print_exc()
                    continue

            # event_date順に整列
            records_all_raw = sorted(date_records.values(), key=lambda x: x["event_date"])

            # official のみ返す
            kept_raw = [r for r in records_all_raw if classify_local(r) == "official"]

            records = [
                {
                    "event_date": r["event_date"],
                    "registered_at": r["registered_at_dt"].strftime("%Y-%m-%d %H:%M:%S"),
                    "status": r["status"],
                }
                for r in kept_raw
            ]

            # ---------------- summary (1 line) ----------------
            if DEBUG:
                if records:
                    first_day = records[0]["event_date"]
                    last_day = records[-1]["event_date"]
                    day_range = f"{first_day}..{last_day}"
                else:
                    day_range = "-"

                dbg(
                    "[POINT][history] "
                    f"user_id={user_id} "
                    f"raw={len(items)} official={len(records)} "
                    f"cancelled={cancelled_count} future={future_count} "
                    f"parse_err={parse_error_count} rescued_no_ts={rescued_no_ts_count} "
                    f"range={day_range}"
                )

            # 詳細時だけ「問題一覧」を少しだけ出す（全件は出さない）
            if DEBUG_DETAIL:
                if rescued_dates:
                    dbg_detail(f"[DETAIL] rescued_dates(sample up to 10)={rescued_dates[:10]}")
                if parse_error_dates:
                    dbg_detail(f"[DETAIL] parse_error_dates(sample up to 10)={parse_error_dates[:10]}")

            return records

        except Exception as e:
            if DEBUG:
                dbg(f"[ERROR] get_user_participation_history_with_timestamp: {e}")
            if DEBUG_DETAIL:
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

        # print(
        #     f"    参加ポイント判定 - イベント日: {event_date:%Y-%m-%d}, "
        #     f"登録日時: {registered_at:%Y-%m-%d %H:%M:%S}"
        # )
        # print(f"      3日前締切: {three_days_deadline:%Y-%m-%d %H:%M:%S}")
        # print(f"      前日締切: {one_day_deadline:%Y-%m-%d %H:%M:%S}")
        # print(f"      → {registration_type}: +{points}P")

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

        # 現在の日付（JST）
        today = datetime.now(JST).date()

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

        valid_dates = []
        for it in items:
            d_str = it.get("date")
            if not d_str:
                continue

            # キャンセル済みは除外
            if (it.get("status") or "").lower() == "cancelled":
                print(f"[DEBUG] キャンセル済みの参加をスキップ: {d_str}")
                continue

            # 文字列 → date に変換して未来除外
            try:
                d = datetime.strptime(d_str, "%Y-%m-%d").date()
            except ValueError:
                print(f"[WARN] 不正な日付形式をスキップ: {d_str}")
                continue

            if d <= today:
                valid_dates.append(d_str)

        # 重複を削除してソート（文字列のままでも YYYY-MM-DD なら時系列順）
        dates = sorted(set(valid_dates))

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
        import os
        from datetime import datetime

        # ログ制御（既存の方式に合わせて環境変数でON/OFF）
        DEBUG = os.getenv("POINT_LOG", "1") == "1"               # サマリー/重要だけ
        DEBUG_DETAIL = os.getenv("POINT_LOG_DETAIL", "0") == "1" # 詳細（配列表示など）

        def dbg(msg: str):
            if DEBUG:
                print(msg)

        def dbg_detail(msg: str):
            if DEBUG_DETAIL:
                print(msg)

        try:
            # === タイムゾーン設定 ===
            today_date = datetime.now(JST).date()
            today_str = today_date.strftime("%Y-%m-%d")
            dbg(f"[SCHEDULE] upcoming today={today_str} today_only={today_only} limit={limit}")

            # === スケジュール取得 ===
            response = self.schedule_table.scan()
            schedules = response.get("Items", [])

            while "LastEvaluatedKey" in response:
                response = self.schedule_table.scan(
                    ExclusiveStartKey=response["LastEvaluatedKey"]
                )
                schedules.extend(response.get("Items", []))

            dbg(f"[SCHEDULE] total_schedules={len(schedules)}")

            upcoming = []
            warn_count = 0

            for schedule in schedules:
                try:
                    s_date = datetime.strptime(schedule["date"], "%Y-%m-%d").date()
                    cond = (s_date == today_date) if today_only else (s_date >= today_date)

                    if cond:
                        upcoming.append({
                            "schedule_id": schedule.get("schedule_id"),
                            "date": schedule["date"],
                            "day_of_week": schedule.get("day_of_week", ""),
                            "start_time": schedule.get("start_time", ""),
                            "end_time": schedule.get("end_time", ""),
                        })

                except Exception as e:
                    warn_count += 1
                    # エラーは残す（ただし大量なら抑制）
                    if warn_count <= 5:
                        print(f"[WARN] スケジュール処理エラー: {schedule} - {e}")
                    continue

            if warn_count > 5:
                print(f"[WARN] スケジュール処理エラー: {warn_count}件（最初の5件のみ表示）")

            # === 当日のみ ===
            if today_only:
                if not upcoming:
                    dbg("[SCHEDULE] today_upcoming=0")
                    return []

                upcoming.sort(key=lambda x: x.get("start_time", "00:00"))
                result = [upcoming[0]]

                dbg(f"[SCHEDULE] today_upcoming=1 first={result[0]['date']} {result[0].get('start_time','')}")
                dbg_detail(f"[SCHEDULE][detail] result={result}")
                return result

            # === 未来分 ===
            upcoming.sort(key=lambda x: (x["date"], x.get("start_time", "")))
            result = upcoming[:limit]

            # 要約ログ（ここだけで十分）
            if result:
                first = result[0]
                last = result[-1]
                dbg(f"[SCHEDULE] upcoming={len(result)}/{len(upcoming)} range={first['date']} {first.get('start_time','')}..{last['date']} {last.get('start_time','')}")
            else:
                dbg(f"[SCHEDULE] upcoming=0/{len(upcoming)}")

            dbg_detail(f"[SCHEDULE][detail] result={result}")
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
        
    
    # def get_point_transactions(
    #     self,
    #     user_id: str,
    #     limit: int = 50,
    #     transaction_type: Optional[str] = None,
    # ) -> List[Dict[str, Any]]:
    #     """
    #     bad-users-history からポイント取引履歴を取得して整形して返す。

    #     transaction_type:
    #     - None / "all"      : earn, spend, adjust を全部返す
    #     - "spend" / "payment": spend だけ返す
    #     - "earn"            : earn だけ返す
    #     - "adjust"          : adjust だけ返す
    #     """
    #     try:
    #         # ---- type 正規化 ----
    #         t = (transaction_type or "spend").strip().lower()
    #         if t in ("payment", "spend"):
    #             kind_filter = {"spend"}
    #             prefix = "points#spend#"
    #         elif t == "earn":
    #             kind_filter = {"earn"}
    #             prefix = "points#earn#"
    #         elif t == "adjust":
    #             kind_filter = {"adjust"}
    #             prefix = "points#adjust#"
    #         elif t in ("all", "*"):
    #             kind_filter = {"earn", "spend", "adjust"}
    #             prefix = "points#"  # まとめて取得して kind で絞る
    #         else:
    #             # 想定外は安全に spend 扱い
    #             kind_filter = {"spend"}
    #             prefix = "points#spend#"

    #         # ---- DynamoDB Query（ページネーション込み）----
    #         txs: List[Dict[str, Any]] = []
    #         last_evaluated_key = None

    #         while len(txs) < limit:
    #             q_kwargs = dict(
    #                 KeyConditionExpression=Key("user_id").eq(user_id) & Key("joined_at").begins_with(prefix),
    #                 ScanIndexForward=False,   # 新しい順
    #                 ConsistentRead=True,
    #                 Limit=min(100, limit - len(txs)),  # 余裕を持って取得
    #             )
    #             if last_evaluated_key:
    #                 q_kwargs["ExclusiveStartKey"] = last_evaluated_key

    #             resp = self.part_history.query(**q_kwargs)
    #             items = resp.get("Items", [])
    #             last_evaluated_key = resp.get("LastEvaluatedKey")

    #             for it in items:
    #                 joined_at = str(it.get("joined_at") or "")
    #                 kind = str(it.get("kind") or "")

    #                 # prefix="points#" のとき等に備えて joined_at/ kind 両方で絞る
    #                 if not joined_at.startswith("points#"):
    #                     continue
    #                 if kind not in kind_filter:
    #                     continue

    #                 # ---- 数値の取り扱いを安全に（Decimal -> int 等）----
    #                 def to_int(v, default=0) -> int:
    #                     try:
    #                         if v is None:
    #                             return default
    #                         if isinstance(v, Decimal):
    #                             return int(v)
    #                         return int(v)
    #                     except Exception:
    #                         return default

    #                 delta_points = to_int(it.get("delta_points"), 0)

    #                 # points_used は 0 を正しく扱う（or にしない）
    #                 pu_raw = it.get("points_used")
    #                 if pu_raw is not None:
    #                     points_used = to_int(pu_raw, 0)
    #                 else:
    #                     # spend の場合は delta_points が負なので反転して正に
    #                     points_used = -delta_points if kind == "spend" else 0

    #                 # 表示用 type
    #                 ui_type = "payment" if kind == "spend" else kind

    #                 txs.append({
    #                     "date": it.get("created_at") or it.get("transaction_date") or it.get("paid_at"),
    #                     "type": ui_type,
    #                     "kind": kind,  # 内部的に区別したい場合に便利
    #                     "points_used": points_used,        # 支払い(消費)は常に正
    #                     "points": points_used,             # 互換
    #                     "delta_points": delta_points,      # +/-
    #                     "description": it.get("reason", "") or "",
    #                     "event_date": it.get("event_date"),
    #                     "payment_type": it.get("payment_type"),
    #                     "tx_id": it.get("tx_id"),
    #                     "source": it.get("source"),
    #                     "schedule_id": it.get("schedule_id"),
    #                     "created_by": it.get("created_by"),
    #                 })

    #                 if len(txs) >= limit:
    #                     break

    #             if not last_evaluated_key:
    #                 break

    #         print(f"[DEBUG] 取引履歴取得 - user_id: {user_id}, 件数: {len(txs)}, type={t}")
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
        - None / "all"       : earn, spend, adjust を全部返す
        - "spend" / "payment": spend だけ返す
        - "earn"             : earn だけ返す
        - "adjust"           : adjust だけ返す
        """
        try:
            # ---- helper: Decimal 等を安全に int 化 ----
            def to_int(v: Any, default: int = 0) -> int:
                try:
                    if v is None:
                        return default
                    if isinstance(v, Decimal):
                        return int(v)
                    return int(v)
                except Exception:
                    return default

            # ---- type 正規化（docstring通り: None は all）----
            t = (transaction_type or "all").strip().lower()

            # kind 正規化（DBに payment が混在しても spend 扱いに寄せる）
            kind_alias = {
                "payment": "spend",
                "spend": "spend",
                "earn": "earn",
                "adjust": "adjust",
            }

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
                # 想定外は安全に all（or spend）どちらでも良いが、ここは all 推奨
                kind_filter = {"earn", "spend", "adjust"}
                prefix = "points#"
                t = "all"

            txs: List[Dict[str, Any]] = []
            last_evaluated_key = None

            # ---- 重複排除（tx_id があればそれ優先）----
            seen: Set[Any] = set()

            while len(txs) < limit:
                q_kwargs = dict(
                    KeyConditionExpression=Key("user_id").eq(user_id) & Key("joined_at").begins_with(prefix),
                    ScanIndexForward=False,  # 新しい順
                    ConsistentRead=True,
                    Limit=min(100, limit - len(txs)),
                )
                if last_evaluated_key:
                    q_kwargs["ExclusiveStartKey"] = last_evaluated_key

                resp = self.part_history.query(**q_kwargs)
                items = resp.get("Items", [])
                last_evaluated_key = resp.get("LastEvaluatedKey")

                for it in items:
                    joined_at = str(it.get("joined_at") or "")

                    # points 系以外は弾く
                    if not joined_at.startswith("points#"):
                        continue

                    raw_kind = str(it.get("kind") or "").strip().lower()
                    kind = kind_alias.get(raw_kind, raw_kind)  # payment -> spend など

                    if kind not in kind_filter:
                        continue

                    delta_points = to_int(it.get("delta_points"), 0)

                    # spend の場合、delta_points は負で入っている想定 → points_used は常に正
                    pu_raw = it.get("points_used")
                    if pu_raw is not None:
                        points_used = to_int(pu_raw, 0)
                    else:
                        points_used = (-delta_points) if kind == "spend" else 0

                    # 表示用 type
                    ui_type = "payment" if kind == "spend" else kind

                    # 日付は joined_at もフォールバックに入れる（「不明」回避）
                    joined_at = str(it.get("joined_at") or "")

                    # payed_at が混在している可能性があるなら両対応（不要なら消してOK）
                    paid_at = it.get("paid_at") or it.get("payed_at")

                    if kind == "spend":
                        # 支払いは「支払い確定日時」を最優先にする
                        tx_date = (
                            paid_at
                            or it.get("created_at")
                            or it.get("transaction_date")
                            or extract_iso_from_joined_at(joined_at)
                            or None
                        )
                    else:
                        # earn/adjust は「作成日時」を基本にする
                        tx_date = (
                            it.get("created_at")
                            or it.get("transaction_date")
                            or extract_iso_from_joined_at(joined_at)
                            or None
                        )

                    # ---- 重複排除キー ----
                    tx_id = it.get("tx_id")
                    if tx_id:
                        dedupe_key = ("tx_id", str(tx_id))
                    else:
                        # tx_id がない場合は、同一取引を識別できる複合キーで緩く排除
                        dedupe_key = (
                            "fallback",
                            str(it.get("schedule_id") or ""),
                            kind,
                            int(delta_points),
                            str(tx_date or ""),
                            str(it.get("source") or ""),
                            str(it.get("reason") or ""),
                        )

                    if dedupe_key in seen:
                        continue
                    seen.add(dedupe_key)

                    txs.append({
                        "date": tx_date,
                        "type": ui_type,
                        "kind": kind,                    # 内部区別用（spend/earn/adjust）
                        "points_used": points_used,      # 支払い(消費)は常に正
                        "points": points_used,           # 互換
                        "delta_points": delta_points,    # +/-（earnは+ / spendは-）
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
    
    # def list_point_spends(self, user_id: str, limit: int = 20):
    #     """
    #     bad-users-history から支払(消費)の直近履歴を返す。
    #     必ず list を返す（例外・0件時は []）。
    #     返す要素: {event_date, amount, reason, created_at, tx_id, joined_at}
    #     """
    #     try:
    #         resp = self.part_history.query(
    #             KeyConditionExpression=Key("user_id").eq(user_id) & Key("joined_at").begins_with("points#spend#"),
    #             ScanIndexForward=False,  # 新しい順
    #             Limit=limit
    #         )
    #         items = resp.get("Items", [])
    #         out = []
    #         for it in items:
    #             # amount は points_used 優先、なければ delta_points (負数) を反転して正数に
    #             amt = it.get("points_used")
    #             if amt is None:
    #                 dp = int(it.get("delta_points", 0))
    #                 amt = -dp if dp < 0 else 0
    #             out.append({
    #                 "event_date": it.get("event_date"),
    #                 "amount": int(amt or 0),
    #                 "reason": it.get("reason", "参加費"),
    #                 "created_at": it.get("created_at"),
    #                 "tx_id": it.get("tx_id"),
    #                 "joined_at": it.get("joined_at"),
    #             })
    #         return out
    #     except Exception as e:
    #         print(f"[WARN] list_point_spends failed: {e}")
    #         return []
        
    def list_point_spends(self, user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        bad-users-history から支払(消費)の直近履歴を返す。
        必ず list を返す（例外・0件時は []）。

        返す要素:
        {
            event_date, amount, reason,
            created_at, tx_id, joined_at,
            schedule_id, payment_type, source
        }
        created_at が無い場合は joined_at から補完する。
        """
        try:
            resp = self.part_history.query(
                KeyConditionExpression=Key("user_id").eq(user_id) & Key("joined_at").begins_with("points#spend#"),
                ScanIndexForward=False,  # 新しい順
                ConsistentRead=True,
                Limit=limit,
            )
            items = resp.get("Items", [])

            def to_int(v: Any, default: int = 0) -> int:
                try:
                    if v is None:
                        return default
                    if isinstance(v, Decimal):
                        return int(v)
                    return int(v)
                except Exception:
                    return default

            def parse_ts_from_joined_at(joined_at: str) -> Optional[str]:
                """
                joined_at 例: points#spend#2026-01-15T12:34:56
                → 2026-01-15T12:34:56 を取り出す
                """
                if not joined_at:
                    return None
                parts = joined_at.split("#", 2)
                if len(parts) >= 3 and parts[2]:
                    return parts[2]
                return None

            def normalize_created_at(it: Dict[str, Any]) -> Optional[str]:
                # 優先順位: created_at -> paid_at -> transaction_date -> joined_at由来
                ca = it.get("created_at") or it.get("paid_at") or it.get("transaction_date")
                if ca:
                    return str(ca)
                ja = str(it.get("joined_at") or "")
                ts = parse_ts_from_joined_at(ja)
                return ts

            def normalize_event_date(it: Dict[str, Any]) -> Optional[str]:
                # event_date が無い場合、joined_at の日付部分で補完（YYYY-MM-DD）
                ed = it.get("event_date")
                if ed:
                    return str(ed)

                ja = str(it.get("joined_at") or "")
                ts = parse_ts_from_joined_at(ja)
                if not ts:
                    return None

                # ts: 2026-01-15T12:34:56 or 2026-01-15 12:34:56 などを想定
                for sep in ("T", " "):
                    if sep in ts:
                        return ts.split(sep, 1)[0]
                # それっぽい形なら先頭10文字
                return ts[:10] if len(ts) >= 10 else ts

            out: List[Dict[str, Any]] = []
            for it in items:
                # amount は points_used 優先、なければ delta_points (負数) を反転して正数に
                pu = it.get("points_used")
                if pu is not None:
                    amount = to_int(pu, 0)
                else:
                    dp = to_int(it.get("delta_points"), 0)
                    amount = -dp if dp < 0 else 0

                out.append({
                    "event_date": normalize_event_date(it),
                    "amount": amount,
                    "reason": str(it.get("reason") or "参加費"),
                    "created_at": normalize_created_at(it),
                    "tx_id": it.get("tx_id"),
                    "joined_at": it.get("joined_at"),
                    # 便利なので追加（不要なら消してOK）
                    "schedule_id": it.get("schedule_id"),
                    "payment_type": it.get("payment_type"),
                    "source": it.get("source"),
                })

            print(f"[DEBUG] list_point_spends - user_id={user_id}, items={len(items)}, out={len(out)}")
            # 日時が欠けてる件をすぐ特定できる
            missing_ca = sum(1 for x in out if not x.get("created_at"))
            if missing_ca:
                print(f"[DEBUG] list_point_spends - created_at missing: {missing_ca}件（joined_at形式を確認）")

            return out

        except Exception as e:
            print(f"[WARN] list_point_spends failed: {e}")
            import traceback; traceback.print_exc()
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
    def get_user_stats(self, user_id: str, raw_history=None, spends=None, all_schedules=None):
        rules = PointRules(reset_days=60, first_participation_points=200)

        manual_points = self.get_manual_points(user_id)

        # 中学生以下の倍率（1.0なら割引なし、0.7なら30%割引）
        JUNIOR_HIGH_MULTIPLIER = 0.7

        user_info = self.get_user_info(user_id)
        is_junior_high = _is_junior_high_or_below(user_info)

        # 中学生以下なら指定倍率、それ以外は通常料金
        point_multiplier = JUNIOR_HIGH_MULTIPLIER if is_junior_high else 1.0

        # ★ raw_history が渡ってきたらそれを使う
        if raw_history is None:
            raw_history = self.get_user_participation_history_with_timestamp(user_id)

        records_all = normalize_participation_history(raw_history)

        if not records_all:
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
                'direct_registration_count': 0,
                'all_time_early_registration_count': 0,
                'all_time_direct_registration_count': 0,
                'is_junior_high': is_junior_high,
                'is_reset': False,
                'days_until_reset': None,
                'manual_points': manual_points,
            }

        # リセット判定
        last_reset_index, is_reset = calc_reset_index(records_all, rules.reset_days)
        records_for_points = slice_records_for_points(records_all, last_reset_index)
        print("[DBG] user_id=", user_id, "records_all=", len(records_all), "records_for_points=", len(records_for_points))

        # ★リセットが発生している場合、手動ポイントもリセット
        if last_reset_index > 0:
            print(f"[DBG] Reset occurred at index {last_reset_index}, clearing manual_points")
            manual_points = 0

        all_time_early, all_time_direct = calc_registration_counts(records_all, self._is_early_registration)

        pc = calc_participation_and_cumulative(
            records_all=records_all,
            records_for_points=records_for_points,
            rules=rules,
            point_multiplier=point_multiplier,
            is_early_registration_fn=self._is_early_registration,
        )
        participation_points = pc.get("participation_points", 0)
        cumulative_count = pc.get("cumulative_count", 0)
        cumulative_bonus_points = pc.get("cumulative_bonus_points", 0)
        early_registration_count = pc.get("early_registration_count", 0)
        direct_registration_count = pc.get("direct_registration_count", 0)

        monthly_bonus_points, monthly_bonuses = calc_monthly_bonus(records_for_points, point_multiplier)

        print(f"[DBG] participation_points={participation_points}, cumulative_bonus={cumulative_bonus_points}")
        print(f"[DBG] monthly_bonus_points={monthly_bonus_points}")

        # 連続ポイント
        from datetime import datetime
        today = datetime.now(JST).date()

        if all_schedules is None:
            all_schedules = self.get_all_past_schedules(today)
            all_schedules.sort(key=lambda s: datetime.strptime(s["date"], "%Y-%m-%d").date())

        if last_reset_index > 0 and records_for_points:
            reset_date = records_for_points[0].event_date.strftime('%Y-%m-%d')
            all_schedules = [s for s in all_schedules if s['date'] >= reset_date]
            print(f"\n[DEBUG] 連続参加チェック（{reset_date} 以降のスケジュールのみ）")
        else:
            print(f"\n[DEBUG] 連続参加チェック（全スケジュール）")

        streak_points, current_streak_count, max_streak, current_streak_start = calc_streak_points(
            records_for_points=records_for_points,
            all_schedules=all_schedules,
            rules=rules,
            point_multiplier=point_multiplier,
        )

        print(f"[DBG] streak_points={streak_points}, current_streak={current_streak_count}")

        # ★ spend 合計の取り方（amount 以外も吸収）
        def _pick_spend_amount(s: dict) -> int:
            v = s.get("points_used")
            if v is not None:
                try:
                    return int(v)
                except Exception:
                    pass

            v = s.get("delta_points")
            if v is not None:
                try:
                    return abs(int(v))
                except Exception:
                    pass

            v = s.get("amount")
            if v is not None:
                try:
                    return int(v)
                except Exception:
                    pass

            return 0

        # ★spends も渡ってきたらそれを使う（二重取得防止）
        # ※注意：spends が「直近20件」だと合計がズレるので、渡す側は limit=1000 推奨
        if spends is None:
            spends = self.list_point_spends(user_id, limit=1000)

        if last_reset_index > 0 and records_for_points:
            reset_date = records_for_points[0].event_date.strftime('%Y-%m-%d')
            spends_after_reset = [s for s in spends if (s.get("event_date", "") or "") >= reset_date]
            total_points_used = sum(_pick_spend_amount(s) for s in spends_after_reset)
            print(f"[DBG] Reset at {reset_date}: points_used={total_points_used} from {len(spends_after_reset)}/{len(spends)} records")
        else:
            total_points_used = sum(_pick_spend_amount(s) for s in spends)
            print(f"[DBG] total_points_used={total_points_used} from {len(spends)} records")

        last_dt = records_all[-1].event_date
        days_until_reset = calc_days_until_reset(last_dt, rules.reset_days)

        total_participation_all_time = len(records_all)

        uguu_points = (
            participation_points
            + streak_points
            + monthly_bonus_points
            + cumulative_bonus_points
            + manual_points
            - total_points_used
        )

        print(f"[DEBUG] Before reset check: uguu_points={uguu_points}, is_reset={is_reset}")

        if is_reset:
            print(f"[DEBUG] Resetting points! Before: uguu={uguu_points}, participation={participation_points}")
            uguu_points = 0
            participation_points = 0
            streak_points = 0
            monthly_bonus_points = 0
            cumulative_bonus_points = 0
            manual_points = 0
            print(f"[DEBUG] After reset: uguu={uguu_points}")

        print(f"[DEBUG] Final uguu_points={uguu_points}")

        return {
            'uguu_points': uguu_points,
            'participation_points': participation_points,
            'streak_points': streak_points,
            'monthly_bonus_points': monthly_bonus_points,
            'cumulative_bonus_points': cumulative_bonus_points,
            'points_used': total_points_used,

            'total_participation': total_participation_all_time,

            'early_registration_count': early_registration_count,
            'direct_registration_count': direct_registration_count,

            'all_time_early_registration_count': all_time_early,
            'all_time_direct_registration_count': all_time_direct,

            'last_participation_date': last_dt.strftime('%Y-%m-%d') if last_dt else None,
            'current_streak_start': current_streak_start,
            'current_streak_count': current_streak_count,
            'cumulative_count': cumulative_count,
            'monthly_bonuses': monthly_bonuses,
            'is_junior_high': is_junior_high,
            'is_reset': is_reset,
            'days_until_reset': days_until_reset,
            'manual_points': manual_points,
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

    # ===== ポイント失効関連メソッド =====
    
    def disable_user_points(self, user_id):
        """ユーザーのポイントを失効させる"""
        try:
            self.users_table.update_item(
                Key={'user_id': user_id},
                UpdateExpression='SET points_disabled = :val',
                ExpressionAttributeValues={':val': True}
            )
            print(f"[INFO] ポイント失効: user_id={user_id}")
            return True
        except Exception as e:
            print(f"[ERROR] ポイント失効失敗: {e}")
            return False

    def enable_user_points(self, user_id):
        """ユーザーのポイント失効を解除"""
        try:
            self.users_table.update_item(
                Key={'user_id': user_id},
                UpdateExpression='SET points_disabled = :val',
                ExpressionAttributeValues={':val': False}
            )
            print(f"[INFO] ポイント失効解除: user_id={user_id}")
            return True
        except Exception as e:
            print(f"[ERROR] ポイント失効解除失敗: {e}")
            return False

    def get_display_points(self, user_data):
        """
        表示用のポイントを取得
        points_disabled=Trueの場合は0を返す
        """
        if user_data.get('points_disabled', False):
            return 0
        return user_data.get('points', 0)
    

def record_spend(history_table, *, user_id: str, points_used: int,
                 event_date: str, payment_type: str = "event_participation",
                 reason: str | None = None, created_by: str | None = None):
    """
    bad-users-history に消費(spend)トランザクションを1件保存する。
    """
    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    tx_id = str(uuid4())
    joined_at = f"points#spend#{now}#{tx_id}"  # ← 既存設計を維持するならそのまま

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
        "created_at": now,   # レコード作成時刻
        "paid_at": now,      # ★支払い確定時刻（新規追加）
        "entity_type": "point_transaction",
        "version": 1,
    }

    if created_by is not None:
        item["created_by"] = created_by

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
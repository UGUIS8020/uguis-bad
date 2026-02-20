# --- 標準ライブラリ ---
import os, time, hashlib, json, base64
import uuid
import random
import calendar
import logging
import io
from io import BytesIO
from datetime import datetime, date, timedelta, timezone
from decimal import Decimal
from urllib.parse import urlparse, urljoin
from flask import current_app
import time

# --- サードパーティライブラリ ---
from dotenv import load_dotenv
from utils.timezone import JST
import requests
from PIL import Image, ExifTags
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
# --- Flask関連 ---
from flask import (
    Flask, render_template, request, redirect, url_for, flash,
    abort, session, jsonify, current_app, json
)
from flask_login import (
    UserMixin, LoginManager, login_user, logout_user,
    login_required, current_user
)
from flask_caching import Cache
from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileAllowed
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

# --- WTForms ---
from wtforms import (
    StringField, PasswordField, SubmitField, SelectField,
    DateField, BooleanField, IntegerField
)
from wtforms.validators import (
    DataRequired, Email, EqualTo, Length,
    Optional, NumberRange, ValidationError
)

# --- アプリ内モジュール ---
from utils import db
from utils.db import (
    get_schedule_table,
    get_schedules_with_formatting,
    get_schedules_with_formatting_all 
)

from uguu.post import post
from badminton_logs_functions import get_badminton_chat_logs
from uguu.dynamo import DynamoDB
from flask_wtf.csrf import CSRFProtect

logger = logging.getLogger(__name__)

login_manager = LoginManager()
cache = Cache()
csrf = CSRFProtect()   # ★グローバルで1回作る


def create_app():
    """アプリケーションの初期化と設定（唯一の create_app）"""
    load_dotenv()
    app = Flask(__name__)

    # --- 環境判定 ---
    APP_ENV = os.getenv("APP_ENV", "development").lower()
    IS_PROD = APP_ENV in ("production", "prod")
    IS_LOCAL_HTTP = not IS_PROD

    # --- Secret Key ---
    secret_key = os.getenv("SECRET_KEY")
    if IS_PROD and not secret_key:
        raise RuntimeError("SECRET_KEY is required in production")
    app.config["SECRET_KEY"] = secret_key or "dev-only-insecure-key"

    # --- セッション設定 ---
    app.config.update(
        PERMANENT_SESSION_LIFETIME=timedelta(days=30),
        SESSION_PERMANENT=True,
        SESSION_COOKIE_SECURE=not IS_LOCAL_HTTP,
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
    )

    # --- Cache ---
    app.config["CACHE_TYPE"] = "SimpleCache"
    app.config["CACHE_DEFAULT_TIMEOUT"] = 600
    app.config["CACHE_THRESHOLD"] = 900
    app.config["CACHE_KEY_PREFIX"] = "uguis_"
    cache.init_app(app)

    # --- CSRF（★ここが今回の核心） ---
    csrf.init_app(app)

    app.config["TABLE_NAME_USER"] = os.getenv("TABLE_NAME_USER")
    app.config["TABLE_NAME_SCHEDULE"] = os.getenv("TABLE_NAME_SCHEDULE")

    # --- AWS ---
    aws_credentials = {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "region_name": os.getenv("AWS_REGION", "ap-northeast-1"),
    }
    required_env_vars = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "S3_BUCKET",
        "TABLE_NAME_USER",
        "TABLE_NAME_SCHEDULE",
    ]
    missing = [v for v in required_env_vars if not os.getenv(v)]
    if IS_PROD and missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    app.config["S3_BUCKET"] = os.getenv("S3_BUCKET", "default-bucket-name")
    app.config["AWS_REGION"] = os.getenv("AWS_REGION", "ap-northeast-1")
    app.config["S3_LOCATION"] = f"https://{app.config['S3_BUCKET']}.s3.{app.config['AWS_REGION']}.amazonaws.com/"

    app.s3 = boto3.client("s3", **aws_credentials)
    app.dynamodb = boto3.resource("dynamodb", **aws_credentials)

    app.table_name_users = os.getenv("TABLE_NAME_USER")
    app.table_name_schedule = os.getenv("TABLE_NAME_SCHEDULE")

    app.table_name = app.table_name_users

    app.table = app.dynamodb.Table(app.table_name_users)
    app.table_schedule = app.dynamodb.Table(app.table_name_schedule)

    app.bad_table_name = os.getenv("BAD_TABLE_NAME", "bad_items")
    app.bad_table = app.dynamodb.Table(app.bad_table_name)

    # --- Flask-Login ---
    login_manager.init_app(app)
    login_manager.session_protection = "strong"
    login_manager.login_view = os.getenv("LOGIN_VIEW_ENDPOINT", "login")
    login_manager.login_message = "このページにアクセスするにはログインが必要です。"

    app.uguu_db = DynamoDB()

    log_level = logging.INFO 
    
    logging.basicConfig(
        level=log_level,  # 変数を使って INFO (または logging.INFO) に固定
        format='[%(levelname)s] %(name)s: %(message)s',
        force=True
    )
    
    app.logger.setLevel(log_level)   

    return app

app = create_app()

from flask_wtf.csrf import CSRFError

@app.errorhandler(CSRFError)
def handle_csrf_error(e):
    current_app.logger.error(f"[CSRFError] {e.description}")
    return e.description, 400

def tokyo_time():
    return datetime.now(JST)


@login_manager.user_loader
def load_user(user_id):
    if not user_id:
        current_app.logger.warning("No user_id provided to load_user")
        return None

    try:
        table_name = current_app.config.get("TABLE_NAME_USER")
        if not table_name:
            current_app.logger.error("TABLE_NAME_USER is not configured")
            return None

        table = current_app.dynamodb.Table(table_name)

        resp = table.get_item(Key={"user#user_id": user_id})
        item = resp.get("Item")
        if not item:
            current_app.logger.info("No user found for ID: %s", user_id)
            return None

        return User.from_dynamodb_item(item)

    except Exception as e:
        current_app.logger.error("Error loading user %s: %s", user_id, e, exc_info=True)
        return None


class RegistrationForm(FlaskForm):
    organization = SelectField('所属', choices=[('', '選択してください'), ('鶯', '鶯'), ('gest', 'ゲスト'), ('Boot_Camp15', 'Boot Camp15'), ('other', 'その他'),], default='', validators=[DataRequired(message='所属を選択してください')])
    display_name = StringField('表示名 LINE名など', validators=[DataRequired(message='表示名を入力してください'), Length(min=1, max=30, message='表示名は1文字以上30文字以下で入力してください')])
    user_name = StringField('ユーザー名', validators=[DataRequired()])
    furigana = StringField('フリガナ', validators=[DataRequired()])
    phone = StringField('電話番号', validators=[DataRequired(), Length(min=10, max=15, message='正しい電話番号を入力してください')])
    post_code = StringField('郵便番号', validators=[DataRequired(), Length(min=7, max=7, message='ハイフン無しで７桁で入力してください')])
    address = StringField('住所', validators=[DataRequired(), Length(max=100, message='住所は100文字以内で入力してください')])
    email = StringField('メールアドレス', validators=[DataRequired(), Email(message='正しいメールアドレスを入力してください')])
    email_confirm = StringField('メールアドレス確認', validators=[DataRequired(), Email(), EqualTo('email', message='メールアドレスが一致していません')])
    password = PasswordField('8文字以上のパスワード', validators=[DataRequired(), Length(min=8, message='パスワードは8文字以上で入力してください'), EqualTo('pass_confirm', message='パスワードが一致していません')])
    pass_confirm = PasswordField('パスワード(確認)', validators=[DataRequired()])    
    gender = SelectField('性別', choices=[('', '性別'), ('male', '男性'), ('female', '女性')], validators=[DataRequired()])
    date_of_birth = DateField('生年月日', format='%Y-%m-%d', validators=[DataRequired()])
    guardian_name = StringField('保護者氏名', validators=[Optional()])  
    emergency_phone = StringField('緊急連絡先電話番号', validators=[Optional(), Length(min=10, max=15, message='正しい電話番号を入力してください')])
    badminton_experience = SelectField(
        'バドミントン歴', 
        choices=[
            ('', 'バドミントン歴を選択してください'),
            ('未経験', '未経験'),
            ('1年未満', '1年未満'),
            ('1-3年未満', '1-3年未満'),
            ('3年以上', '3年以上')
        ], 
        validators=[
            DataRequired(message='バドミントン歴を選択してください')
        ]
    )
    submit = SubmitField('登録')

    def validate_guardian_name(self, field):
        if self.date_of_birth.data:
            today = date.today()
            age = today.year - self.date_of_birth.data.year - ((today.month, today.day) < (self.date_of_birth.data.month, self.date_of_birth.data.day))
            if age < 18 and not field.data:
                raise ValidationError('18歳未満の方は保護者氏名の入力が必要です')

    def validate_email(self, field):
        try:
            table = current_app.table
            email = (field.data or "").strip().lower()

            response = table.query(
                IndexName="email-index",
                KeyConditionExpression=Key("email").eq(email)
            )

            if response.get("Items"):
                raise ValidationError("入力されたメールアドレスは既に登録されています。")

        except ValidationError:
            raise
        except Exception as e:
            current_app.logger.error(f"Error validating email: {str(e)}", exc_info=True)
            raise ValidationError("メールアドレスの確認中にエラーが発生しました。")
                    
        
class UpdateUserForm(FlaskForm):
    organization = SelectField('所属', choices=[('鶯', '鶯'), ('gest', 'ゲスト'), ('Boot_Camp15', 'Boot Camp15'), ('other', 'その他')], default='鶯', validators=[DataRequired(message='所属を選択してください')])
    display_name = StringField('表示名 LINE名など', validators=[DataRequired(), Length(min=1, max=30)])
    user_name = StringField('ユーザー名', validators=[DataRequired()])
    furigana = StringField('フリガナ', validators=[Optional()])
    phone = StringField('電話番号', validators=[Optional(), Length(min=10, max=15)])
    post_code = StringField('郵便番号', validators=[Optional(), Length(min=7, max=7)])
    address = StringField('住所', validators=[Optional(), Length(max=100)])    
    email = StringField('メールアドレス', validators=[DataRequired(), Email()])
    email_confirm = StringField('確認用メールアドレス', validators=[Optional(), Email()])
    password = PasswordField('パスワード', validators=[Optional(), Length(min=8), EqualTo('pass_confirm', message='パスワードが一致していません')])
    pass_confirm = PasswordField('パスワード(確認)')
    gender = SelectField('性別', choices=[('male', '男性'), ('female', '女性')], validators=[Optional()])
    date_of_birth = DateField('生年月日', format='%Y-%m-%d', validators=[Optional()])
    guardian_name = StringField('保護者氏名', validators=[Optional()])    
    emergency_phone = StringField('緊急連絡先電話番号', validators=[Optional(), Length(min=10, max=15, message='正しい電話番号を入力してください')])
    badminton_experience = SelectField(
        'バドミントン歴', 
        choices=[
            ('', 'バドミントン歴を選択してください'),
            ('未経験', '未経験'),
            ('1年未満', '1年未満'),
            ('1-3年未満', '1-3年未満'),
            ('3年以上', '3年以上')
        ], 
        validators=[
            DataRequired(message='バドミントン歴を選択してください')
        ]
    )
    profile_image = FileField('プロフィール画像', validators=[
        FileAllowed(['jpg', 'png', 'jpeg', 'gif'], '画像ファイルのみ許可されます。')
    ])

    submit = SubmitField('更新')

    def __init__(self, user_id, dynamodb_table, *args, **kwargs):
        super(UpdateUserForm, self).__init__(*args, **kwargs)
        self.id = f'user#{user_id}'
        self.table = dynamodb_table

         # フィールドを初期化
        self.email_readonly = True  # デフォルトでは編集不可

    def validate_email_confirm(self, field):
        # フォームでemailが変更されていない場合は何もしない
        if self.email_readonly:
            return

        # email_confirmが空の場合のエラーチェック
        if not field.data:
            raise ValidationError('確認用メールアドレスを入力してください。')

        # email_confirmが入力されている場合のみ一致を確認
        if field.data != self.email.data:
            raise ValidationError('メールアドレスが一致していません。再度入力してください。')
            

    def validate_email(self, field):
        if self.email_readonly or not field.data:
            return

        try:
            email = (field.data or "").strip().lower()

            response = self.table.query(
                IndexName="email-index",
                KeyConditionExpression=Key("email").eq(email)
            )

            for item in response.get("Items", []):
                user_id = item.get("user#user_id") or item.get("user_id")
                if user_id and user_id != self.id:
                    raise ValidationError("このメールアドレスは既に使用されています。")

        except ValidationError:
            raise
        except Exception as e:
            current_app.logger.error(f"Unexpected error querying DynamoDB: {e}", exc_info=True)
            raise ValidationError("メールアドレスの確認中にエラーが発生しました。")


class TempRegistrationForm(FlaskForm):
    # 表示名
    display_name = StringField(
        '表示名', 
        validators=[
            DataRequired(message='表示名を入力してください'),
            Length(min=1, max=30, message='表示名は1文字以上30文字以下で入力してください')
        ]
    )

    # 名前
    user_name = StringField(
        '名前',
        validators=[
            DataRequired(message='名前を入力してください'),
            Length(min=1, max=30, message='名前は1文字以上30文字以下で入力してください')
        ]
    )
    
    # 性別
    gender = SelectField(
        '性別', 
        choices=[
            ('', '性別を選択してください'),
            ('male', '男性'),
            ('female', '女性')
        ], 
        validators=[
            DataRequired(message='性別を選択してください')
        ]
    )

    date_of_birth = DateField(
        '生年月日',
        format='%Y-%m-%d',
        validators=[DataRequired(message='生年月日を入力してください')]
    )
    
    # バドミントン歴
    badminton_experience = SelectField(
        'バドミントン歴', 
        choices=[
            ('', 'バドミントン歴を選択してください'),
            ('未経験', '未経験'),
            ('1年未満', '1年未満'),
            ('1-3年未満', '1-3年未満'),
            ('3-5年未満', '3-5年未満'),
            ('5年以上', '5年以上')
        ], 
        validators=[
            DataRequired(message='バドミントン歴を選択してください')
        ]
    )

    # 電話番号
    phone = StringField(
        '電話番号',
        validators=[
            DataRequired(message='電話番号を入力してください'),
            Length(min=10, max=15, message='正しい電話番号を入力してください')
        ]
    )
    
    # メールアドレス
    email = StringField(
        'メールアドレス', 
        validators=[
            DataRequired(message='メールアドレスを入力してください'),
            Email(message='正しいメールアドレスを入力してください')
        ],
        # HTML属性をPython側で一括管理
        render_kw={
            "type": "email",
            "inputmode": "email",
            "autocomplete": "email",
            "autocapitalize": "none",
            "autocorrect": "off",
            "spellcheck": "false"
        }
    )

    # メールアドレス確認
    confirm_email = StringField(
        'メールアドレス（確認）',
        validators=[
            DataRequired(message='確認用メールアドレスを入力してください'),
            Email(message='正しいメールアドレスを入力してください'),
            EqualTo('email', message='メールアドレスが一致しません')
        ],
        # emailと同じ設定を適用（コピペの手間を減らすなら共通変数にしてもOK）
        render_kw={
            "type": "email",
            "inputmode": "email",
            "autocomplete": "email",
            "autocapitalize": "none",
            "autocorrect": "off",
            "spellcheck": "false"
        }
    )
    
    # パスワード
    password = PasswordField(
        'パスワード', 
        validators=[
            DataRequired(message='パスワードを入力してください'),
            Length(min=8, message='パスワードは8文字以上で入力してください')
        ]
    )

    # パスワード確認
    confirm_password = PasswordField(
        'パスワード（確認）',
        validators=[
            DataRequired(message='確認用パスワードを入力してください'),
            EqualTo('password', message='パスワードが一致しません')
        ]
    )
    
    # 登録ボタン
    submit = SubmitField('仮登録')  

    def validate_email(self, field):
        try:
            table = current_app.table  # create_app() で app.table を作っている前提
            email = (field.data or "").strip().lower()

            current_app.logger.debug(f"Querying email-index for email: {email}")

            response = table.query(
                IndexName="email-index",
                KeyConditionExpression=Key("email").eq(email)
            )

            if response.get("Items"):
                raise ValidationError("このメールアドレスは既に使用されています。他のメールアドレスをお試しください。")

        except ValidationError:
            raise
        except Exception as e:
            current_app.logger.error(f"Error validating email: {e}", exc_info=True)
            raise ValidationError("メールアドレスの確認中にエラーが発生しました。")


class LoginForm(FlaskForm):
    email = StringField(
        'メールアドレス',
        validators=[
            DataRequired(message='メールアドレスを入力してください'),
            Email(message='正しいメールアドレスの形式で入力してください')
        ],
        # ↓↓↓ 入力モードと自動補完の設定を追加 ↓↓↓
        render_kw={
            "type": "email",
            "inputmode": "email",
            "autocomplete": "email"
        }
    )
    password = PasswordField(
        'パスワード', 
        validators=[DataRequired(message='パスワードを入力してください')],
        # ↓↓↓ 半角英数字入力を促し、マネージャーとの連携を強化 ↓↓↓
        render_kw={
            "inputmode": "verbatim",
            "autocomplete": "current-password"
        }
    )
    remember = BooleanField('ログイン状態を保持する')
    submit = SubmitField('ログイン')

    def __init__(self, *args, **kwargs):
        super(LoginForm, self).__init__(*args, **kwargs)
        self.user = None

    def validate_email(self, field):
        """メールアドレスの存在確認"""
        try:
            table = current_app.table  # create_appで app.table を作ってる前提
            email = (field.data or "").strip().lower()

            response = table.query(
                IndexName="email-index",
                KeyConditionExpression=Key("email").eq(email)
            )

            items = response.get("Items", [])
            if not items:
                raise ValidationError("このメールアドレスは登録されていません")

            self.user = items[0]
            current_app.logger.debug(f"User found for email: {email}")

        except ValidationError:
            raise
        except Exception as e:
            current_app.logger.error(f"Login error: {e}", exc_info=True)
            raise ValidationError("ログイン処理中にエラーが発生しました")

    def validate_password(self, field):
        """パスワードの検証"""
        if not self.user:
            raise ValidationError('先にメールアドレスを確認してください')

        stored_hash = self.user.get('password')
        app.logger.debug(f"Retrieved user: {self.user}")
        app.logger.debug(f"Stored hash: {stored_hash}")
        if not stored_hash:
            app.logger.error("No password hash found in user data")
            raise ValidationError('登録情報が正しくありません')

        app.logger.debug("Validating password against stored hash")
        if not check_password_hash(stored_hash, field.data):
            app.logger.debug("Password validation failed")
            raise ValidationError('パスワードが正しくありません')

class User(UserMixin):
    def __init__(self, user_id, display_name, user_name, furigana, email, password_hash,
                 gender, date_of_birth, post_code, address, phone, guardian_name, emergency_phone, badminton_experience,
                 organization='other', administrator=False, 
                 created_at=None, updated_at=None, profile_image_url=None):
        super().__init__()
        self.id = user_id
        self.user_id = user_id  
        self.display_name = display_name
        self.user_name = user_name
        self.furigana = furigana
        self.email = email 
        self._password_hash = password_hash
        self.gender = gender
        self.date_of_birth = date_of_birth
        self.post_code = post_code
        self.address = address
        self.phone = phone
        self.guardian_name = guardian_name 
        self.emergency_phone = emergency_phone 
        self.organization = organization
        self.badminton_experience = badminton_experience
        self.administrator = administrator
        self.created_at = created_at or datetime.now().isoformat()        
        self.updated_at = updated_at or datetime.now().isoformat()
        self.profile_image_url = profile_image_url

    def check_password(self, password):
        return check_password_hash(self._password_hash, password)  # _password_hashを使用

    @property
    def is_admin(self):
        return self.administrator    
   

    @staticmethod
    def from_dynamodb_item(item):
        def get_value(field, default=None):
            return item.get(field, default)
        
        # user# プレフィックスを除去してUIDだけにする
        user_id = get_value('user#user_id') or ""

        return User(
            user_id=user_id,
            display_name=get_value('display_name'),
            user_name=get_value('user_name'),
            furigana=get_value('furigana'),
            email=get_value('email'),
            password_hash=get_value('password'),  # 修正：password フィールドを取得
            gender=get_value('gender'),
            date_of_birth=get_value('date_of_birth'),
            post_code=get_value('post_code'),
            address=get_value('address'),
            phone=get_value('phone'),
            guardian_name=get_value('guardian_name', default=None),
            emergency_phone=get_value('emergency_phone', default=None),
            organization=get_value('organization', default='other'),
            badminton_experience=get_value('badminton_experience'),
            administrator=bool(get_value('administrator', False)),
            created_at=get_value('created_at'),
            updated_at=get_value('updated_at'),
            profile_image_url=get_value('profile_image_url')
        )

    def to_dynamodb_item(self):
        fields = ['user_id', 'organization', 'address', 'administrator', 'created_at', 
                  'display_name', 'email', 'furigana', 'gender', 'password', 
                  'phone', 'post_code', 'updated_at', 'user_name','guardian_name', 'emergency_phone']
        item = {field: {"S": str(getattr(self, field))} for field in fields if getattr(self, field, None)}
        item['administrator'] = {"BOOL": self.administrator}
        if self.date_of_birth:
            item['date_of_birth'] = {"S": str(self.date_of_birth)}
        
        return item
        

# @cache.memoize(timeout=600)
# def get_participants_info(schedule):     
#     participants_info = []

#     try:
#         user_table = app.dynamodb.Table(app.table_name)

#         if 'participants' in schedule and schedule['participants']:            
#             for participant_id in schedule['participants']:
#                 try:
#                     response = user_table.scan(
#                         FilterExpression='contains(#uid, :pid)',
#                         ExpressionAttributeNames={'#uid': 'user#user_id'},
#                         ExpressionAttributeValues={':pid': participant_id}
#                     )
#                     if response.get('Items'):
#                         user = response['Items'][0]                        
                        
#                         raw_score = user.get('skill_score')
#                         if isinstance(raw_score, Decimal):
#                             skill_score = int(raw_score)
#                         elif isinstance(raw_score, (int, float)):
#                             skill_score = int(raw_score)
#                         else:
#                             skill_score = None

#                         participants_info.append({                            
#                             'user_id': user.get('user#user_id'),
#                             'display_name': user.get('display_name', '名前なし'),
#                             'skill_score': skill_score
#                         })
#                     else:
#                         logger.warning(f"[参加者ID: {participant_id}] ユーザーが見つかりませんでした。")
#                 except Exception as e:
#                     app.logger.error(f"参加者情報の取得中にエラー（ID: {participant_id}）: {str(e)}")

#     except Exception as e:
#         app.logger.error(f"参加者情報の全体取得中にエラー: {str(e)}")

#     return participants_info


# 緊急修正版: scanをget_itemに変更
# @cache.memoize(timeout=600)
# def get_participants_info(schedule):     
#     participants_info = []

#     try:
#         user_table = app.dynamodb.Table(app.table_name)

#         if 'participants' in schedule and schedule['participants']:            
#             for participant_id in schedule['participants']:
#                 try:
#                     # scanを削除してget_itemに変更
#                     response = user_table.get_item(
#                         Key={'user#user_id': participant_id}
#                     )
                    
#                     if 'Item' in response:
#                         user = response['Item']                        
                        
#                         raw_score = user.get('skill_score')
#                         if isinstance(raw_score, Decimal):
#                             skill_score = int(raw_score)
#                         elif isinstance(raw_score, (int, float)):
#                             skill_score = int(raw_score)
#                         else:
#                             skill_score = None

#                         # 参加回数（practice_count）を取得
#                         raw_practice = user.get('practice_count')
#                         join_count = int(raw_practice) if isinstance(raw_practice, (Decimal, int, float)) else None

#                         participants_info.append({                            
#                             'user_id': user.get('user#user_id'),
#                             'display_name': user.get('display_name', '名前なし'),
#                             'skill_score': skill_score,
#                             'join_count': join_count
#                         })
#                     else:
#                         logger.warning(f"[参加者ID: {participant_id}] ユーザーが見つかりませんでした。")
#                 except Exception as e:
#                     app.logger.error(f"参加者情報の取得中にエラー（ID: {participant_id}）: {str(e)}")

#     except Exception as e:
#         app.logger.error(f"参加者情報の全体取得中にエラー: {str(e)}")

#     return participants_info

@cache.memoize(timeout=600)
def get_participants_info(schedule):
    participants_info = []
    try:
        table_name = current_app.config.get("TABLE_NAME_USER")
        dynamodb = current_app.dynamodb

        raw = schedule.get("participants") or []

        ids = []
        seen = set()
        for x in raw:
            uid = (x.get("user_id") or x.get("user#user_id")) if isinstance(x, dict) else x
            if uid:
                s = str(uid)
                if s not in seen:
                    seen.add(s)
                    ids.append(s)

        if not ids:
            return participants_info

        request = {
            table_name: {
                "Keys": [{"user#user_id": uid}],
                "ProjectionExpression": "#uid, display_name, profile_image_url, skill_score, practice_count",
                "ExpressionAttributeNames": {"#uid": "user#user_id"}
            }
        }

        responses = []
        unprocessed = request

        client = dynamodb.meta.client if hasattr(dynamodb, "meta") else dynamodb

        for attempt in range(5):
            resp = client.batch_get_item(RequestItems=unprocessed)
            responses.extend(resp.get("Responses", {}).get(table_name, []))
            unprocessed = resp.get("UnprocessedKeys") or {}
            if not unprocessed:
                break
            time.sleep(0.1 * (attempt + 1))

        by_id = {it["user#user_id"]: it for it in responses}

        # 修正後：1つのループに統合 ✅
        for uid in ids:
            pk = uid
            user = by_id.get(pk)
            if user:
                raw_score = user.get("skill_score")
                skill_score = int(raw_score) if isinstance(raw_score, (int, float, Decimal)) else None

                raw_practice = user.get("practice_count")
                join_count = int(raw_practice) if isinstance(raw_practice, (int, float, Decimal)) else None

                url = (user.get("profile_image_url") or "").strip()
                profile_image_url = url if url and url.lower() != "none" else None

                participants_info.append({
                    "user_id": user.get("user#user_id"),
                    "display_name": user.get("display_name", "名前なし"),
                    "skill_score": skill_score,
                    "join_count": join_count,
                    "profile_image_url": profile_image_url,
                    "is_valid": True,
                })
            else:
                participants_info.append({
                    "user_id": uid,
                    "display_name": "削除されたユーザー",
                    "skill_score": None,
                    "join_count": None,
                    "profile_image_url": None,
                    "is_deleted": True,
                    "is_valid": False,
                })

    except Exception as e:
        logger.exception(f"参加者情報の取得中にエラー: {e}")

    participants_info.sort(
        key=lambda x: (x.get("join_count") is None, (x.get("join_count") or 0), x.get("display_name",""))
    )
    return participants_info


# さらに最適化版: バッチ取得
@cache.memoize(timeout=600) 
def get_all_users_dict():
    """全ユーザーを1回のscanで取得し辞書として返す"""
    try:
        user_table = app.dynamodb.Table(app.table_name)
        response = user_table.scan()  # 1回だけscan
        
        users_dict = {}
        for user in response.get('Items', []):
            user_id = user.get('user#user_id')
            if user_id:
                raw_score = user.get('skill_score')
                if isinstance(raw_score, Decimal):
                    skill_score = int(raw_score)
                elif isinstance(raw_score, (int, float)):
                    skill_score = int(raw_score)
                else:
                    skill_score = None
                
                users_dict[user_id] = {
                    'user_id': user_id,
                    'display_name': user.get('display_name', '名前なし'),
                    'skill_score': skill_score
                }
        
        logger.info(f"ユーザー辞書を作成: {len(users_dict)}人")
        return users_dict
    except Exception as e:
        app.logger.error(f"ユーザー一括取得エラー: {str(e)}")
        return {}

@app.template_filter('format_date')
def format_date(value, fmt='%m/%d'):
    """
    日付文字列/ISO文字列を受け取り、指定フォーマットで返す。
    既定は 'MM/DD'。例: {{ value|format_date('%Y年%m月%d日') }}
    """
    if not value:
        return value
    s = str(value)

    # まず先頭10桁 'YYYY-MM-DD' を優先的に解釈
    try:
        dt = datetime.strptime(s[:10], "%Y-%m-%d")
    except Exception:
        # ダメなら ISO8601 を試す（Z -> +00:00）
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return value  # どれもダメなら原文返し

    try:
        return dt.strftime(fmt)
    except Exception:
        return value


@app.route('/schedules')
def get_schedules():
    schedules = get_schedules_with_formatting()
    return jsonify(schedules)     

# @app.route("/", methods=['GET'])
# @app.route("/index", methods=['GET'])
# def index():
#     try:
#         start_time = time.time()
        
#         # 軽量なスケジュール情報のみ取得
#         schedules = get_schedules_with_formatting()

#         # DynamoDB ユーザーテーブル
#         user_table = current_app.dynamodb.Table(app.table_name)

#         db_access_count = 0  # DynamoDBアクセス回数をカウント
#         total_users = 0  # 取得したユーザー数
        
#         for schedule in schedules:
#             # --- 参加者 ---
#             participants_info = []
#             raw_participants = schedule.get("participants", [])
#             user_ids = []

#             # 参加者IDの抽出（複数形式に対応）
#             for item in raw_participants:
#                 if isinstance(item, dict) and "S" in item:
#                     user_ids.append(item["S"])
#                 elif isinstance(item, dict) and "user_id" in item:
#                     user_ids.append(item["user_id"])
#                 elif isinstance(item, str):
#                     user_ids.append(item)

#             # ユーザー詳細を取得
#             db_start = time.time()
#             for uid in user_ids:
#                 try:
#                     res = user_table.get_item(Key={"user#user_id": uid})
#                     db_access_count += 1  # アクセスカウント
#                     user = res.get("Item")
#                     if not user:
#                         continue

#                     total_users += 1

#                     # 画像URLは複数候補からフォールバック
#                     url = (user.get("profile_image_url")
#                            or user.get("profileImageUrl")
#                            or user.get("large_image_url")
#                            or "")
#                     url = url.strip() if isinstance(url, str) else None

#                     raw_practice = user.get("practice_count")
#                     try:
#                         join_count = int(raw_practice) if raw_practice is not None else 0
#                     except (ValueError, TypeError):
#                         join_count = 0

#                     participants_info.append({
#                         "user_id": user["user#user_id"],
#                         "display_name": user.get("display_name", "不明"),
#                         "profile_image_url": url if url and url.lower() != "none" else None,
#                         "is_admin": bool(user.get("administrator")),
#                         "join_count": join_count,
#                     })
#                 except Exception as e:
#                     logger.error(f"参加者取得エラー ({uid}): {e}")
#                     pass

#             # 管理者を先頭にソート
#             participants_info.sort(
#                 key=lambda x: (not x.get("is_admin", False), (x.get("join_count") or 0), x.get("display_name",""))
#             )
#             schedule["participants_info"] = participants_info

#             # --- たら参加者 ---
#             tara_participants_info = []
#             raw_tara = schedule.get("tara_participants", [])
#             tara_ids = []

#             for item in raw_tara:
#                 if isinstance(item, dict) and "S" in item:
#                     tara_ids.append(item["S"])
#                 elif isinstance(item, dict) and "user_id" in item:
#                     tara_ids.append(item["user_id"])
#                 elif isinstance(item, str):
#                     tara_ids.append(item)

#             for uid in tara_ids:
#                 try:
#                     res = user_table.get_item(Key={"user#user_id": uid})
#                     db_access_count += 1  # アクセスカウント
#                     user = res.get("Item")
#                     if not user:
#                         continue

#                     total_users += 1

#                     url = (user.get("profile_image_url")
#                            or user.get("profileImageUrl")
#                            or user.get("large_image_url")
#                            or "")
#                     url = url.strip() if isinstance(url, str) else None

#                     tara_participants_info.append({
#                         "user_id": user["user#user_id"],
#                         "display_name": user.get("display_name", "不明"),
#                         "profile_image_url": url if url and url.lower() != "none" else None,
#                         "is_admin": bool(user.get("administrator")),
#                     })
#                 except Exception as e:
#                     logger.error(f"たら参加者取得エラー ({uid}): {e}")
#                     pass

#             tara_participants_info.sort(key=lambda x: not x.get("is_admin", False))
#             schedule["tara_participants_info"] = tara_participants_info
#             schedule["tara_participants"] = tara_ids
#             schedule["tara_count"] = len(tara_ids)

#         total_time = time.time() - start_time
        
#         # ★ パフォーマンスログ（旧方式）
#         logger.info(f"[index:旧方式] DynamoDBアクセス回数: {db_access_count}回")
#         logger.info(f"[index:旧方式] 取得ユーザー数: {total_users}人")
#         logger.info(f"[index:旧方式] 合計処理時間: {total_time:.3f}秒")
#         logger.info(f"[index:旧方式] 平均アクセス時間: {(total_time/db_access_count):.3f}秒/回" if db_access_count > 0 else "[index:旧方式] アクセスなし")

#         # トップ画像
#         image_files = [
#             'images/top001.jpg',
#             'images/top002.jpg',  # ★ typo修正
#             'images/top003.jpg',
#             'images/top004.jpg',
#             'images/top005.jpg'
#         ]
#         selected_image = random.choice(image_files)

#         return render_template(
#             "index.html",
#             schedules=schedules,
#             selected_image=selected_image,
#             canonical=url_for('index', _external=True)
#         )

#     except Exception as e:
#         logger.error(f"[index] スケジュール取得エラー: {e}")
#         flash('スケジュールの取得中にエラーが発生しました', 'error')
#         return render_template(
#             "index.html",
#             schedules=[],
#             selected_image='images/default.jpg'
#         )


@app.route("/", methods=["GET"])
@app.route("/index", methods=["GET"])
def index():
    start_time = time.time()

    try:
        schedules = get_schedules_with_formatting()

        table_name = (
            current_app.config.get("TABLE_NAME_USER")
            or getattr(current_app, "table_name", None)
        )
        if not table_name:
            raise RuntimeError("TABLE_NAME_USER is not configured")

        # =========================================================
        # 1) 全スケジュールから user_id(UUID) を集める
        # =========================================================
        all_user_ids = set()

        def _extract_uid(item):
            # DynamoDB low-level形式: {"S": "..."}
            if isinstance(item, dict) and "S" in item:
                return item.get("S")
            # dict形式: {"user_id": "..."}
            if isinstance(item, dict) and "user_id" in item:
                return item.get("user_id")
            # 文字列: "uuid..."
            if isinstance(item, str):
                return item
            return None

        for schedule in schedules:
            for item in schedule.get("participants", []) or []:
                uid = _extract_uid(item)
                if uid:
                    all_user_ids.add(uid)

            for item in schedule.get("tara_participants", []) or []:
                uid = _extract_uid(item)
                if uid:
                    all_user_ids.add(uid)

        current_app.logger.info("[index] 取得するユーザー数: %d", len(all_user_ids))

        # =========================================================
        # 2) DynamoDB BatchGet（100件ずつ）
        #    - user# は使わない（UUIDのみ）
        #    - UnprocessedKeys をリトライして取りこぼし防止
        # =========================================================
        user_cache = {}
        user_ids_list = list(all_user_ids)

        # current_app.dynamodb が resource / client どちらでも動くようにする
        dynamo_client = getattr(current_app.dynamodb, "meta", None)
        dynamo_client = dynamo_client.client if dynamo_client else current_app.dynamodb

        batch_count = 0
        batch_start = time.time()

        def _cache_user(user_item: dict):
            pk = user_item.get("user#user_id")  # この属性名はそのまま（値はUUIDのみ）
            if pk:
                user_cache[pk] = user_item

        for i in range(0, len(user_ids_list), 100):
            batch_ids = user_ids_list[i:i + 100]
            keys = [{"user#user_id": uid} for uid in batch_ids if uid]

            request_items = {table_name: {"Keys": keys}}
            tries = 0

            while request_items and tries < 5:
                tries += 1
                try:
                    resp = dynamo_client.batch_get_item(RequestItems=request_items)
                    batch_count += 1

                    for user_item in resp.get("Responses", {}).get(table_name, []):
                        _cache_user(user_item)

                    # 取りこぼしがある場合は再試行
                    request_items = resp.get("UnprocessedKeys", {})
                    if request_items:
                        time.sleep(min(0.2 * tries, 1.0))

                except Exception as e:
                    current_app.logger.error("[index] バッチ取得エラー: %s", e, exc_info=True)
                    break

        batch_time = time.time() - batch_start
        current_app.logger.info(
            "[index] DynamoDBバッチ取得: %d回, %.3f秒, キャッシュ件数: %d",
            batch_count, batch_time, len(user_cache)
        )

        # =========================================================
        # 3) キャッシュから participants_info を構築
        # =========================================================
        process_start = time.time()

        def _pick_profile_url(user: dict):
            url = (
                user.get("profile_image_url")
                or user.get("profileImageUrl")
                or user.get("large_image_url")
                or ""
            )
            url = url.strip() if isinstance(url, str) else ""
            return url if url and url.lower() != "none" else None

        def _to_int(v, default=0):
            try:
                return int(v)
            except Exception:
                return default

        def _get_user(uid: str):
            return user_cache.get(uid)

        for schedule in schedules:
            # --- 参加者 ---
            user_ids = []
            for item in schedule.get("participants", []) or []:
                uid = _extract_uid(item)
                if uid:
                    user_ids.append(uid)

            participants_info = []
            for uid in user_ids:
                user = _get_user(uid)
                if not user:
                    continue

                participants_info.append({
                    "user_id": user.get("user#user_id"),  # 値はUUID
                    "display_name": user.get("display_name", "不明"),
                    "profile_image_url": _pick_profile_url(user),
                    "is_admin": bool(user.get("administrator")),
                    "join_count": _to_int(user.get("practice_count"), 0),
                })

            participants_info.sort(
                key=lambda x: (
                    not x.get("is_admin", False),
                    (x.get("join_count") or 0),
                    x.get("display_name", "")
                )
            )
            schedule["participants_info"] = participants_info

            # --- たら参加者 ---
            tara_ids = []
            for item in schedule.get("tara_participants", []) or []:
                uid = _extract_uid(item)
                if uid:
                    tara_ids.append(uid)

            tara_participants_info = []
            for uid in tara_ids:
                user = _get_user(uid)
                if not user:
                    continue

                tara_participants_info.append({
                    "user_id": user.get("user#user_id"),
                    "display_name": user.get("display_name", "不明"),
                    "profile_image_url": _pick_profile_url(user),
                    "is_admin": bool(user.get("administrator")),
                })

            tara_participants_info.sort(key=lambda x: not x.get("is_admin", False))
            schedule["tara_participants_info"] = tara_participants_info
            schedule["tara_participants"] = tara_ids
            schedule["tara_count"] = len(tara_ids)

        process_time = time.time() - process_start
        total_time = time.time() - start_time

        current_app.logger.info("[index] 参加者情報処理: %.3f秒", process_time)
        current_app.logger.info("[index] 合計処理時間: %.3f秒", total_time)

        # =========================================================
        # 4) ランダム背景画像
        # =========================================================
        image_files = [f"images/top{i:03d}.jpg" for i in range(1, 9)]
        selected_image = random.choice(image_files)

        return render_template(
            "index.html",
            schedules=schedules,
            selected_image=selected_image,
            canonical=url_for("index", _external=True),
        )

    except Exception as e:
        current_app.logger.error("[index] エラー: %s", e, exc_info=True)
        # 既存テンプレがあるならエラーページへ。なければ最低限 index に落とすなどでもOK
        return render_template(
            "index.html",
            schedules=[],
            selected_image=None,
            canonical=url_for("index", _external=True),
        ), 500
    
    
# =========================================================
# schedule_koyomi 関数（BatchGet導入による高速化版）
# =========================================================
@app.route("/schedule_koyomi", methods=['GET'])
@app.route("/schedule_koyomi/<int:year>/<int:month>", methods=['GET'])
def schedule_koyomi(year=None, month=None):
    try:
        if year is None or month is None:
            today = date.today()
            year, month = today.year, today.month
        
        # 前月・翌月の計算
        prev_date = date(year, month, 1) - timedelta(days=1)
        prev_year, prev_month = prev_date.year, prev_date.month
        next_date = date(year, month, 28) + timedelta(days=5) # 翌月へ確実に飛ばす
        next_year, next_month = next_date.year, next_date.month
        
        calendar.setfirstweekday(calendar.SUNDAY)       
        cal = calendar.monthcalendar(year, month)
        
        # スケジュール取得
        schedules = get_schedules_with_formatting_all()
        table_name = current_app.config.get("TABLE_NAME_USER") or "bad-users"

        # 1) 全参加者の user_id を抽出
        all_uids = set()
        for s in schedules:
            for item in s.get("participants", []):
                if isinstance(item, dict) and "S" in item: all_uids.add(item["S"])
                elif isinstance(item, str): all_uids.add(item)

        # 2) 参加者情報を一括取得 (BatchGet)
        user_cache = {}
        uid_list = list(all_uids)
        for i in range(0, len(uid_list), 100):
            batch = uid_list[i:i+100]
            keys = [{"user#user_id": uid} for uid in batch]
            res = current_app.dynamodb.batch_get_item(RequestItems={table_name: {"Keys": keys}})
            for u in res.get("Responses", {}).get(table_name, []):
                user_cache[u["user#user_id"]] = u

        # 3) スケジュールに参加者情報を紐付け
        for schedule in schedules:
            p_info = []
            raw_p = schedule.get("participants", [])
            for item in raw_p:
                uid = item["S"] if isinstance(item, dict) else item
                user = user_cache.get(uid)
                if user:
                    p_info.append({
                        "user_id": user["user#user_id"],
                        "display_name": user.get("display_name", "不明")
                    })
            schedule["participants_info"] = p_info
        
        # カレンダーデータ構築
        calendar_data = []
        today_obj = date.today()
        for week in cal:
            week_data = []
            for day_num in week:
                if day_num == 0:
                    week_data.append({'day': 0, 'is_other_month': True, 'schedules': []})
                else:
                    d_obj = date(year, month, day_num)
                    d_str = d_obj.strftime('%Y-%m-%d')
                    day_schedules = [s for s in schedules if s.get("date") == d_str]
                    week_data.append({
                        'day': day_num,
                        'is_today': d_obj == today_obj,
                        'is_other_month': False,
                        'schedules': day_schedules,
                        'has_schedule': len(day_schedules) > 0
                    })
            calendar_data.append(week_data)

        month_name = f"{month}月"
        selected_image = random.choice([f"images/top{i:03d}.jpg" for i in range(1, 6)])

        return render_template("schedule_koyomi.html", 
                               schedules=schedules,
                               selected_image=selected_image,
                               year=year, month=month,
                               month_name=month_name,
                               prev_year=prev_year, prev_month=prev_month,
                               next_year=next_year, next_month=next_month,
                               calendar_data=calendar_data)
        
    except Exception as e:
        current_app.logger.error(f"[schedule_koyomi] エラー: {e}", exc_info=True)
        flash('カレンダーの取得中にエラーが発生しました', 'error')
        return render_template("schedule_koyomi.html", schedules=[], year=year, month=month)
    
    
@app.route("/day_of_participants", methods=["GET"])
def day_of_participants():
    try:
        date = request.args.get("date")
        if not date:
            flash("日付が指定されていません", "warning")
            return redirect(url_for("index"))

        schedules = get_schedules_with_formatting()
        schedule = next((s for s in schedules if s.get("date") == date), None)
        if not schedule:
            flash(f"{date} のスケジュールが見つかりません", "warning")
            return redirect(url_for("index"))

        participants = get_participants_info(schedule)

        return render_template("day_of_participants.html", 
                               date=date,
                               location=schedule.get("location"),
                               participants=participants)

    except Exception as e:
        logger.error(f"[day_of_participants] エラー: {e}")
        flash("参加者情報の取得中にエラーが発生しました", "danger")
        return render_template("day_of_participants.html", participants=[], date="未定", location="未定")


@app.route('/schedule/<string:schedule_id>/join', methods=['POST'])
@login_required
def join_schedule(schedule_id):
    try:
        data = request.get_json() or {}
        date = (data.get('date') or "").strip()

        if not date:
            app.logger.warning(f"'date' is not provided for schedule_id={schedule_id}")
            return jsonify({'status': 'error', 'message': '日付が不足しています。'}), 400

        schedule_table = app.dynamodb.Table(app.table_name_schedule)

        # スケジュール取得
        response = schedule_table.get_item(Key={'schedule_id': schedule_id, 'date': date})
        schedule = response.get('Item')
        if not schedule:
            return jsonify({'status': 'error', 'message': 'スケジュールが見つかりません。'}), 404

        user_id = current_user.id

        participants = schedule.get('participants', []) or []
        tara_participants = schedule.get('tara_participants', []) or []

        history_table = app.dynamodb.Table("bad-users-history")
        users_table = app.dynamodb.Table("bad-users")  # ★追加：最終参加日更新用

        now_utc_iso = datetime.now(timezone.utc).isoformat()

        # 参加キャンセル
        if user_id in participants:
            participants.remove(user_id)
            message = "参加をキャンセルしました"
            is_joining = False

            # 1) 既存の履歴を「cancelled」に更新（既存関数）
            try:
                db.cancel_participation(user_id, date, schedule_id)
                app.logger.info(
                    f"✓ ユーザー {user_id} の参加履歴をキャンセル済みに更新しました (date={date}, schedule_id={schedule_id})"
                )
            except Exception as e:
                app.logger.error(f"[cancel_participation エラー]: {e}")

            # 2) 保険として「キャンセル」履歴も1件追加
            try:
                history_table.put_item(
                    Item={
                        "user_id": user_id,
                        "joined_at": now_utc_iso,
                        "schedule_id": schedule_id,
                        "date": date,
                        "location": schedule.get("location") or schedule.get("venue") or "未設定",
                        "status": "cancelled",
                        "action": "cancel",
                    }
                )
            except Exception as e:
                app.logger.error(f"[キャンセル履歴保存エラー] bad-users-history: {e}")

            # ※キャンセル時は last_participation_date を更新しない（巻き戻しが必要になるため）
            #   必要になったら後で仕様を決めて実装

        # 参加登録（正式参加）
        else:
            participants.append(user_id)
            message = "参加登録が完了しました！"
            is_joining = True

            # 正式参加したら「たら」から自動削除
            if user_id in tara_participants:
                tara_participants.remove(user_id)
                app.logger.info(
                    f"✓ ユーザー {user_id} の「たら」を自動削除しました (schedule_id={schedule_id}, date={date})"
                )

            # 参加回数カウントは初回だけ（従来仕様）
            if not previously_joined(schedule_id, user_id):
                try:
                    increment_practice_count(user_id)
                except Exception as e:
                    app.logger.error(f"[practice_count 更新エラー]: {e}")

            # 履歴は「毎回」追加
            try:
                history_table.put_item(
                    Item={
                        "user_id": user_id,
                        "joined_at": now_utc_iso,
                        "schedule_id": schedule_id,
                        "date": date,
                        "location": schedule.get("location") or schedule.get("venue") or "未設定",
                        "status": "registered",
                        "action": "join",
                    }
                )
            except Exception as e:
                app.logger.error(f"[履歴保存エラー] bad-users-history: {e}")

            # ★追加：最終参加日を bad-users に反映（registered のときだけ）
            try:
                update_last_participation(users_table, user_id=user_id, event_date=date)
                app.logger.info(f"✓ last_participation_date updated: user_id={user_id}, date={date}")
            except Exception as e:
                app.logger.error(f"[last_participation 更新エラー] bad-users: {e}")

        # スケジュール更新（participants / count / tara も一緒に更新）
        schedule_table.update_item(
            Key={'schedule_id': schedule_id, 'date': date},
            UpdateExpression=(
                "SET participants = :participants, "
                "participants_count = :count, "
                "tara_participants = :tara, "
                "updated_at = :ua"
            ),
            ExpressionAttributeValues={
                ':participants': participants,
                ':count': len(participants),
                ':tara': tara_participants,
                ':ua': now_utc_iso,
            }
        )

        # キャッシュリセット
        cache.delete_memoized(get_schedules_with_formatting)

        return jsonify({
            'status': 'success',
            'message': message,
            'is_joining': is_joining,
            'participants': participants,
            'participants_count': len(participants),
            'tara_participants': tara_participants,
        })

    except ClientError as e:
        app.logger.error(f"DynamoDB ClientError: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'message': 'データベースエラーが発生しました。'}), 500
    except Exception as e:
        app.logger.error(f"Unexpected error in join_schedule: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'message': '予期しないエラーが発生しました。'}), 500
    

def update_last_participation(users_table, user_id: str, event_date: str):
    """
    bad-users の最終参加日を更新（registered のときに呼ぶ）
    user_id: "uuid..."
    event_date: "YYYY-MM-DD"
    
    ★変更点: recent_sk を固定化（日付を含めない）
    → 1ユーザー1GSIエントリになり、重複が発生しない
    """
    now_utc_iso = datetime.now(timezone.utc).isoformat()

    users_table.update_item(
        Key={"user#user_id": user_id},  # user#プレフィックスなし
        UpdateExpression=(
            "SET last_participation_date = :d, "
            "recent_pk = :pk, "
            "recent_sk = :sk, "
            "last_participation_updated_at = :u"
        ),
        ExpressionAttributeValues={
            ":d": event_date,
            ":pk": "recent",            
            ":sk": user_id,
            ":u": now_utc_iso,
        },
    )
    
    
@app.route('/tara_join', methods=['POST'])
@login_required
def tara_join():
    """「たら」参加（条件付き参加希望）の登録・解除"""
    try:
        data = request.get_json()
        schedule_id = data.get('schedule_id')
        schedule_date = data.get('schedule_date')

        if not schedule_id or not schedule_date:
            return jsonify({'status': 'error', 'message': '必要なデータが不足しています'}), 400

        schedule_table = app.dynamodb.Table(app.table_name_schedule)
        history_table  = app.dynamodb.Table("bad-users-history")

        user_id = current_user.id
        now_utc = datetime.now(timezone.utc).isoformat()

        # スケジュール取得
        response = schedule_table.get_item(
            Key={'schedule_id': schedule_id, 'date': schedule_date}
        )
        if 'Item' not in response:
            return jsonify({'status': 'error', 'message': 'スケジュールが見つかりません'}), 404

        schedule = response['Item']
        tara_participants = schedule.get('tara_participants', [])

        is_tara_joined = user_id in tara_participants

        if is_tara_joined:
            # 解除
            tara_participants.remove(user_id)
            message = '「たら」参加を解除しました'

            # ★解除も履歴に残す（後から正しく判定できる）
            try:
                history_table.put_item(
                    Item={
                        "user_id": user_id,
                        "joined_at": now_utc,
                        "schedule_id": schedule_id,
                        "date": schedule_date,
                        "action": "tara_join",
                        "status": "cancelled",  # ★解除は cancelled
                        "location": schedule.get("venue", schedule.get("location", "未設定")),
                    }
                )
            except Exception as e:
                app.logger.error(f"[たら解除 履歴保存エラー]: {e}")

        else:
            # 追加
            tara_participants.append(user_id)
            message = '「たら」参加しました'

            # ★たらは tentative（仮参加）として保存
            try:
                history_table.put_item(
                    Item={
                        "user_id": user_id,
                        "joined_at": now_utc,
                        "schedule_id": schedule_id,
                        "date": schedule_date,
                        "action": "tara_join",
                        "status": "tentative",  # ★重要
                        "location": schedule.get("venue", schedule.get("location", "未設定")),
                    }
                )
            except Exception as e:
                app.logger.error(f"[たら履歴保存エラー]: {e}")

        # 更新
        schedule_table.update_item(
            Key={'schedule_id': schedule_id, 'date': schedule_date},
            UpdateExpression="SET tara_participants = :tp, updated_at = :ua",
            ExpressionAttributeValues={':tp': tara_participants, ':ua': now_utc}
        )

        cache.delete_memoized(get_schedules_with_formatting)

        return jsonify({
            'status': 'success',
            'message': message,
            'tara_count': len(tara_participants),
            'is_tara_joined': not is_tara_joined
        })

    except Exception as e:
        app.logger.error(f"「たら」参加処理エラー: {e}")
        return jsonify({'status': 'error', 'message': '処理中にエラーが発生しました'}), 500

def previously_joined(schedule_id, user_id):
    """
    過去にそのスケジュールに参加していたかを確認する。
    """
    schedule_table = app.dynamodb.Table(app.table_name_schedule)

    response = schedule_table.scan(
        FilterExpression=Attr('schedule_id').eq(schedule_id) & Attr('participants').contains(user_id)
    )
    return bool(response.get('Items'))

def _user_pk(user_id: str) -> dict:
    return {"user#user_id": user_id}

def increment_practice_count(user_id):
    user_table = app.dynamodb.Table(app.table_name_users)

    try:
        user_table.update_item(
            Key=_user_pk(user_id),
            UpdateExpression="SET practice_count = if_not_exists(practice_count, :start) + :inc",
            ExpressionAttributeValues={
                ":start": Decimal(0),
                ":inc": Decimal(1),
            },
            ConditionExpression="attribute_exists(#pk)",
            ExpressionAttributeNames={"#pk": "user#user_id"},
        )
    except ClientError as e:
        # キー間違い/ユーザー未作成ならここに来る
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            app.logger.warning("[increment_practice_count] user not found: %s", user_id)
            return False
        raise
    return True

@app.route('/participants/by_date/<schedule_id>')
@login_required
def participants_by_date(schedule_id):
    schedule_table = app.dynamodb.Table(app.table_name_schedule)
    response = schedule_table.scan(FilterExpression=Key('schedule_id').eq(schedule_id))
    items = response.get('Items', [])
    
    if not items:
        flash('指定されたスケジュールが見つかりません', 'warning')
        return redirect(url_for('index'))

    schedule = items[0]
    participants = schedule.get('participants', [])

    user_table = app.dynamodb.Table(app.table_name_users)
    participants_info = []

    for uid in participants:
        user_resp = user_table.scan(FilterExpression=Key('user#user_id').eq(uid))
        if user_resp.get("Items"):
            participants_info.append(user_resp["Items"][0])

    return render_template('participants_by_date.html',
                           schedule=schedule,
                           participants_info=participants_info)


@app.route('/signup', methods=['GET', 'POST'])
def signup():
    form = RegistrationForm()

    if not form.validate_on_submit():
        # バリデーションエラー表示
        if form.errors:
            app.logger.warning(f"Form validation errors: {form.errors}")
            for field, errors in form.errors.items():
                for error in errors:
                    flash(f'{form[field].label.text}: {error}', 'error')
        return render_template('signup.html', form=form)

    # -------------------------
    # ここから登録処理
    # -------------------------
    user_id = str(uuid.uuid4())
    current_time = datetime.now(timezone.utc).isoformat()
    hashed_password = generate_password_hash(form.password.data, method='pbkdf2:sha256')

    # usersテーブル（bad-users想定）
    users_table = app.dynamodb.Table(app.table_name)  # app.table_name が users テーブル名ならOK

    # ユーザーItem（作成）
    user_item = {
        "user#user_id": user_id,
        "address": form.address.data,
        "administrator": False,
        "created_at": current_time,
        "updated_at": current_time,

        "date_of_birth": form.date_of_birth.data.strftime('%Y-%m-%d'),
        "display_name": form.display_name.data,
        "user_name": form.user_name.data,
        "furigana": form.furigana.data,
        "gender": form.gender.data,

        "email": form.email.data.lower(),
        "password": hashed_password,

        "phone": form.phone.data,
        "post_code": form.post_code.data,
        "organization": form.organization.data,

        "guardian_name": form.guardian_name.data,
        "emergency_phone": form.emergency_phone.data,
        "badminton_experience": form.badminton_experience.data,

        # プロフィール用の追加フィールド
        "bio": "",
        "profile_image_url": "",
        "followers_count": 0,
        "following_count": 0,
        "posts_count": 0,
        "skill_score": Decimal("50.0"),  # 現在のプレイヤー平均に合わせる
         "skill_sigma": Decimal("8.333"),  # TrueSkillのデフォルト
    }

    try:
        # 1) email重複チェック（GSI: email-index）
        email_check = users_table.query(
            IndexName='email-index',
            KeyConditionExpression='email = :email',
            ExpressionAttributeValues={':email': form.email.data.lower()},
        )

        if email_check.get('Items'):
            app.logger.warning(f"Duplicate email registration attempt: {form.email.data}")
            flash('このメールアドレスは既に登録されています。', 'error')
            return redirect(url_for('signup'))

        # 2) usersテーブルにユーザー作成
        users_table.put_item(
            Item=user_item,
            ConditionExpression='attribute_not_exists(#pk)',
            ExpressionAttributeNames={"#pk": "user#user_id"},
        )    

        app.logger.info(
            f"New user created - ID: {user_id}, Organization: {form.organization.data}, Email: {form.email.data}"
        )
        flash('アカウントが作成されました！ログインしてください。', 'success')
        return redirect(url_for('login'))

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error'].get('Message', '')
        app.logger.error(f"DynamoDB error - Code: {error_code}, Message: {error_message}", exc_info=True)

        if error_code == 'ConditionalCheckFailedException':
            flash('このメールアドレスは既に登録されています。', 'error')
        elif error_code == 'ValidationException':
            flash('入力データが無効です。', 'error')
        elif error_code == 'ResourceNotFoundException':
            flash('システムエラーが発生しました。', 'error')
            app.logger.critical(f"DynamoDB table not found: {app.table_name}")
        else:
            flash('アカウント作成中にエラーが発生しました。', 'error')

        return redirect(url_for('signup'))

    except Exception as e:
        app.logger.error(f"Unexpected error during signup: {str(e)}", exc_info=True)
        flash('予期せぬエラーが発生しました。時間をおいて再度お試しください。', 'error')
        return redirect(url_for('signup'))

@app.route('/temp_register', methods=['GET', 'POST'])
def temp_register():
    form = TempRegistrationForm()

    if form.validate_on_submit():
        skill_score = int(request.form.get('skill_score') or 0)

        try:
            current_time = datetime.now(timezone.utc).isoformat()
            hashed_password = generate_password_hash(form.password.data, method='pbkdf2:sha256')
            user_id = str(uuid.uuid4())

            table = current_app.table   # ✅ これ（または app.table）

            temp_data = {
                "user#user_id": user_id,
                "display_name": form.display_name.data,
                "user_name": form.user_name.data,
                "gender": form.gender.data,
                "badminton_experience": form.badminton_experience.data,
                "email": form.email.data.lower().strip(),
                "password": hashed_password,
                "phone": form.phone.data,
                "organization": "仮登録",
                "created_at": current_time,
                "administrator": False,

                # ✅ DynamoDBのNumberはDecimalが安全
                "skill_score": Decimal(str(skill_score)),
                "skill_sigma": Decimal("8.333"),

                "date_of_birth": form.date_of_birth.data.isoformat()
            }

            table.put_item(Item=temp_data)

            flash("仮登録が完了しました。ログインしてください。", "success")
            return redirect(url_for('login'))

        except Exception as e:
            current_app.logger.error(f"DynamoDBへの登録中にエラーが発生しました: {e}", exc_info=True)
            flash(f"登録中にエラーが発生しました: {str(e)}", 'danger')

    return render_template('temp_register.html', form=form) 

@app.route('/login', methods=['GET', 'POST'])
def login():

    if current_user.is_authenticated:
        return redirect(url_for('index')) 
    
    form = LoginForm()
    if form.validate_on_submit():
        try:
            response = app.table.query(
                IndexName='email-index',
                KeyConditionExpression='email = :email',
                ExpressionAttributeValues={
                    ':email': form.email.data
                }
            )

            print(f"Query response: {response}")
            items = response.get('Items', [])
            print(f"Items found: {len(items)}")
            
            items = response.get('Items', [])
            user_data = items[0] if items else None
            
            if not user_data:
                app.logger.warning(f"No user found for email: {form.email.data}")
                flash('メールアドレスまたはパスワードが正しくありません。', 'error')
                return render_template('login.html', form=form)           

            try:
                user = User(
                    user_id=user_data['user#user_id'],
                    display_name=user_data['display_name'],
                    user_name=user_data['user_name'],
                    furigana=user_data.get('furigana', None),
                    email=user_data['email'],
                    password_hash=user_data['password'],
                    gender=user_data['gender'],
                    date_of_birth=user_data.get('date_of_birth', None),
                    post_code=user_data.get('post_code', None),
                    address=user_data.get('address',None),
                    phone=user_data.get('phone', None),
                    guardian_name=user_data.get('guardian_name', None),  
                    emergency_phone=user_data.get('emergency_phone', None), 
                    badminton_experience=user_data.get('badminton_experience', None),
                    administrator=user_data['administrator'],
                    organization=user_data.get('organization', 'other')
                    
                    
                )
                                
            except KeyError as e:
                app.logger.error(f"Error creating user object: {str(e)}")
                flash('ユーザーデータの読み込みに失敗しました。', 'error')
                return render_template('login.html', form=form)

            if not hasattr(user, 'check_password'):
                app.logger.error("User object missing check_password method")
                flash('ログイン処理中にエラーが発生しました。', 'error')
                return render_template('login.html', form=form)

            if user.check_password(form.password.data):
                session.permanent = True  # セッションを永続化
                login_user(user, remember=True)  # 常にremember=Trueに設定
                
                flash('ログインしました。', 'success')
                
                next_page = request.args.get('next')
                if not next_page or not is_safe_url(next_page):
                    next_page = url_for('index')
                return redirect(next_page)            
                        
            app.logger.warning(f"Invalid password attempt for email: {form.email.data}")
            time.sleep(random.uniform(0.1, 0.3))
            flash('メールアドレスまたはパスワードが正しくありません。', 'error')
                
        except Exception as e:
            app.logger.error(f"Login error: {str(e)}")
            flash('ログイン処理中にエラーが発生しました。', 'error')
    
    return render_template('login.html', form=form)
    

# セキュアなリダイレクト先かを確認する関数
def is_safe_url(target):
    ref_url = urlparse(request.host_url)
    test_url = urlparse(urljoin(request.host_url, target))
    return test_url.scheme in ('http', 'https') and ref_url.netloc == test_url.netloc

        
@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect("/")
    

# ポイントと参加回数を無効化したバージョン：
def _encode_lek(lek: dict) -> str:
    raw = json.dumps(lek, ensure_ascii=False).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8")

def _decode_lek(token: str) -> dict:
    raw = base64.urlsafe_b64decode(token.encode("utf-8"))
    return json.loads(raw.decode("utf-8"))


@app.route("/user_maintenance", methods=["GET"])
@login_required
def user_maintenance():
    try:
        sort_by = request.args.get("sort_by", "last_participation")
        order = request.args.get("order", "desc")
        
        from utils.timezone import JST
        today = datetime.now(JST).date().isoformat()
        
        current_app.logger.info(f"[user_maintenance] 今日の日付: {today}")
        
        # 過去のスケジュールを全件取得
        schedule_table = app.dynamodb.Table(app.table_name_schedule)
        schedules = []
        last_evaluated_key = None
        
        while True:
            if last_evaluated_key:
                schedule_response = schedule_table.scan(
                    FilterExpression=Attr("date").lt(today),
                    ExclusiveStartKey=last_evaluated_key
                )
            else:
                schedule_response = schedule_table.scan(
                    FilterExpression=Attr("date").lt(today)
                )
            
            schedules.extend(schedule_response.get("Items", []))
            last_evaluated_key = schedule_response.get("LastEvaluatedKey")
            
            if not last_evaluated_key:
                break
        
        current_app.logger.info(f"[user_maintenance] 過去のスケジュール: {len(schedules)}件")
        
        # ★デバッグ：最初のスケジュールの参加者を確認
        if schedules:
            sample = schedules[0]
            current_app.logger.info(f"[user_maintenance] サンプルスケジュール: date={sample.get('date')}")
            current_app.logger.info(f"[user_maintenance] participants形式: {type(sample.get('participants'))}")
            current_app.logger.info(f"[user_maintenance] participants最初の3件: {sample.get('participants', [])[:3]}")
        
        # ユーザーごとの最終参加日を集計
        user_last_dates = {}
        for schedule in schedules:
            event_date = schedule.get("date")
            participants = schedule.get("participants", [])
            
            # ★デバッグ：各スケジュールの参加者数
            if participants:
                current_app.logger.debug(f"[user_maintenance] {event_date}: {len(participants)}人")
            
            for participant in participants:
                if isinstance(participant, dict):
                    user_id = participant.get("user_id") or participant.get("user#user_id") or participant.get("S")
                elif isinstance(participant, str):
                    user_id = participant
                else:
                    current_app.logger.warning(f"[user_maintenance] 不明な形式: {type(participant)} - {participant}")
                    continue
                
                # ★削除：user#プレフィックス除去（移行済みのため不要）
                if user_id and isinstance(user_id, str):
                    if user_id not in user_last_dates or event_date > user_last_dates[user_id]:
                        user_last_dates[user_id] = event_date
        
        current_app.logger.info(f"[user_maintenance] 参加ユーザー数: {len(user_last_dates)}人")
        
        # ユーザー情報を取得
        user_ids = list(user_last_dates.keys())
        
        users_data = {}
        if user_ids:
            users_table = app.dynamodb.Table(app.table_name_users)
            
            # バッチ取得（100件ずつ）+ 未処理キーの再取得
            for i in range(0, len(user_ids), 100):
                batch_ids = user_ids[i:i + 100]
                keys = [{"user#user_id": uid} for uid in batch_ids]
                
                request_items = {app.table_name_users: {"Keys": keys}}
                
                try:
                    # ★未処理キーがなくなるまで繰り返し
                    while request_items:
                        batch_response = app.dynamodb.batch_get_item(RequestItems=request_items)
                        
                        for user in batch_response.get("Responses", {}).get(app.table_name_users, []):
                            uid = user.get("user#user_id", "")  
                            users_data[uid] = user
                        
                        # 未処理キーを確認
                        unprocessed = batch_response.get("UnprocessedKeys", {})
                        if unprocessed:
                            current_app.logger.warning(f"[user_maintenance] 未処理キー: {len(unprocessed.get(app.table_name_users, {}).get('Keys', []))}件")
                            request_items = unprocessed
                        else:
                            request_items = None
                        
                except Exception as e:
                    current_app.logger.error(f"[user_maintenance] バッチ取得エラー: {e}")
        
        current_app.logger.info(f"[user_maintenance] ユーザー情報取得: {len(users_data)}人")
        
        # 結果を整形
        unique_users = []
        for user_id, last_date in user_last_dates.items():
            user = users_data.get(user_id, {})
            user["user_id"] = user_id
            user["last_participation_date"] = last_date
            user["points"] = None
            user["total_participation"] = None
            user["phone"] = user.get("phone") or ""
            user["emergency_phone"] = user.get("emergency_phone") or ""
            user["user_name"] = user.get("user_name") or ""
            user["email"] = user.get("email") or ""
            user["display_name"] = user.get("display_name") or ""
            unique_users.append(user)
        
        # ソート
        if sort_by == "last_participation":
            unique_users.sort(
                key=lambda u: u.get("last_participation_date", ""),
                reverse=(order == "desc")
            )
        elif sort_by == "user_name":
            unique_users.sort(
                key=lambda u: (u.get("user_name") or "").lower(),
                reverse=(order == "desc")
            )
        
        current_app.logger.info(f"[user_maintenance] 最終件数: {len(unique_users)}件（過去の実参加）")
        
        return render_template(
            "user_maintenance.html",
            users=unique_users,
            sort_by=sort_by,
            order=order,
            limit=len(unique_users),
            next_token=None,
            stats_disabled=True,
        )

    except Exception as e:
        current_app.logger.error(f"[user_maintenance] エラー: {e}", exc_info=True)
        flash("ユーザー一覧の読み込み中にエラーが発生しました。", "error")
        return redirect(url_for("index"))
    
    
# USERチェックアクセス方法: http://127.0.0.1:5000/check_user_data/4c7f822d-ff39-4797-9b7b-8ebc205490f5
@app.route("/check_user_data/<user_id>", methods=["GET"])
@login_required
def check_user_data(user_id):
    """特定ユーザーのデータを直接確認"""
    try:
        # ★user#プレフィックスを付けずにそのまま使う
        response = app.table.get_item(
            Key={"user#user_id": user_id}  # ← 修正
        )
        
        user = response.get("Item")
        if user:
            return jsonify({
                "status": "ok",
                "user_id": user_id,
                "display_name": user.get("display_name"),
                "user_name": user.get("user_name"),
                "email": user.get("email"),
                "phone": user.get("phone"),
                "created_at": str(user.get("created_at")),
                "last_participation_date": user.get("last_participation_date"),
                "exists_in_main_table": True,
                "full_data": {k: str(v) for k, v in user.items() if k != "password"}
            })
        else:
            return jsonify({
                "status": "not_found", 
                "user_id": user_id,
                "message": "メインテーブルにデータが見つかりません"
            })
            
    except Exception as e:
        current_app.logger.error(f"[check_user_data] エラー: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)})
    

@app.route("/check_table_structure", methods=["GET"])
@login_required
def check_table_structure():
    """テーブル構造を確認"""
    try:
        # テーブルのメタデータを取得
        table_description = app.table.meta.client.describe_table(
            TableName=app.table_name_users
        )
        
        table_info = table_description["Table"]
        
        return jsonify({
            "status": "ok",
            "table_name": table_info["TableName"],
            "key_schema": table_info["KeySchema"],
            "attribute_definitions": table_info["AttributeDefinitions"],
            "global_secondary_indexes": table_info.get("GlobalSecondaryIndexes", []),
            "item_count": table_info.get("ItemCount"),
        })
            
    except Exception as e:
        current_app.logger.error(f"[check_table_structure] エラー: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)})
    

@app.route("/sample_records", methods=["GET"])
@login_required  
def sample_records():
    """テーブルの最初の10件を取得"""
    try:
        response = app.table.scan(Limit=10)
        
        items = response.get("Items", [])
        
        return jsonify({
            "status": "ok",
            "count": len(items),
            "records": [{k: str(v)[:100] if k != "password" else "***" for k, v in item.items()} for item in items]
        })
            
    except Exception as e:
        current_app.logger.error(f"[sample_records] エラー: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)})
    

@app.route("/table_info")
def get_table_info():
    try:
        table = get_schedule_table()
        # テーブルの詳細情報を取得
        response = {
            'table_name': table.name,
            'key_schema': table.key_schema,
            'attribute_definitions': table.attribute_definitions,
            # サンプルデータも取得
            'sample_data': table.scan(Limit=1)['Items']
        }
        return str(response)
    except Exception as e:
        return f'Error: {str(e)}'    
    
    
def _user_key(user_id: str) -> dict:
    uid = str(user_id)
    return {"user#user_id": uid}


@app.route('/account/<string:user_id>', methods=['GET', 'POST'])
def account(user_id):
    try:
        table = app.dynamodb.Table(app.table_name)
        # 更新直後でも最新を読む
        response = table.get_item(Key=_user_key(user_id), ConsistentRead=True)
        user = response.get('Item')
        if not user:
            abort(404)

        # 内部表記を統一
        user['user_id'] = user.get('user#user_id', user_id)

        form = UpdateUserForm(user_id=user_id, dynamodb_table=table)

        # デフォルト画像URL（テンプレ共通）
        default_image_url = url_for('static', filename='images/default.jpg')

        # プロフィール画像URL（空/空白は None 扱い）
        piu = user.get('profile_image_url')
        user['profile_image_url'] = (piu if isinstance(piu, str) and piu.strip() else None)

        if request.method == 'GET':
            # 既存値をフォームに流し込み
            form.display_name.data = user['display_name']
            form.user_name.data = user['user_name']
            form.furigana.data = user.get('furigana', None)
            form.email.data = user['email']
            form.phone.data = user.get('phone', None)
            form.post_code.data = user.get('post_code', None)
            form.address.data = user.get('address', None)
            form.badminton_experience.data = user.get('badminton_experience', None)
            form.gender.data = user['gender']
            try:
                form.date_of_birth.data = datetime.strptime(user['date_of_birth'], '%Y-%m-%d')
            except (ValueError, KeyError):
                form.date_of_birth.data = None
            form.organization.data = user.get('organization', '')
            form.guardian_name.data = user.get('guardian_name', '')
            form.emergency_phone.data = user.get('emergency_phone', '')

            return render_template('account.html', form=form, user=user, default_image_url=default_image_url)

        # POST: 更新処理
        if request.method == 'POST' and form.validate_on_submit():
            current_time = datetime.now(timezone.utc).isoformat()
            update_expression_parts = []
            expression_values = {}

            fields_to_update = [
                ('display_name', 'display_name'),
                ('user_name', 'user_name'),
                ('furigana', 'furigana'),
                ('email', 'email'),
                ('phone', 'phone'),
                ('post_code', 'post_code'),
                ('address', 'address'),
                ('gender', 'gender'),
                ('organization', 'organization'),
                ('guardian_name', 'guardian_name'),
                ('emergency_phone', 'emergency_phone'),
                ('badminton_experience', 'badminton_experience')
            ]

            for field_name, db_field in fields_to_update:
                value = getattr(form, field_name).data
                if value:
                    update_expression_parts.append(f"{db_field} = :{db_field}")
                    expression_values[f":{db_field}"] = value

            if form.date_of_birth.data:
                update_expression_parts.append("date_of_birth = :date_of_birth")
                expression_values[':date_of_birth'] = form.date_of_birth.data.strftime('%Y-%m-%d')

            if form.password.data:
                hashed = generate_password_hash(form.password.data, method='pbkdf2:sha256')
                if hashed != user.get('password'):
                    update_expression_parts.append("password = :password")
                    expression_values[':password'] = hashed

            # 更新日時は常に更新
            update_expression_parts.append("updated_at = :updated_at")
            expression_values[':updated_at'] = current_time

            # 画像アップロード（この経路を使う場合のみ）
            if form.profile_image.data:
                image_file = form.profile_image.data
                filename = secure_filename(image_file.filename)
                s3_key = f"profile-images/{user_id}/{filename}"
                try:
                    img = Image.open(image_file)
                    img.thumbnail((400, 400))
                    buffer = BytesIO()
                    img.save(buffer, format='JPEG', quality=85)
                    buffer.seek(0)
                    app.s3.upload_fileobj(buffer, app.config["S3_BUCKET"], s3_key)
                    image_url = f"{app.config['S3_LOCATION']}{s3_key}"
                    update_expression_parts.append("profile_image_url = :profile_image_url")
                    expression_values[":profile_image_url"] = image_url
                except ClientError as e:
                    app.logger.error(f"S3 upload failed in /account for user {user_id}: {e}", exc_info=True)
                    flash("画像のアップロードに失敗しました。", "danger")
                except Exception as e:
                    app.logger.error(f"Image processing failed in /account for user {user_id}: {e}", exc_info=True)
                    flash("画像の処理に失敗しました。", "danger")

            try:
                if update_expression_parts:
                    update_expression = "SET " + ", ".join(update_expression_parts)
                    table.update_item(
                        Key=_user_key(user_id),
                        UpdateExpression=update_expression,
                        ExpressionAttributeValues=expression_values,
                        ConditionExpression="attribute_exists(#pk)",
                        ExpressionAttributeNames={"#pk": "user#user_id"},
                        ReturnValues="ALL_NEW"
                    )
                    flash('プロフィールが更新されました。', 'success')
                else:
                    flash('更新する項目がありません。', 'info')

                return redirect(url_for('account', user_id=user_id))

            except ClientError as e:
                app.logger.error(f"DynamoDB ClientError in /account for user {user_id}: {e}", exc_info=True)
                flash('DynamoDBでエラーが発生しました。', 'error')
                return redirect(url_for('account', user_id=user_id))
            except Exception as e:
                app.logger.error(f"Unexpected error in /account for user {user_id}: {e}", exc_info=True)
                flash('予期せぬエラーが発生しました。', 'error')
                return redirect(url_for('index'))

        # POST だがバリデーションNG → 再表示
        return render_template('account.html', form=form, user=user, default_image_url=default_image_url)

    except Exception as e:
        app.logger.error(f"Unexpected error in /account for user {user_id}: {e}", exc_info=True)
        flash('予期せぬエラーが発生しました。', 'error')
        return redirect(url_for('index'))

def is_image_accessible(url):
    try:
        response = requests.head(url)
        return response.status_code == 200
    except:
        return False                

@app.route("/delete_user/<string:user_id>")
@login_required
def delete_user(user_id):
    try:
        table = app.dynamodb.Table(app.table_name)
        key = _user_key(user_id)

        response = table.get_item(Key=key, ConsistentRead=True)
        user = response.get("Item")

        if not user:
            flash("ユーザーが見つかりません。", "error")
            return redirect(url_for("user_maintenance"))

        # 権限チェック：current_user.id が uuid なら uuid に揃えて比較
        uid_no_prefix = key["user#user_id"]
        if current_user.id != uid_no_prefix and not getattr(current_user, "administrator", False):
            app.logger.warning(
                "Unauthorized delete attempt by user %s for user %s",
                current_user.id, uid_no_prefix
            )
            abort(403)

        table.delete_item(Key=key)

        if current_user.id == uid_no_prefix:
            logout_user()
            flash("アカウントが削除されました。再度ログインしてください。", "info")
            return redirect(url_for("login"))

        flash("ユーザーアカウントが削除されました", "success")
        return redirect(url_for("user_maintenance"))

    except ClientError as e:
        app.logger.error("DynamoDB error: %s", str(e), exc_info=True)
        flash("データベースエラーが発生しました。", "error")
        return redirect(url_for("user_maintenance"))
    

@app.route("/gallery", methods=["GET", "POST"])
def gallery():
    posts = []

    if request.method == "POST":
        image = request.files.get("image")
        if image and image.filename != '':
            original_filename = secure_filename(image.filename)
            unique_filename = f"gallery/{uuid.uuid4().hex}_{original_filename}"

            img = Image.open(image)

            try:
                exif = img._getexif()
                if exif is not None:
                    for orientation in ExifTags.TAGS.keys():
                        if ExifTags.TAGS[orientation] == "Orientation":
                            break
                    orientation_value = exif.get(orientation)
                    if orientation_value == 3:
                        img = img.rotate(180, expand=True)
                    elif orientation_value == 6:
                        img = img.rotate(270, expand=True)
                    elif orientation_value == 8:
                        img = img.rotate(90, expand=True)
            except (AttributeError, KeyError, IndexError):
                # EXIFが存在しない場合はそのまま続行
                pass

            max_width = 500           
            if img.width > max_width:
                # アスペクト比を維持したままリサイズ
                new_height = int((max_width / img.width) * img.height)                
                img = img.resize((max_width, new_height), Image.LANCZOS)

            # リサイズされた画像をバイトIOオブジェクトに保存
            img_byte_arr = io.BytesIO()
            img.save(img_byte_arr, format='JPEG')
            img_byte_arr.seek(0)

            # appを直接参照
            app.s3.upload_fileobj(
                img_byte_arr,
                app.config["S3_BUCKET"],
                unique_filename
            )
            image_url = f"{app.config['S3_LOCATION']}{unique_filename}"

            print(f"Uploaded Image URL: {image_url}")
            return redirect(url_for("gallery"))  # POST後はGETリクエストにリダイレクト

    # GETリクエスト: S3バケット内の画像を取得
    try:
        response = app.s3.list_objects_v2(Bucket=app.config["S3_BUCKET"],
                                          Prefix="gallery/")
        if "Contents" in response:
            for obj in response["Contents"]: 
                if obj['Key'] != "gallery/":
                            print(f"Found object key: {obj['Key']}")
                            posts.append({
                                "image_url": f"{app.config['S3_LOCATION']}{obj['Key']}"
                            })
    except Exception as e:
        print(f"Error fetching images from S3: {e}")

    return render_template("gallery.html", posts=posts)


@app.route("/delete_image/<filename>", methods=["POST"])
@login_required
def delete_image(filename):
    try:
        # S3から指定されたファイルを削除
        app.s3.delete_object(Bucket=app.config["S3_BUCKET"], Key=f"gallery/{filename}")
        print(f"Deleted {filename} from S3")

        # 削除成功後にアップロードページにリダイレクト
        return redirect(url_for("gallery"))

    except Exception as e:
        print(f"Error deleting {filename}: {e}")
        return "Error deleting the image", 500
        
@app.route("/remove_participant_from_date", methods=['POST'])
@login_required
def remove_participant_from_date():
    print("=== remove_participant_from_date エンドポイントが呼ばれました ===")
    
    if not current_user.administrator:
        return jsonify({"success": False, "message": "権限がありません"}), 403
    
    try:
        data = request.get_json()
        user_id_to_remove = data.get('user_id')
        date = data.get('date')
        
        print(f"削除リクエスト: user_id={user_id_to_remove}, date={date}")
        
        if not user_id_to_remove or not date:
            return jsonify({"success": False, "message": "必要な情報が不足しています"}), 400
        
        # スケジュールを取得
        schedules = get_schedules_with_formatting()
        target_schedule = next((s for s in schedules if s.get("date") == date), None)
        
        if not target_schedule:
            return jsonify({
                "success": False, 
                "message": f"日付 {date} のスケジュールが見つかりません"
            }), 404
        
        schedule_id = target_schedule.get('schedule_id')
        participants = target_schedule.get('participants', [])
        
        print(f"対象スケジュール: {schedule_id}")
        print(f"対象日付: {date}")
        print(f"現在の参加者数: {len(participants)}")
        print(f"削除対象: {user_id_to_remove}")
        
        if user_id_to_remove not in participants:
            return jsonify({
                "success": False, 
                "message": "指定されたユーザーは参加していません"
            })
        
        # 参加者リストから削除
        updated_participants = [p for p in participants if p != user_id_to_remove]
        
        print(f"更新後の参加者数: {len(updated_participants)}")
        
        schedule_table = get_schedule_table()
        
        # 複合主キーを使用（schedule_id + date）
        composite_key = {
            "schedule_id": schedule_id,
            "date": date
        }
        
        print(f"使用する複合キー: {composite_key}")
        
        update_response = schedule_table.update_item(
            Key=composite_key,
            UpdateExpression="SET participants = :participants, participants_count = :count",
            ExpressionAttributeValues={
                ":participants": updated_participants,
                ":count": len(updated_participants)
            },
            ReturnValues="UPDATED_NEW"
        )
        
        print(f"DynamoDB更新完了: {update_response}")
        print(f"削除成功: {user_id_to_remove} を {schedule_id} から削除")
        
        return jsonify({
            "success": True, 
            "message": "参加者を削除しました"
        })
        
    except Exception as e:
        print(f"参加者削除エラー: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False, 
            "message": f"削除処理中にエラーが発生しました: {str(e)}"
        }), 500    
    
@app.route("/tournament/tournament_first")
def tournament_first():
    return render_template("tournament/tournament_first.html")

@app.route("/tournament/uguis2024_tournament")
def uguis2024_tournament():
    return render_template("tournament/uguis2024_tournament.html")

@app.route("/tournament/uguis2025_tournament")
def uguis2025_tournament():
    return render_template("tournament/uguis2025_tournament.html")

@app.route("/tournament/uguis2026_tournament")
def uguis2026_tournament():
    return render_template("tournament/uguis2026_tournament.html")

@app.route("/bad_manager")
def bad_manager():
    return render_template("bad_manager.html")

@app.route("/videos")
def video_link():
    return render_template("video_link.html")


@app.route('/badminton-chat-logs')
def badminton_chat_logs_page():
    """
    バドミントンチャットログ表示ページ
    """
    return render_template('badminton_chat_logs.html')

# JSON API用（既存のまま）
@app.route('/api/badminton-chat-logs', methods=['GET'])
def api_chat_logs():
    """
    バドミントンチャットログAPI（JSON専用）
    """
    cache_filter = request.args.get('cache')
    limit = int(request.args.get('limit', 100))
    
    result = get_badminton_chat_logs(cache_filter, limit)
    return jsonify(result)


@app.route('/update_skill_score', methods=['POST'])
@login_required
def update_skill_score():
    try:
        data = request.get_json(silent=True) or {}
        user_id = data.get("user_id")
        new_score = data.get("skill_score")

        if not user_id or new_score is None:
            return jsonify({"success": False, "error": "Missing parameters"}), 400

        table = app.dynamodb.Table(app.table_name)

        resp = table.update_item(
            Key=_user_key(user_id),
            UpdateExpression='SET skill_score = :score',
            ExpressionAttributeValues={':score': Decimal(str(new_score))},
            ReturnValues="UPDATED_NEW"
        )

        return jsonify({
            "success": True,
            "message": "Skill score updated",
            "updated_score": new_score,
            "ddb": resp.get("Attributes", {})
        }), 200

    except Exception as e:
        app.logger.error("[update_skill_score] 更新エラー: %s", str(e), exc_info=True)
        return jsonify({"success": False, "error": "更新に失敗しました"}), 500
    
    
@app.route('/api/user_info/<string:user_id>')
@login_required
def api_user_info(user_id):
    try:
        table = current_app.dynamodb.Table(current_app.table_name)  # usersテーブル想定

        resp = table.get_item(
            Key=_user_key(user_id),
            ConsistentRead=True
        )

        item = resp.get("Item")
        if not item:
            current_app.logger.warning("[api_user_info] not found user_id=%s", user_id)
            return jsonify({"success": False, "error": "not found"}), 404

        return jsonify({
            "success": True,
            "user_id": user_id,
            "birth_date": item.get("date_of_birth"),
            "display_name": item.get("display_name", ""),
            "skill_score": item.get("skill_score", 0),
        }), 200

    except ClientError as e:
        current_app.logger.error("[api_user_info] ddb error: %s", str(e), exc_info=True)
        return jsonify({"success": False, "error": "ddb error"}), 500
    except Exception as e:
        current_app.logger.error("[api_user_info] error: %s", str(e), exc_info=True)
        return jsonify({"success": False, "error": "server error"}), 500
    
    
@app.route('/profile_image_edit/<user_id>', methods=['GET', 'POST'])
@login_required
def profile_image_edit(user_id):
    """プロフィール画像編集ページ"""
    try:
        table = app.dynamodb.Table(app.table_name)
        resp = table.get_item(Key=_user_key(user_id), ConsistentRead=True)

        table.update_item(
            Key=_user_key(user_id),
            UpdateExpression="SET profile_image_url = :p, large_image_url = :l, updated_at = :u",
            ExpressionAttributeValues={...},
        )
        user = resp.get('Item')
        if not user:
            flash('ユーザーが見つかりません。', 'error')
            return redirect(url_for('index'))

        # 内部表記を統一
        user["user_id"] = user.get("user#user_id", user_id)
        me = current_user.get_id()
        if me != user_id:
            flash('アクセス権限がありません。', 'error')
            return redirect(url_for('index'))

        # POST: 画像アップロード
        if request.method == 'POST':
            ts = int(time.time())

            # 入力取り出し
            if 'profile_image' not in request.files:
                flash('ファイルが選択されていません。', 'error')
                return redirect(request.url)

            profile_file = request.files['profile_image']      # フロントでトリミング済み（JPEG想定）
            orig_file    = request.files.get('original_image') # 元画像（任意）

            if not profile_file or profile_file.filename == '':
                flash('ファイルが選択されていません。', 'error')
                return redirect(request.url)

            # 拡張子バリデーション
            def _is_allowed_upload(f):
                return f and f.filename and allowed_file(f.filename)
            if not _is_allowed_upload(profile_file):
                flash('サポートされていないファイル形式です。', 'error')
                return redirect(request.url)
            if orig_file and not _is_allowed_upload(orig_file):
                flash('サポートされていないファイル形式です。', 'error')
                return redirect(request.url)

            # ファイル名（largeはオリジナル基準、無ければprofile基準）
            base_name = secure_filename((orig_file or profile_file).filename)
            base, _ext = os.path.splitext(base_name)
            base_filename = base or "image"
            ext = (_ext or ".jpg").lower()

            # 座標（クライアントで計算したクロップ位置が来ていれば使用）
            sx  = request.form.get('crop_sx',   type=float)
            sy  = request.form.get('crop_sy',   type=float)
            ssz = request.form.get('crop_side', type=float)            

            # large と サーバ側プロフィール生成用に元画像のバイト列を確保
            source_for_large = orig_file or profile_file

            # large と サーバ側プロフィール生成用に元画像のバイト列を確保
            source_for_large = orig_file or profile_file
            source_for_large.stream.seek(0)
            orig_bytes = source_for_large.stream.read()

            # 元画像の健全性チェック（偽装拡張子など）
            try:
                _tmp = Image.open(BytesIO(orig_bytes))
                _tmp.verify()
                del _tmp
            except Exception:
                flash('画像ファイルを読み込めませんでした。', 'error')
                return redirect(request.url)

            # 実処理用に再オープン
            try:
                src = Image.open(BytesIO(orig_bytes))
            except UnidentifiedImageError:
                flash('画像ファイルを認識できませんでした。', 'error')
                return redirect(request.url)

            # EXIF回転補正
            try:
                from PIL import ImageOps
                src = ImageOps.exif_transpose(src)
            except Exception:
                pass

            # ===== プロフィール画像（正方形300px, JPEG） =====
            profile_bytes = None

            if orig_file and sx is not None and sy is not None and ssz is not None and ssz > 0:
                app.logger.info("Debug: Using server-side cropping with coordinates")

                iw, ih = src.size

                # 受け取った座標（float想定）→ 一貫して丸め（切り捨て禁止）
                left = int(round(float(sx)))
                top  = int(round(float(sy)))
                side = int(round(float(ssz)))

                # 画像境界でクランプ
                max_side = min(iw - left, ih - top)
                side = max(1, min(side, max_side))

                # 正方形を厳密に維持
                right  = left + side
                bottom = top  + side

                app.logger.info(f"crop box: left={left}, top={top}, side={side}, iw={iw}, ih={ih}")

                # クロップ
                prof_img = src.crop((left, top, right, bottom))

                # 透過→白合成 & RGB
                if prof_img.mode in ('RGBA', 'LA') or (prof_img.mode == 'P' and 'transparency' in prof_img.info):
                    bg = Image.new('RGB', prof_img.size, (255, 255, 255))
                    alpha = prof_img.split()[-1] if prof_img.mode in ('RGBA', 'LA') else prof_img.convert('RGBA').split()[-1]
                    bg.paste(prof_img, mask=alpha)
                    prof_img = bg
                elif prof_img.mode != 'RGB':
                    prof_img = prof_img.convert('RGB')

                # 厳密正方形のままリサイズ（thumbnailは使わない）
                out_size = 200  # ← 表示と揃えるなら 200 などに変更可
                prof_img = prof_img.resize((out_size, out_size), Image.Resampling.LANCZOS)

                _pbuf = BytesIO()
                prof_img.save(_pbuf, format='JPEG', quality=85, optimize=True, progressive=True)
                profile_bytes = _pbuf.getvalue()

            # フォールバック：フロントでトリミング済みの画像をそのまま使用
            if profile_bytes is None:
                profile_file.stream.seek(0)
                profile_bytes = profile_file.stream.read()

            profile_s3_key = f"profile-images/{user_id}/{base_filename}_{ts}_profile.jpg"
            profile_content_type = "image/jpeg"

            # ===== large（長辺2000px・比率維持）=====
            # アニメGIF/WEBPは無変換で保存、それ以外は長辺2000に縮小して拡張子に合わせて保存
            mime_map = {
                ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                ".png": "image/png", ".gif": "image/gif",
                ".webp": "image/webp"
            }
            large_content_type = mime_map.get(ext, getattr(source_for_large, "mimetype", None) or "application/octet-stream")
            large_s3_key = f"profile-images/{user_id}/{base_filename}_{ts}_large{ext}"

            try:
                src2 = Image.open(BytesIO(orig_bytes))
                if getattr(src2, "is_animated", False) and ext in (".gif", ".webp"):
                    large_buf = BytesIO(orig_bytes)
                else:
                    # EXIF回転補正
                    try:
                        from PIL import ImageOps
                        src2 = ImageOps.exif_transpose(src2)
                    except Exception:
                        pass

                    # 透過→非透過への変換はJPEG保存時のみ白合成
                    if ext in (".jpg", ".jpeg") and (src2.mode in ('RGBA', 'LA') or (src2.mode == 'P' and 'transparency' in src2.info)):
                        bg = Image.new('RGB', src2.size, (255, 255, 255))
                        alpha = src2.split()[-1] if src2.mode in ('RGBA', 'LA') else src2.convert('RGBA').split()[-1]
                        bg.paste(src2, mask=alpha)
                        src2 = bg
                    elif src2.mode not in ('RGB', 'RGBA', 'LA', 'P'):
                        src2 = src2.convert('RGB')

                    # 長辺2000px まで縮小（小さければ無変更）
                    if max(src2.size) > 2000:
                        src2.thumbnail((2000, 2000), Image.Resampling.LANCZOS)

                    large_buf = BytesIO()
                    if ext in (".jpg", ".jpeg"):
                        if src2.mode != 'RGB':
                            src2 = src2.convert('RGB')
                        src2.save(large_buf, format='JPEG', quality=90, optimize=True, progressive=True)
                    elif ext == ".png":
                        src2.save(large_buf, format='PNG', optimize=True)
                    elif ext == ".webp":
                        src2.save(large_buf, format='WEBP', quality=90, method=6)
                    elif ext == ".gif":
                        src2.save(large_buf, format='GIF', optimize=True)
                    else:
                        # 想定外は無加工
                        large_buf = BytesIO(orig_bytes)
                        large_content_type = getattr(source_for_large, "mimetype", None) or "application/octet-stream"

                    large_buf.seek(0)

            except UnidentifiedImageError:
                flash('画像ファイルを認識できませんでした。', 'error')
                return redirect(request.url)

            # ===== S3 アップロード（ACLなし）=====
            try:
                app.s3.upload_fileobj(
                    Fileobj=BytesIO(profile_bytes),
                    Bucket=app.config["S3_BUCKET"],
                    Key=profile_s3_key,
                    ExtraArgs={
                        "ContentType": profile_content_type,
                        "CacheControl": "public, max-age=31536000, immutable",
                    },
                )
                app.s3.upload_fileobj(
                    Fileobj=large_buf,
                    Bucket=app.config["S3_BUCKET"],
                    Key=large_s3_key,
                    ExtraArgs={
                        "ContentType": large_content_type,
                        "CacheControl": "public, max-age=31536000, immutable",
                    },
                )
            except ClientError:
                flash('画像のアップロードに失敗しました（S3）。', 'error')
                return redirect(request.url)

            # URL作成
            base_url = app.config.get("S3_LOCATION", "").rstrip("/")
            if not base_url:
                region = app.config.get("AWS_REGION") or os.getenv("AWS_REGION") or "ap-northeast-1"
                base_url = f"https://{app.config['S3_BUCKET']}.s3.{region}.amazonaws.com"
            profile_image_url = f"{base_url}/{profile_s3_key}"
            large_image_url   = f"{base_url}/{large_s3_key}"

            # DynamoDB 更新
            now_iso = datetime.now(timezone.utc).isoformat()
            table.update_item(
                Key={'user#user_id': user_id},
                UpdateExpression="SET profile_image_url = :p, large_image_url = :l, updated_at = :u",
                ExpressionAttributeValues={
                    ":p": profile_image_url,
                    ":l": large_image_url,
                    ":u": now_iso
                },
                ReturnValues="NONE"
            )

            flash('プロフィール画像を更新しました。', 'success')
            return redirect(url_for('account', user_id=user_id))

        # GET（初回表示）
        default_image_url = url_for('static', filename='images/default.jpg')
        user['profile_image_url'] = user.get('profile_image_url')
        return render_template('profile_image_edit.html', user=user, default_image_url=default_image_url)

    except Exception as e:
        app.logger.error(f"Unexpected error in profile_image_edit for {user_id}: {e}", exc_info=True)
        flash('予期しないエラーが発生しました。', 'error')
        return redirect(url_for('index'))


def allowed_file(filename):
    """許可されたファイル拡張子かチェック"""
    ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")
# match_table = dynamodb.Table("bad-game-match_entries")

# ---- imports（不足分をすべて追加）----
from urllib.parse import quote_plus
from dateutil import parser as dtp

import requests, feedparser
from bs4 import BeautifulSoup

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

from flask import render_template, request

@app.route("/admin/run_badnews")
def run_badnews():
    total = collect_badminton_news()
    return f"collect ok ({total})"

# ---- 収集系ユーティリティ ----
REAL_UA = {"User-Agent": (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)}

# ---- ユーティリティ関数 ----
def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def iso(dt):
    if not dt:
        return None
    try:
        d = dtp.parse(str(dt))
        if not d.tzinfo:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None

def extract_og_image(url: str, timeout=6):
    try:
        resp = requests.get(url, timeout=timeout, headers=REAL_UA, allow_redirects=True)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        # 優先順: og:image:secure_url → og:image → twitter:image
        for prop, attr in [("og:image:secure_url", "property"),
                           ("og:image", "property"),
                           ("twitter:image", "name")]:
            tag = soup.find("meta", **{attr: prop})
            if tag and tag.get("content"):
                img = tag["content"].strip()
                return urljoin(resp.url, img)
    except Exception:
        pass
    return None

def put_unique(item: dict):
    table = current_app.bad_table  # アプリケーションコンテキストから取得
    pk = f"URL#{sha256(item['url'])}"
    try:
        table.put_item(
            Item={
                "pk": pk, "sk": "METADATA",
                "url": item["url"], "title": item.get("title"),
                "source": item.get("source"), "kind": item.get("kind"),
                "lang": item.get("lang"), "published_at": item.get("published_at"),
                "summary": item.get("summary"), "image_url": item.get("image_url"),
                "author": item.get("author"),
                "gsi1pk": f"KIND#{item.get('kind')}#LANG#{item.get('lang')}",
                "gsi1sk": item.get("published_at") or "0000-00-00T00:00:00"
            },
            ConditionExpression="attribute_not_exists(pk)"
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise

# ---- データ収集関数 ----
def fetch_google_news(query="バドミントン", lang="ja"):
    """Google Newsからニュースを取得"""
    url = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=ja&gl=JP&ceid=JP:ja"
    feed = feedparser.parse(url)
    
    count = 0
    for e in feed.entries:
        link = getattr(e, "link", None)
        if not link:
            continue
            
        item = {
            "source": "google_news",
            "kind": "news",
            "title": (getattr(e, "title", "") or "").strip(),
            "url": link,
            "published_at": iso(getattr(e, "published", None)),
            "summary": getattr(e, "summary", None),
            "author": getattr(e, "source", {}).get("title") if hasattr(e, "source") else None,
            "image_url": None,
            "lang": lang,
        }
        
        # OG画像を取得
        if not item["image_url"]:
            item["image_url"] = extract_og_image(item["url"])
            
        if put_unique(item):
            count += 1
            
    print(f"Google News: {count}件の新しい記事を追加 (クエリ: {query})")
    return count

def fetch_youtube_rss(query="badminton", lang="en"):
    """YouTubeのRSSから動画を取得"""
    url = f"https://www.youtube.com/feeds/videos.xml?search_query={quote_plus(query)}"
    feed = feedparser.parse(url)
    
    count = 0
    for e in feed.entries:
        link = getattr(e, "link", None)
        if not link:
            continue
            
        thumb = None
        media = getattr(e, "media_thumbnail", None)
        if media and len(media) > 0:
            thumb = media[0].get("url")
            
        item = {
            "source": "youtube_rss",
            "kind": "video",
            "title": (getattr(e, "title", "") or "").strip(),
            "url": link,
            "published_at": iso(getattr(e, "published", None)),
            "summary": None,
            "author": getattr(e, "author", None),
            "image_url": thumb,
            "lang": lang,
        }
        
        if put_unique(item):
            count += 1
            
    print(f"YouTube RSS: {count}件の新しい動画を追加 (クエリ: {query})")
    return count

# ---- DynamoDB クエリ関数 ----
def bad_query_items(kind=None, lang=None, limit=40, last_evaluated_key=None):
    table = current_app.bad_table
    if not kind:
        kind = "news"
    if not lang:
        lang = "ja"

    kwargs = {
        "IndexName": "gsi1",
        "KeyConditionExpression": Key("gsi1pk").eq(f"KIND#{kind}#LANG#{lang}"),
        "ScanIndexForward": False,  # gsi1sk（published_at）で新しい順
        "Limit": limit,
    }
    if last_evaluated_key:
        kwargs["ExclusiveStartKey"] = last_evaluated_key

    resp = table.query(**kwargs)
    return resp.get("Items", []), resp.get("LastEvaluatedKey")


# ---- ルート定義 ----
import base64, json

@app.route("/bad_news")
def bad_news():
    kind = request.args.get("kind") or "news"   # 'news' or 'video'
    lang = request.args.get("lang") or "ja"     # 'ja' or 'en'
    page_token = request.args.get("tok")

    last_evaluated_key = None
    if page_token:
        try:
            last_evaluated_key = json.loads(
                base64.urlsafe_b64decode(page_token.encode()).decode()
            )
        except Exception:
            last_evaluated_key = None  # トークン壊れ時の保険

    items, lek = bad_query_items(kind=kind, lang=lang, limit=40, last_evaluated_key=last_evaluated_key)

    next_tok = None
    if lek:
        next_tok = base64.urlsafe_b64encode(json.dumps(lek).encode()).decode()

    return render_template("bad_news.html", rows=items, kind=kind, lang=lang, page=1, next_tok=next_tok)

@app.route("/bad_news/demo")
def bad_news_demo():
    """デモ用のダミーデータ表示"""
    test_items = [
        {
            "title": "桃田賢斗が全英オープンで快勝",
            "url": "https://example.com/news1",
            "author": "NHK",
            "published_at": "2025-09-10T09:00:00",
            "summary": "バドミントン男子シングルスで桃田賢斗選手が準々決勝進出。",
            "kind": "news",
            "image_url": "https://placehold.jp/300x200.png"
        },
        {
            "title": "Badminton World Championships Highlights",
            "url": "https://example.com/video1",
            "author": "YouTube",
            "published_at": "2025-09-09T18:30:00",
            "summary": None,
            "kind": "video",
            "image_url": "https://placehold.jp/300x200.png"
        }
    ]
    return render_template("bad_news.html", rows=test_items)

# ---- データ収集実行 ----
def collect_badminton_news():
    """バドミントンニュースを収集する"""
    print("バドミントンニュース収集を開始...")
    
    total = 0
    
    # 日本語ニュース
    total += fetch_google_news("バドミントン", "ja")
    time.sleep(1)
    
    # 英語ニュース
    total += fetch_google_news("badminton", "en")
    time.sleep(1)
    
    # 英語動画
    total += fetch_youtube_rss("badminton highlights", "en")
    time.sleep(1)
    
    # 日本語動画
    total += fetch_youtube_rss("バドミントン 試合", "ja")
    
    print(f"収集完了: 合計 {total}件の新しいコンテンツを追加")
    return total

@app.route("/admin/auto_collect", methods=["POST"])
def auto_collect():
    """定期的にニュース収集を実行"""
    try:
        total = collect_badminton_news()
        return {"success": True, "total": total}
    except Exception as e:
        return {"success": False, "error": str(e)}, 500
    
@app.route('/debug/routes')
def show_routes():
    """全ルート一覧を表示（開発時のみ）"""
    import urllib
    output = []
    for rule in app.url_map.iter_rules():
        methods = ','.join(rule.methods)
        line = urllib.parse.unquote(f"{rule.endpoint:30s} {methods:20s} {rule}")
        output.append(line)
    
    return '<br>'.join(sorted(output))

@app.route('/schedule/<string:schedule_id>/participant/<string:user_id>/remove', methods=['POST'])
@login_required
def remove_participant(schedule_id, user_id):
    """管理者が参加者を削除する"""
    try:
        # 管理者権限チェック
        if not current_user.administrator:
            return jsonify({'status': 'error', 'message': '権限がありません。'}), 403

        data = request.get_json()
        date = data.get('date')

        if not date:
            return jsonify({'status': 'error', 'message': '日付が不足しています。'}), 400

        # スケジュールの取得
        schedule_table = app.dynamodb.Table(app.table_name_schedule)
        response = schedule_table.get_item(
            Key={
                'schedule_id': schedule_id,
                'date': date
            }
        )
        schedule = response.get('Item')
        if not schedule:
            return jsonify({'status': 'error', 'message': 'スケジュールが見つかりません。'}), 404

        # 参加者リストから削除
        participants = schedule.get('participants', [])
        
        if user_id not in participants:
            return jsonify({'status': 'error', 'message': 'この参加者は登録されていません。'}), 400
        
        participants.remove(user_id)
        
        # bad-users-historyのstatusを更新（schedule_idも渡す）
        db.cancel_participation(user_id, date, schedule_id)  # ★ schedule_idを追加
        app.logger.info(f"✓ 管理者がユーザー {user_id} を削除しました (date={date}, schedule_id={schedule_id})")
        
        # DynamoDB の更新
        schedule_table.update_item(
            Key={
                'schedule_id': schedule_id,
                'date': date
            },
            UpdateExpression="SET participants = :participants, participants_count = :count",
            ExpressionAttributeValues={
                ':participants': participants,
                ':count': len(participants)
            }
        )

        # キャッシュのリセット
        cache.delete_memoized(get_schedules_with_formatting)

        return jsonify({
            'status': 'success',
            'message': '参加者を削除しました',
            'participants': participants,
            'participants_count': len(participants)
        })

    except ClientError as e:
        app.logger.error(f"DynamoDB ClientError: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'message': 'データベースエラーが発生しました。'}), 500
    except Exception as e:
        app.logger.error(f"Unexpected error in remove_participant: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'message': '予期しないエラーが発生しました。'}), 500

@app.route('/admin/user/<user_id>/disable_points', methods=['POST'])
@login_required
def disable_points(user_id):
    if session.get('user_id') != 'admin_user_id':  # 管理者チェック
        flash('権限がありません', 'danger')
        return redirect(url_for('index'))
    
    db = DynamoDB()
    if db.disable_user_points(user_id):
        flash('ポイントを失効しました', 'success')
    else:
        flash('ポイント失効に失敗しました', 'danger')
    
    return redirect(url_for('user_detail', user_id=user_id))

@app.route('/admin/user/<user_id>/enable_points', methods=['POST'])
@login_required
def enable_points(user_id):
    if session.get('user_id') != 'admin_user_id':  # 管理者チェック
        flash('権限がありません', 'danger')
        return redirect(url_for('index'))
    
    db = DynamoDB()
    if db.enable_user_points(user_id):
        flash('ポイント失効を解除しました', 'success')
    else:
        flash('ポイント失効解除に失敗しました', 'danger')
    
    return redirect(url_for('user_detail', user_id=user_id))

from uguu.timeline import uguu
from uguu.users import users
from uguu.post import post          
from schedule.views import bp as bp_schedule
from game.views import bp_game
from uguu.analytics import analytics

for blueprint in [uguu, post, users, analytics]:
    app.register_blueprint(blueprint, url_prefix='/uguu')

app.register_blueprint(bp_schedule, url_prefix='/schedule')
app.register_blueprint(bp_game, url_prefix='/game')

if __name__ == "__main__":
    app.run(debug=True)

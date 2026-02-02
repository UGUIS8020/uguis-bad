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

# DynamoDBインスタンスを作成（グローバルに1つ）
uguu_db = DynamoDB()

# log = logging.getLogger('werkzeug')
# log.setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger(__name__)

# Flask-Login用
login_manager = LoginManager()

cache = Cache()

def create_app():
    """アプリケーションの初期化と設定"""
    try:
        load_dotenv()
        app = Flask(__name__)

        # --- 環境判定 ---
        APP_ENV = os.getenv("APP_ENV", "development").lower()
        IS_PROD = APP_ENV in ("production", "prod")
        IS_LOCAL_HTTP = not IS_PROD  # ローカルはHTTP前提に

        # --- Secret Key（本番は必須。未設定なら起動失敗にする） ---
        secret_key = os.getenv("SECRET_KEY")
        if IS_PROD and not secret_key:
            raise RuntimeError("SECRET_KEY is required in production")
        app.config["SECRET_KEY"] = secret_key or "dev-only-insecure-key"  # 開発用の固定キー

        # --- セッション設定（環境で切り替え） ---
        app.config.update(
            PERMANENT_SESSION_LIFETIME=timedelta(days=30),
            SESSION_PERMANENT=True,
            # Flask-Session を使わないなら SESSION_TYPE は不要。使うなら 'filesystem' と Session(app) を有効化。
            # SESSION_TYPE='filesystem',
            SESSION_COOKIE_SECURE=not IS_LOCAL_HTTP,   # ← ローカルHTTPでは False、本番HTTPSで True
            SESSION_COOKIE_HTTPONLY=True,
            SESSION_COOKIE_SAMESITE="Lax",             # サブドメイン跨ぎや外部POSTが必要なら 'None' + Secure=True
            # 必要に応じてサブドメイン運用時:
            # SESSION_COOKIE_DOMAIN=".shibuya8020.com" if IS_PROD else None,
        )

        # --- Flask-Session を使う場合（任意） ---
        # from flask_session import Session
        # Session(app)

        # --- Cache 設定 ---
        app.config['CACHE_TYPE'] = 'SimpleCache'
        app.config['CACHE_DEFAULT_TIMEOUT'] = 600
        app.config['CACHE_THRESHOLD'] = 900
        app.config['CACHE_KEY_PREFIX'] = 'uguis_'
        cache.init_app(app)

        # --- AWS 認証 ---
        aws_credentials = {
            'aws_access_key_id': os.getenv("AWS_ACCESS_KEY_ID"),
            'aws_secret_access_key': os.getenv("AWS_SECRET_ACCESS_KEY"),
            'region_name': os.getenv("AWS_REGION", "ap-northeast-1"),
        }
        required_env_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "S3_BUCKET", "TABLE_NAME_USER", "TABLE_NAME_SCHEDULE"]
        missing = [v for v in required_env_vars if not os.getenv(v)]
        if IS_PROD and missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

        app.config["S3_BUCKET"] = os.getenv("S3_BUCKET", "default-bucket-name")
        app.config["AWS_REGION"] = os.getenv("AWS_REGION", "ap-northeast-1")
        app.config['S3_LOCATION'] = f"https://{app.config['S3_BUCKET']}.s3.{app.config['AWS_REGION']}.amazonaws.com/"

        app.s3 = boto3.client('s3', **aws_credentials)
        app.dynamodb = boto3.resource('dynamodb', **aws_credentials)
        app.table_name = os.getenv("TABLE_NAME_USER")
        app.table_name_users = os.getenv("TABLE_NAME_USER")
        app.table_name_schedule = os.getenv("TABLE_NAME_SCHEDULE")
        app.table = app.dynamodb.Table(app.table_name)
        app.table_schedule = app.dynamodb.Table(app.table_name_schedule)
        app.bad_table_name = os.getenv("BAD_TABLE_NAME", "bad_items")
        app.bad_table = app.dynamodb.Table(app.bad_table_name)

        # --- Flask-Login ---
        login_manager.init_app(app)
        login_manager.session_protection = "strong"
        # ブループリント使用時は 'auth.login' など正しい endpoint 名に直す
        login_manager.login_view = os.getenv("LOGIN_VIEW_ENDPOINT", "auth.login")
        login_manager.login_message = 'このページにアクセスするにはログインが必要です。'

        return app
    except Exception as e:
        logger.error(f"Failed to initialize application: {str(e)}")
        raise

# アプリケーションの初期化
app = create_app()

def tokyo_time():
    return datetime.now(JST)


@login_manager.user_loader
def load_user(user_id):
    # デバッグログを削除
    # app.logger.debug(f"Loading user with ID: {user_id}")

    if not user_id:
        # 警告ログは残す（重要な警告なので）
        app.logger.warning("No user_id provided to load_user")
        return None

    try:
        # DynamoDBリソースでテーブルを取得
        table = app.dynamodb.Table(app.table_name)  # テーブル名を取得
        response = table.get_item(
            Key={
                "user#user_id": user_id,   # パーティションキーをそのまま指定
            }
        )        

        if 'Item' in response:
            user_data = response['Item']
            user = User.from_dynamodb_item(user_data)
            # ユーザーデータのログ出力を削除（機密情報を含むため）
            # app.logger.info(f"DynamoDB user data: {user_data}")
            return user
        else:
            # このログも削除可能ですが、デバッグに役立つので残すか検討
            app.logger.info(f"No user found for ID: {user_id}")
            return None

    except Exception as e:
        # エラーログは残す（問題診断に重要）
        app.logger.error(f"Error loading user with ID: {user_id}: {str(e)}", exc_info=True)
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
            # Flaskアプリケーションコンテキスト内のDynamoDBテーブル取得
            table = current_app.dynamodb.Table(current_app.table_name)
            current_app.logger.debug(f"Querying email-index for email: {field.data}")

            response = table.query(
                IndexName='email-index',
                KeyConditionExpression=Key('email').eq(field.data)
            )
            current_app.logger.debug(f"Query response: {response}")

            if response.get('Items'):
                raise ValidationError('入力されたメールアドレスは既に登録されています。')

        except ValidationError:
            raise  # 明示的に通す

        except Exception as e:
            current_app.logger.error(f"Error validating email: {str(e)}", exc_info=True)
            raise ValidationError('メールアドレスの確認中にエラーが発生しました。')
                    
        
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
        # メールアドレスが変更されていない場合はバリデーションをスキップ
        if self.email_readonly or not field.data:
            return

        try:
            # DynamoDBにクエリを投げて重複チェックを実行
            response = self.table.query(
                IndexName='email-index',
                KeyConditionExpression='email = :email',
                ExpressionAttributeValues={
                    ':email': field.data
                }
            )

            app.logger.debug(f"Query response: {response}")

            if response.get('Items'):
                for item in response['Items']:
                    user_id = item.get('user#user_id') or item.get('user_id')
                    if user_id and user_id != self.id:
                        raise ValidationError('このメールアドレスは既に使用されています。他のメールアドレスをお試しください。')
        except ClientError as e:
            app.logger.error(f"Error querying DynamoDB: {e}")
            raise ValidationError('メールアドレスの確認中にエラーが発生しました。管理者にお問い合わせください。')
        except Exception as e:
            app.logger.error(f"Unexpected error querying DynamoDB: {e}")
            raise ValidationError('予期しないエラーが発生しました。管理者にお問い合わせください。')


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
        ]
    )

    # メールアドレス確認
    confirm_email = StringField(
        'メールアドレス（確認）',
        validators=[
            DataRequired(message='確認用メールアドレスを入力してください'),
            Email(message='正しいメールアドレスを入力してください'),
            EqualTo('email', message='メールアドレスが一致しません')
        ]
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
            # DynamoDB テーブル取得
            table = app.dynamodb.Table(app.table_name)
            current_app.logger.debug(f"Querying email-index for email: {field.data}")

            # email-indexを使用してクエリ
            response = table.query(
                IndexName='email-index',
                KeyConditionExpression=Key('email').eq(field.data)  # 修正済み
            )
            current_app.logger.debug(f"Query response: {response}")

            # 登録済みのメールアドレスが見つかった場合
            if response.get('Items'):
                raise ValidationError('このメールアドレスは既に使用されています。他のメールアドレスをお試しください。')

        except ValidationError as ve:
            # ValidationErrorはそのままスロー
            raise ve

        except Exception as e:
            # その他の例外をキャッチしてログに出力
            current_app.logger.error(f"Error validating email: {str(e)}")
            raise ValidationError('メールアドレスの確認中にエラーが発生しました。')


class LoginForm(FlaskForm):
    email = StringField('メールアドレス', validators=[DataRequired(message='メールアドレスを入力してください'), Email(message='正しいメールアドレスの形式で入力してください')])
    password = PasswordField('パスワード', validators=[DataRequired(message='パスワードを入力してください')])
    remember = BooleanField('ログイン状態を保持する')    
    submit = SubmitField('ログイン')

    def __init__(self, *args, **kwargs):
        super(LoginForm, self).__init__(*args, **kwargs)
        self.user = None  # self.userを初期化

    def validate_email(self, field):
        """メールアドレスの存在確認"""
        try:
            # メールアドレスでユーザーを検索
            response = app.table.query(
                IndexName='email-index',
                KeyConditionExpression='email = :email',
                ExpressionAttributeValues={
                    ':email': field.data
                }
            )
            
            items = response.get('Items', [])
            if not items:
                raise ValidationError('このメールアドレスは登録されていません')            
            
            # ユーザー情報を保存（パスワード検証で使用）
            self.user = items[0]
            # ユーザーをロード
            app.logger.debug(f"User found for email: {field.data}")       
           
        
        except Exception as e:
            app.logger.error(f"Login error: {e}")
            raise ValidationError('ログイン処理中にエラーが発生しました')

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

        return User(
            user_id=get_value('user#user_id'),
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

#                         # ✅ 参加回数（practice_count）を取得
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
        table_name = current_app.table_name
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
                "Keys": [{"user#user_id": uid} for uid in ids],
                "ProjectionExpression": "#uid, display_name, profile_image_url, skill_score, practice_count",
                "ExpressionAttributeNames": {"#uid": "user#user_id"},
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

        for uid in ids:
            user = by_id.get(uid)
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

@app.route("/", methods=['GET'])
@app.route("/index", methods=['GET'])
def index():
    try:
        # 軽量なスケジュール情報のみ取得
        schedules = get_schedules_with_formatting()

        # DynamoDB ユーザーテーブル
        user_table = current_app.dynamodb.Table(app.table_name)

        for schedule in schedules:
            # --- 参加者 ---
            participants_info = []
            raw_participants = schedule.get("participants", [])
            user_ids = []

            # 参加者IDの抽出（複数形式に対応）
            for item in raw_participants:
                if isinstance(item, dict) and "S" in item:
                    user_ids.append(item["S"])
                elif isinstance(item, dict) and "user_id" in item:
                    user_ids.append(item["user_id"])
                elif isinstance(item, str):
                    user_ids.append(item)

            # ユーザー詳細を取得
            for uid in user_ids:
                try:
                    res = user_table.get_item(Key={"user#user_id": uid})
                    user = res.get("Item")
                    if not user:
                        continue

                    # 画像URLは複数候補からフォールバック
                    url = (user.get("profile_image_url")
                           or user.get("profileImageUrl")
                           or user.get("large_image_url")
                           or "")
                    url = url.strip() if isinstance(url, str) else None

                    raw_practice = user.get("practice_count")
                    try:
                        join_count = int(raw_practice) if raw_practice is not None else 0
                    except (ValueError, TypeError):
                        join_count = 0

                    participants_info.append({
                        "user_id": user["user#user_id"],
                        "display_name": user.get("display_name", "不明"),
                        "profile_image_url": url if url and url.lower() != "none" else None,
                        "is_admin": bool(user.get("administrator")),
                        "join_count": join_count,   # ★これが必要
                    })
                except Exception:
                    # 個別の取得失敗はスキップ（全体は継続）
                    pass

            # 管理者を先頭にソート
            participants_info.sort(
                key=lambda x: (not x.get("is_admin", False), (x.get("join_count") or 0), x.get("display_name",""))
            )
            schedule["participants_info"] = participants_info

            # --- たら参加者 ---
            tara_participants_info = []
            raw_tara = schedule.get("tara_participants", [])
            tara_ids = []

            for item in raw_tara:
                if isinstance(item, dict) and "S" in item:
                    tara_ids.append(item["S"])
                elif isinstance(item, dict) and "user_id" in item:
                    tara_ids.append(item["user_id"])
                elif isinstance(item, str):
                    tara_ids.append(item)

            for uid in tara_ids:
                try:
                    res = user_table.get_item(Key={"user#user_id": uid})
                    user = res.get("Item")
                    if not user:
                        continue

                    url = (user.get("profile_image_url")
                           or user.get("profileImageUrl")
                           or user.get("large_image_url")
                           or "")
                    url = url.strip() if isinstance(url, str) else None

                    tara_participants_info.append({
                        "user_id": user["user#user_id"],
                        "display_name": user.get("display_name", "不明"),
                        "profile_image_url": url if url and url.lower() != "none" else None,
                        "is_admin": bool(user.get("administrator")),
                    })
                except Exception:
                    pass

            tara_participants_info.sort(key=lambda x: not x.get("is_admin", False))
            schedule["tara_participants_info"] = tara_participants_info
            schedule["tara_participants"] = tara_ids
            schedule["tara_count"] = len(tara_ids)

        # トップ画像
        image_files = [
            'images/top001.jpg',
            'images[top002.jpg',
            'images/top003.jpg',
            'images/top004.jpg',
            'images/top005.jpg'
        ]
        selected_image = random.choice(image_files)

        return render_template(
            "index.html",
            schedules=schedules,
            selected_image=selected_image,
            canonical=url_for('index', _external=True)
        )

    except Exception as e:
        logger.error(f"[index] スケジュール取得エラー: {e}")
        flash('スケジュールの取得中にエラーが発生しました', 'error')
        return render_template(
            "index.html",
            schedules=[],
            selected_image='images/default.jpg'
        )
    
@app.route("/schedule_koyomi", methods=['GET'])
@app.route("/schedule_koyomi/<int:year>/<int:month>", methods=['GET'])
def schedule_koyomi(year=None, month=None):
    try:
        # 年月が指定されていない場合は現在の年月を使用
        if year is None or month is None:
            today = date.today()
            year = today.year
            month = today.month
        
        # 前月と翌月の計算
        if month == 1:
            prev_month = 12
            prev_year = year - 1
        else:
            prev_month = month - 1
            prev_year = year
        
        if month == 12:
            next_month = 1
            next_year = year + 1
        else:
            next_month = month + 1
            next_year = year
        
        # カレンダー情報の生成 - cal_module を使用 
        calendar.setfirstweekday(calendar.SUNDAY)       
        cal = calendar.monthcalendar(year, month)
        
        # 軽量なスケジュール情報のみ取得
        schedules = get_schedules_with_formatting_all()

        # 参加者詳細情報の取得を追加
        user_table = current_app.dynamodb.Table("bad-users")  # 適宜変更

        for schedule in schedules:
            participants_info = []
            raw_participants = schedule.get("participants", [])
            user_ids = []

            # [{"S": "uuid"}] 形式にも対応
            for item in raw_participants:
                if isinstance(item, dict) and "S" in item:
                    user_ids.append(item["S"])
                elif isinstance(item, str):
                    user_ids.append(item)

            for user_id in user_ids:
                try:
                    res = user_table.get_item(Key={"user#user_id": user_id})
                    user = res.get("Item")
                    if user:
                        participants_info.append({
                            "user_id": user["user#user_id"],
                            "display_name": user.get("display_name", "不明")
                        })
                except Exception:
                    # ログ出力を削除
                    pass

            schedule["participants_info"] = participants_info
        
        # カレンダーデータの作成（簡易版）
        calendar_data = []
        today_date = date.today()
        
        for week in cal:
            week_data = []
            for day_num in week:
                if day_num == 0:
                    # 月外の日
                    week_data.append({
                        'day': 0,
                        'is_today': False,
                        'is_other_month': True,
                        'schedules': []
                    })
                else:
                    # その月の日
                    day_date = date(year, month, day_num)
                    date_str = day_date.strftime('%Y-%m-%d')
                    
                    # その日のスケジュール
                    day_schedules = [s for s in schedules if s.get("date") == date_str]
                    
                    week_data.append({
                        'day': day_num,
                        'is_today': day_date == today_date,
                        'is_other_month': False,
                        'schedules': day_schedules,
                        'has_schedule': len(day_schedules) > 0,
                        'has_full_schedule': any(s.get("participants_count", 0) >= s.get("max_participants", 0) for s in day_schedules)
                    })
            calendar_data.append(week_data)

        image_files = [
            'images/top001.jpg',
            'images/top002.jpg',
            'images/top003.jpg',
            'images/top004.jpg',
            'images/top005.jpg'
        ]

        selected_image = random.choice(image_files)

        # 月の日本語表記
        month_name = ["１月", "２月", "３月", "４月", "５月", "６月", 
                     "７月", "８月", "９月", "１０月", "１１月", "１２月"][month-1]

        # テンプレート名は schedule_koyomi.html
        return render_template("schedule_koyomi.html", 
                               schedules=schedules,
                               selected_image=selected_image,
                               canonical=url_for('schedule_koyomi', _external=True),
                               year=year,
                               month=month,
                               month_name=month_name,
                               prev_year=prev_year,
                               prev_month=prev_month,
                               next_year=next_year,
                               next_month=next_month,
                               calendar_data=calendar_data)
        
    except Exception as e:
        # 重要なエラーのみログ出力を残す
        logger.error(f"[schedule_koyomi] スケジュール取得エラー: {e}")
        flash('スケジュールの取得中にエラーが発生しました', 'error')
        return render_template("schedule_koyomi.html", 
                               schedules=[], 
                               selected_image='images/default.jpg',
                               year=date.today().year,
                               month=date.today().month)
    
    
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

        # 現在の参加者 / たら参加者
        participants = schedule.get('participants', []) or []
        tara_participants = schedule.get('tara_participants', []) or []

        history_table = app.dynamodb.Table("bad-users-history")

        # timezone-aware UTC（DynamoDB保存はこれで統一推奨）
        now_utc_iso = datetime.now(timezone.utc).isoformat()

        # 参加キャンセル
        if user_id in participants:
            participants.remove(user_id)
            message = "参加をキャンセルしました"
            is_joining = False

            # 1) 既存の履歴を「cancelled」に更新（あなたの既存関数を利用）
            try:
                db.cancel_participation(user_id, date, schedule_id)
                app.logger.info(
                    f"✓ ユーザー {user_id} の参加履歴をキャンセル済みに更新しました (date={date}, schedule_id={schedule_id})"
                )
            except Exception as e:
                app.logger.error(f"[cancel_participation エラー]: {e}")

            # 2) 保険として「キャンセル」履歴も1件追加（後の集計が安定）
            #    ※この1件が最新になり、同日の状態が cancelled として扱われます。
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

        # 参加登録（正式参加）
        else:
            participants.append(user_id)
            message = "参加登録が完了しました！"
            is_joining = True

            # 正式参加したら「たら」から自動削除
            if user_id in tara_participants:
                tara_participants.remove(user_id)
                app.logger.info(f"✓ ユーザー {user_id} の「たら」を自動削除しました (schedule_id={schedule_id}, date={date})")

            # 参加回数カウントは初回だけ（従来仕様を維持）
            if not previously_joined(schedule_id, user_id):
                try:
                    increment_practice_count(user_id)
                except Exception as e:
                    app.logger.error(f"[practice_count 更新エラー]: {e}")

            # 履歴は「毎回」追加してOK（同日の最新レコードが正式参加として残る）
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

        # スケジュール更新（participants / count / tara も一緒に更新）
        schedule_table.update_item(
            Key={'schedule_id': schedule_id, 'date': date},
            UpdateExpression="SET participants = :participants, participants_count = :count, tara_participants = :tara, updated_at = :ua",
            ExpressionAttributeValues={
                ':participants': participants,
                ':count': len(participants),
                ':tara': tara_participants,
                ':ua': now_utc_iso,  # ここもUTCで統一
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

def increment_practice_count(user_id):
    user_table = app.dynamodb.Table(app.table_name_users)

    user_table.update_item(
        Key={'user#user_id': user_id},
        UpdateExpression="SET practice_count = if_not_exists(practice_count, :start) + :inc",
        ExpressionAttributeValues={
            ':start': Decimal(0),
            ':inc': Decimal(1)
        }
    )

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
        skill_score = int(request.form.get('skill_score', 0))
        try:
            current_time = datetime.now(timezone.utc).isoformat()
            hashed_password = generate_password_hash(form.password.data, method='pbkdf2:sha256')
            user_id = str(uuid.uuid4())

            table = app.dynamodb.Table(app.table_name)

            temp_data = {
                "user#user_id": user_id,
                "display_name": form.display_name.data,
                "user_name": form.user_name.data,
                "gender": form.gender.data,
                "badminton_experience": form.badminton_experience.data,
                "email": form.email.data.lower(),
                "password": hashed_password,
                "phone": form.phone.data,
                "organization": "仮登録",
                "created_at": current_time,
                "administrator": False,
                "skill_score": skill_score,
                "date_of_birth": form.date_of_birth.data.isoformat()
            }

            # DynamoDBに保存
            table.put_item(Item=temp_data)

            # 仮登録成功後、ログインページにリダイレクト
            flash("仮登録が完了しました。ログインしてください。", "success")
            return redirect(url_for('login'))

        except Exception as e:
            logger.error(f"DynamoDBへの登録中にエラーが発生しました: {e}", exc_info=True)
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
                
                flash('ログインに成功しました。', 'success')
                
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


@app.route("/user_maintenance", methods=["GET", "POST"])
@login_required
def user_maintenance():
    try:
        # ソートパラメータを取得
        sort_by = request.args.get('sort_by', 'created_at')  # デフォルトは作成日
        order = request.args.get('order', 'desc')  # デフォルトは降順
        
        # テーブルからすべてのユーザーを取得
        response = app.table.scan()
        
        # ユーザーデータを処理
        users = response.get('Items', [])        
        for user in users:
            if 'user#user_id' in user:
                user['user_id'] = user.pop('user#user_id').replace('user#', '')
            
            # ポイント情報を取得
            try:
                user_stats = uguu_db.get_user_stats(user['user_id'])
                user['points'] = user_stats.get('uguu_points', 0)
                user['total_participation'] = user_stats.get('total_participation', 0)
                
                # デバッグ出力（最初のユーザーのみ）
                if users.index(user) == 0:
                    print(f"[DEBUG] First user - user_id: {user['user_id']}")
                    print(f"[DEBUG] uguu_points: {user['points']}")
                    print(f"[DEBUG] total_participation: {user['total_participation']}")
            except Exception as e:
                print(f"[ERROR] Failed to get stats for user {user.get('user_id', 'unknown')}: {e}")
                user['points'] = 0
                user['total_participation'] = 0
        
        # ソート処理
        reverse = (order == 'desc')
        
        if sort_by == 'points':
            sorted_users = sorted(users, key=lambda x: x.get('points', 0), reverse=reverse)
        elif sort_by == 'total_participation':
            sorted_users = sorted(users, key=lambda x: x.get('total_participation', 0), reverse=reverse)
        elif sort_by == 'user_name':
            sorted_users = sorted(users, key=lambda x: x.get('user_name', ''), reverse=reverse)
        elif sort_by == 'created_at':
            sorted_users = sorted(users, key=lambda x: x.get('created_at', ''), reverse=reverse)
        else:
            sorted_users = sorted(users, key=lambda x: x.get('created_at', ''), reverse=True)
        
        return render_template("user_maintenance.html", 
                             users=sorted_users, 
                             page=1, 
                             has_next=False,
                             sort_by=sort_by,
                             order=order)
    except Exception as e:
        print(f"[ERROR] Error in user_maintenance: {str(e)}")
        import traceback
        traceback.print_exc()
        flash('ユーザー一覧の読み込み中にエラーが発生しました。', 'error')
        return redirect(url_for('index'))

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
    

@app.route('/account/<string:user_id>', methods=['GET', 'POST'])
def account(user_id):
    try:
        table = app.dynamodb.Table(app.table_name)
        # 更新直後でも最新を読む
        response = table.get_item(Key={'user#user_id': user_id}, ConsistentRead=True)
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
                        Key={'user#user_id': user_id},
                        UpdateExpression=update_expression,
                        ExpressionAttributeValues=expression_values,
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
def delete_user(user_id):
    try:
        table = app.dynamodb.Table(app.table_name)
        response = table.get_item(
            TableName=app.table_name,
            Key={
                'user#user_id': user_id
            }
        )
        user = response.get('Item')
        
        if not user:
            flash('ユーザーが見つかりません。', 'error')
            return redirect(url_for('user_maintenance'))
            
          # 削除権限を確認（本人または管理者のみ許可）
        if current_user.id != user_id and not current_user.administrator:
            app.logger.warning(f"Unauthorized delete attempt by user {current_user.id} for user {user_id}.")
            abort(403)  # 権限がない場合は403エラー
        
        # ここで実際の削除処理を実行
        table = app.dynamodb.Table(app.table_name)
        table.delete_item(Key={'user#user_id': user_id})

         # ログイン中のユーザーが削除対象の場合はログアウト
        if current_user.id == user_id:
            logout_user()
            flash('アカウントが削除されました。再度ログインしてください。', 'info')
            return redirect(url_for('login'))

        flash('ユーザーアカウントが削除されました', 'success')
        return redirect(url_for('user_maintenance'))

    except ClientError as e:
        app.logger.error(f"DynamoDB error: {str(e)}")
        flash('データベースエラーが発生しました。', 'error')
        return redirect(url_for('user_maintenance'))
    

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
def update_skill_score():
    try:
        data = request.get_json()
        user_id = data.get("user_id")
        new_score = data.get("skill_score")

        if not user_id or new_score is None:
            return jsonify({"success": False, "error": "Missing parameters"}), 400

        # DynamoDB テーブルを取得
        table = app.dynamodb.Table(app.table_name)

        # データ更新
        table.update_item(
            Key={'user#user_id': user_id},
            UpdateExpression='SET skill_score = :score',
            ExpressionAttributeValues={':score': Decimal(str(new_score))}
        )

        return jsonify({
            "success": True,
            "message": "Skill score updated",
            "updated_score": new_score
        }), 200

    except Exception as e:
        app.logger.error(f"[update_skill_score] 更新エラー: {e}")
        return jsonify({
            "success": False,
            "error": "更新に失敗しました"
        }), 500
    
@app.route('/api/user_info/<user_id>')
def get_user_info(self, user_id: str):
    """
    ユーザー情報を取得（生年月日を含む）
    """
    try:
        response = self.table.get_item(
            Key={'user#user_id': user_id}
        )
        
        if 'Item' not in response:
            print(f"[WARN] ユーザー情報が見つかりません - user_id: {user_id}")
            return None
        
        item = response['Item']
        
        # 生年月日を取得（date_of_birthフィールド）
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
    
@app.route('/profile_image_edit/<user_id>', methods=['GET', 'POST'])
@login_required
def profile_image_edit(user_id):
    """プロフィール画像編集ページ"""
    try:
        table = app.dynamodb.Table(app.table_name)
        resp = table.get_item(Key={'user#user_id': user_id}, ConsistentRead=True)
        user = resp.get('Item')
        if not user:
            flash('ユーザーが見つかりません。', 'error')
            return redirect(url_for('index'))

        # 内部表記を統一
        user['user_id'] = user.get('user#user_id', user_id)

        # 権限チェック
        if session.get('_user_id') != user_id:
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

            # ★ここに追加★
            print(f"Debug received: sx={sx}, sy={sy}, ssz={ssz}")
            print(f"Debug orig_file exists: {orig_file is not None}")

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

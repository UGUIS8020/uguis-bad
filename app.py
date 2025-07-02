from flask_caching import Cache
from flask_wtf import FlaskForm
from flask import Flask, render_template, request, redirect, url_for, flash, abort, session, jsonify, current_app, json
from flask_login import UserMixin, LoginManager, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from wtforms import ValidationError, StringField, PasswordField, SubmitField, SelectField, DateField, BooleanField, IntegerField
from wtforms.validators import DataRequired, Email, EqualTo, Length, Optional, NumberRange
import pytz
import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
from werkzeug.utils import secure_filename
import uuid
from datetime import datetime, date, timedelta
import io
from PIL import Image, ExifTags
from botocore.exceptions import ClientError
import logging
import time
import random
from urllib.parse import urlparse, urljoin
from utils.db import get_schedule_table, get_schedules_with_formatting 
from uguu.post import post
from badminton_logs_functions import get_badminton_chat_logs
from decimal import Decimal


from dotenv import load_dotenv

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
        
        # Flaskアプリケーションの作成
        app = Flask(__name__)               
        
        # Secret Keyの設定
        app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", os.urandom(24))
        
          # セッションの永続化設定を追加
        app.config.update(
            PERMANENT_SESSION_LIFETIME = timedelta(days=30),  # セッション有効期限
            SESSION_PERMANENT = True,  # セッションを永続化
            SESSION_TYPE = 'filesystem',  # セッションの保存方式
            SESSION_COOKIE_SECURE = True,  # HTTPS接続のみ
            SESSION_COOKIE_HTTPONLY = True,  # JavaScriptからのアクセスを防止
            SESSION_COOKIE_SAMESITE = 'Lax'  # クロスサイトリクエスト制限
        )
        
        # キャッシュの設定と初期化
        app.config['CACHE_TYPE'] = 'SimpleCache'
        app.config['CACHE_DEFAULT_TIMEOUT'] = 600
        app.config['CACHE_THRESHOLD'] = 900
        app.config['CACHE_KEY_PREFIX'] = 'uguis_'

        # 既存のcacheオブジェクトを初期化
        cache.init_app(app)
    
        logger.info("Cache initialized with SimpleCache")                 
       

        # AWS認証情報の設定
        aws_credentials = {
            'aws_access_key_id': os.getenv("AWS_ACCESS_KEY_ID"),
            'aws_secret_access_key': os.getenv("AWS_SECRET_ACCESS_KEY"),
            'region_name': os.getenv("AWS_REGION", "us-east-1")
        }

        # 必須環境変数のチェック
        required_env_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "S3_BUCKET", "TABLE_NAME_USER", "TABLE_NAME_SCHEDULE","TABLE_NAME_BOARD"]
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

         # 必須環境変数をFlaskの設定に追加
        app.config["S3_BUCKET"] = os.getenv("S3_BUCKET", "default-bucket-name")
        app.config["AWS_REGION"] = os.getenv("AWS_REGION")
        app.config['S3_LOCATION'] = f"https://{app.config['S3_BUCKET']}.s3.{os.getenv('AWS_REGION')}.amazonaws.com/"
        print(f"S3_BUCKET: {app.config['S3_BUCKET']}")  # デバッグ用

         # AWSクライアントの初期化
        # app.s3 = boto3.client('s3', **aws_credentials)
        # app.dynamodb = boto3.resource('dynamodb', **aws_credentials)
        # app.dynamodb_resource = boto3.resource('dynamodb', **aws_credentials)

        # AWSクライアントの初期化
        app.s3 = boto3.client('s3', **aws_credentials)
        app.dynamodb = boto3.resource('dynamodb', **aws_credentials)

        # DynamoDBテーブルの設定
        app.table_name = os.getenv("TABLE_NAME_USER")
        app.table_name_board = os.getenv("TABLE_NAME_BOARD")
        app.table_name_schedule = os.getenv("TABLE_NAME_SCHEDULE")
        app.table_name_users = app.table_name
        app.table = app.dynamodb.Table(app.table_name)           # dynamodb_resource → dynamodb
        app.table_board = app.dynamodb.Table(app.table_name_board)     # dynamodb_resource → dynamodb
        app.table_schedule = app.dynamodb.Table(app.table_name_schedule) # dynamodb_resource → dynamodb

        # Flask-Loginの設定
        login_manager.init_app(app)
        login_manager.session_protection = "strong"
        login_manager.login_view = 'login'
        login_manager.login_message = 'このページにアクセスするにはログインが必要です。'

        # DynamoDBテーブルの初期化（init_tablesの実装が必要）
        # init_tables()

        logger.info("Application initialized successfully")
        return app

    except Exception as e:
        logger.error(f"Failed to initialize application: {str(e)}")
        raise


# アプリケーションの初期化
app = create_app()

def tokyo_time():
    return datetime.now(pytz.timezone('Asia/Tokyo'))


@login_manager.user_loader
def load_user(user_id):
    app.logger.debug(f"Loading user with ID: {user_id}")

    if not user_id:
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
            app.logger.info(f"DynamoDB user data: {user_data}")
            return user
        else:
            app.logger.info(f"No user found for ID: {user_id}")
            return None

    except Exception as e:
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
                 created_at=None, updated_at=None):
        super().__init__()
        self.id = user_id
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
            updated_at=get_value('updated_at')
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
@cache.memoize(timeout=600)
def get_participants_info(schedule):     
    participants_info = []

    try:
        user_table = app.dynamodb.Table(app.table_name)

        if 'participants' in schedule and schedule['participants']:            
            for participant_id in schedule['participants']:
                try:
                    # scanを削除してget_itemに変更
                    response = user_table.get_item(
                        Key={'user#user_id': participant_id}
                    )
                    
                    if 'Item' in response:
                        user = response['Item']                        
                        
                        raw_score = user.get('skill_score')
                        if isinstance(raw_score, Decimal):
                            skill_score = int(raw_score)
                        elif isinstance(raw_score, (int, float)):
                            skill_score = int(raw_score)
                        else:
                            skill_score = None

                        participants_info.append({                            
                            'user_id': user.get('user#user_id'),
                            'display_name': user.get('display_name', '名前なし'),
                            'skill_score': skill_score
                        })
                    else:
                        logger.warning(f"[参加者ID: {participant_id}] ユーザーが見つかりませんでした。")
                except Exception as e:
                    app.logger.error(f"参加者情報の取得中にエラー（ID: {participant_id}）: {str(e)}")

    except Exception as e:
        app.logger.error(f"参加者情報の全体取得中にエラー: {str(e)}")

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
def format_date(value):
    """日付を 'MM/DD' 形式にフォーマット"""
    try:
        date_obj = datetime.fromisoformat(value)  # ISO 形式から日付オブジェクトに変換
        return date_obj.strftime('%m/%d')        # MM/DD フォーマットに変換
    except ValueError:
        return value  # 変換できない場合はそのまま返す   



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
        logger.info(f"[index] スケジュール件数: {len(schedules)}")

        # 参加者詳細情報は取得しない

        image_files = [
            'images/top001.jpg',
            'images/top002.jpg',
            'images/top003.jpg',
            'images/top004.jpg',
            'images/top005.jpg'
        ]

        selected_image = random.choice(image_files)

        return render_template("index.html", 
                               schedules=schedules,
                               selected_image=selected_image,
                               canonical=url_for('index', _external=True))
        
    except Exception as e:
        logger.error(f"[index] スケジュール取得エラー: {e}")
        flash('スケジュールの取得中にエラーが発生しました', 'error')
        return render_template("index.html", schedules=[], selected_image='images/default.jpg')
    
    
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


@app.route('/temp_register', methods=['GET', 'POST'])
def temp_register():
    form = TempRegistrationForm()    

    if form.validate_on_submit():
        skill_score = int(request.form.get('skill_score', 0))
        try:
            current_time = datetime.now().isoformat()  # UTCで統一
            hashed_password = generate_password_hash(form.password.data, method='pbkdf2:sha256')
            user_id = str(uuid.uuid4())

            table = app.dynamodb.Table(app.table_name)

            temp_data = {
                "user#user_id": user_id,
                "display_name": form.display_name.data,
                "user_name": form.user_name.data,
                "gender": form.gender.data,
                "badminton_experience": form.badminton_experience.data,
                "email": form.email.data,
                "password": hashed_password,
                "phone": form.phone.data,
                "organization": "仮登録",
                "created_at": current_time,
                "administrator": False,
                "skill_score": skill_score
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


@app.route('/schedule/<string:schedule_id>/join', methods=['POST'])
@login_required
def join_schedule(schedule_id):
    try:
        # リクエストデータの取得
        data = request.get_json()
        date = data.get('date')

        if not date:
            app.logger.warning(f"'date' is not provided for schedule_id={schedule_id}")
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

        # 参加者リストの更新
        participants = schedule.get('participants', [])
        if current_user.id in participants:
            participants.remove(current_user.id)
            message = "参加をキャンセルしました"
            is_joining = False
        else:
            participants.append(current_user.id)
            message = "参加登録が完了しました！"
            is_joining = True
            if is_joining and not previously_joined(schedule_id, current_user.id):
                increment_practice_count(current_user.id)
                try:
                    history_table = app.dynamodb.Table("bad-users-history")
                    history_table.put_item(
                        Item={
                            "user_id": current_user.id,
                            "joined_at": datetime.utcnow().isoformat(),
                            "schedule_id": schedule_id,
                            "date": date,
                            "location": schedule.get("location", "未設定")
                        }
                    )
                except Exception as e:
                    app.logger.error(f"[履歴保存エラー] bad-users-history: {e}")

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

        # 成功レスポンス
        return jsonify({
            'status': 'success',
            'message': message,
            'is_joining': is_joining,
            'participants': participants,
            'participants_count': len(participants)
        })

    except ClientError as e:
        app.logger.error(f"DynamoDB ClientError: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'message': 'データベースエラーが発生しました。'}), 500
    except Exception as e:
        app.logger.error(f"Unexpected error in join_schedule: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'message': '予期しないエラーが発生しました。'}), 500

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
    if form.validate_on_submit():
        try:
            current_time = datetime.now().isoformat()
            hashed_password = generate_password_hash(form.password.data, method='pbkdf2:sha256')
            user_id = str(uuid.uuid4())          

            table = app.dynamodb.Table(app.table_name) 
            posts_table = app.dynamodb.Table('posts')  # 投稿用テーブル

            # メールアドレスの重複チェック用のクエリ
            email_check = table.query(
                IndexName='email-index',
                KeyConditionExpression='email = :email',
                ExpressionAttributeValues={
                    ':email': form.email.data
                }
            )

            if email_check.get('Items'):
                app.logger.warning(f"Duplicate email registration attempt: {form.email.data}")
                flash('このメールアドレスは既に登録されています。', 'error')
                return redirect(url_for('signup'))         

            app.table.put_item(
                Item={
                    "user#user_id": user_id,                    
                    "address": form.address.data,
                    "administrator": False,
                    "created_at": current_time,
                    "date_of_birth": form.date_of_birth.data.strftime('%Y-%m-%d'),
                    "display_name": form.display_name.data,
                    "email": form.email.data,
                    "furigana": form.furigana.data,
                    "gender": form.gender.data,
                    "password": hashed_password,
                    "phone": form.phone.data,
                    "post_code": form.post_code.data,
                    "updated_at": current_time,
                    "user_name": form.user_name.data,
                    "guardian_name": form.guardian_name.data,
                    "emergency_phone": form.emergency_phone.data,
                    "badminton_experience": form.badminton_experience.data,
                    "organization": form.organization.data,
                    # プロフィール用の追加フィールド
                    "bio": "",  # 自己紹介
                    "profile_image_url": "",  # プロフィール画像URL
                    "followers_count": 0,  # フォロワー数
                    "following_count": 0,  # フォロー数
                    "posts_count": 0  # 投稿数
                },
                ConditionExpression='attribute_not_exists(#user_id)',
                ExpressionAttributeNames={ "#user_id": "user#user_id"
                }
            )

            posts_table.put_item(
                Item={
                    'PK': f"USER#{user_id}",
                    'SK': 'TIMELINE#DATA',
                    'user_id': user_id,
                    'created_at': current_time,
                    'updated_at': current_time,
                    'last_post_time': None
                }
            )           
            

            # ログ出力を詳細に
            app.logger.info(f"New user created - ID: {user_id}, Organization: {form.organization.data}, Email: {form.email.data}")
            
            # 成功メッセージ
            flash('アカウントが作成されました！ログインしてください。', 'success')
            return redirect(url_for('login'))
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            
            app.logger.error(f"DynamoDB error - Code: {error_code}, Message: {error_message}")
            
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
            
    # フォームのバリデーションエラーの場合
    if form.errors:
        app.logger.warning(f"Form validation errors: {form.errors}")
        for field, errors in form.errors.items():
            for error in errors:
                flash(f'{form[field].label.text}: {error}', 'error')
    
    return render_template('signup.html', form=form)       

@app.route('/login', methods=['GET', 'POST'])
def login():

    if current_user.is_authenticated:
        return redirect(url_for('index')) 
    
    form = LoginForm()
    if form.validate_on_submit():
        try:
            print("う")
            # メールアドレスでユーザーを取得
            response = app.table.query(
                IndexName='email-index',
                KeyConditionExpression='email = :email',
                ExpressionAttributeValues={
                    ':email': form.email.data.lower()
                }
            )
            
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
        # テーブルからすべてのユーザーを取得
        response = app.table.scan()
        
        # デバッグ用に取得したユーザーデータを表示
        users = response.get('Items', [])        
        for user in users:
            if 'user#user_id' in user:
                user['user_id'] = user.pop('user#user_id').replace('user#', '')

        

         # created_at の降順でソート（新しい順）
        sorted_users = sorted(users, 
                            key=lambda x: x.get('created_at'),
                            reverse=True)

        app.logger.info(f"Sorted users by created_at: {sorted_users}")

        return render_template("user_maintenance.html", 
                             users=sorted_users, 
                             page=1, 
                             has_next=False)

    except ClientError as e:
        app.logger.error(f"DynamoDB error: {str(e)}")
        flash('ユーザー情報の取得に失敗しました。', 'error')
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
        response = table.get_item(Key={'user#user_id': user_id})
        user = response.get('Item')

        if not user:
            abort(404)

        user['user_id'] = user.pop('user#user_id')
        app.logger.info(f"User loaded successfully: {user_id}")

        form = UpdateUserForm(user_id=user_id, dynamodb_table=app.table)

        if request.method == 'GET':
            app.logger.debug("Initializing form with GET request.")
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
            except (ValueError, KeyError) as e:
                app.logger.error(f"Invalid date format for user {user_id}: {e}")
                form.date_of_birth.data = None
            form.organization.data = user.get('organization', '')
            form.guardian_name.data = user.get('guardian_name', '')
            form.emergency_phone.data = user.get('emergency_phone', '')
            return render_template('account.html', form=form, user=user)

        if request.method == 'POST' and form.validate_on_submit():            
            current_time = datetime.now().isoformat()
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
                field_value = getattr(form, field_name).data
                if field_value:
                    update_expression_parts.append(f"{db_field} = :{db_field}")
                    expression_values[f":{db_field}"] = field_value

            if form.date_of_birth.data:
                date_str = form.date_of_birth.data.strftime('%Y-%m-%d')
                update_expression_parts.append("date_of_birth = :date_of_birth")
                expression_values[':date_of_birth'] = date_str

            if form.password.data:
                hashed_password = generate_password_hash(form.password.data, method='pbkdf2:sha256')
                if hashed_password != user.get('password'):
                    update_expression_parts.append("password = :password")
                    expression_values[':password'] = hashed_password

            # 更新日時は常に更新
            update_expression_parts.append("updated_at = :updated_at")
            expression_values[':updated_at'] = current_time

            try:
                if update_expression_parts:
                    update_expression = "SET " + ", ".join(update_expression_parts)
                    app.logger.debug(f"Final update expression: {update_expression}")
                    response = table.update_item(
                        Key={'user#user_id': user_id},
                        UpdateExpression=update_expression,
                        ExpressionAttributeValues=expression_values,
                        ReturnValues="ALL_NEW"
                    )
                    app.logger.info(f"User {user_id} updated successfully: {response}")
                    flash('プロフィールが更新されました。', 'success')
                else:
                    flash('更新する項目がありません。', 'info')
                
                return redirect(url_for('account', user_id=user_id)) 
            except ClientError as e:
                # DynamoDB クライアントエラーの場合
                error_code = e.response['Error']['Code']
                error_message = e.response['Error']['Message']
                app.logger.error(f"DynamoDB ClientError in account route for user {user_id}: {error_message} (Code: {error_code})", exc_info=True)
                flash(f'DynamoDBでエラーが発生しました: {error_message}', 'error')       

            except Exception as e:                
                app.logger.error(f"Unexpected error in account route for user {user_id}: {e}", exc_info=True)
                flash('予期せぬエラーが発生しました。', 'error')
                return redirect(url_for('index'))

    except Exception as e:        
        app.logger.error(f"Unexpected error in account route for user {user_id}: {e}", exc_info=True)
        flash('予期せぬエラーが発生しました。', 'error')
        return redirect(url_for('index'))
                

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
    

# プロフィール表示用
@app.route('/user/<string:user_id>')
def user_profile(user_id):
    try:
        table = app.dynamodb.Table(app.table_name)
        response = table.get_item(Key={'user#user_id': user_id})
        user = response.get('Item')

        if not user:
            abort(404)

        # 投稿データの取得を追加
        posts_table = app.dynamodb.Table('posts')
        posts_response = posts_table.query(
            KeyConditionExpression="PK = :pk AND begins_with(SK, :sk_prefix)",
            ExpressionAttributeValues={
                ':pk': f"USER#{user_id}",
                ':sk_prefix': 'METADATA#'
            }
        )
        posts = posts_response.get('Items', [])

        return render_template('user_profile.html', user=user, posts=posts)

    except Exception as e:
        app.logger.error(f"Error loading profile: {str(e)}")
        flash('プロフィールの読み込み中にエラーが発生しました', 'error')
        return redirect(url_for('index'))

    
@app.route("/uguis2024_tournament")
def uguis2024_tournament():
    return render_template("uguis2024_tournament.html")

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
def get_user_info(user_id):
    try:
        table = app.dynamodb.Table(app.table_name)
        response = table.get_item(Key={'user#user_id': user_id})
        item = response.get('Item')

        if not item:
            return jsonify({'error': 'ユーザーが見つかりません'}), 404

        # 戦闘力を処理
        raw_score = item.get('skill_score')
        if isinstance(raw_score, Decimal):
            skill_score = int(raw_score)
        elif isinstance(raw_score, (int, float)):
            skill_score = int(raw_score)
        else:
            skill_score = None

        return jsonify({
            'user_id': item.get('user#user_id'),
            'display_name': item.get('display_name', '名前なし'),
            'skill_score': skill_score
        })

    except Exception as e:
        app.logger.error(f"/api/user_info エラー: {e}")
        return jsonify({'error': 'サーバーエラー'}), 500

dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")
match_table = dynamodb.Table("bad-game-match_entries")

from uguu.timeline import uguu
from uguu.users import users
from schedule.views import bp as bp_schedule
from game.views import bp_game

for blueprint in [uguu, post, users]:
    app.register_blueprint(blueprint, url_prefix='/uguu')

app.register_blueprint(bp_schedule, url_prefix='/schedule')
app.register_blueprint(bp_game, url_prefix='/game')

# if __name__ == "__main__":       
#     app.run(debug=True)


if __name__ == "__main__":
    app.run(debug=True)


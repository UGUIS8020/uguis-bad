from flask_wtf import FlaskForm
from wtforms import DateField, StringField, IntegerField, SelectField, SubmitField, HiddenField, BooleanField
from wtforms.validators import DataRequired, NumberRange

class ScheduleForm(FlaskForm):
    title = StringField('タイトル')
    date = DateField('日付', validators=[DataRequired()])
    day_of_week = StringField('曜日', render_kw={'readonly': True})

    venue = SelectField('体育館', validators=[DataRequired()], choices=[
        ('', '体育館を選択してください'),
        ('越谷市立地域スポーツセンター', '越谷市立地域スポーツセンター'),
        ('越谷市立総合体育館', '越谷市立総合体育館'),
        ('ウィングハット', 'ウィングハット')
    ])

    # ★ シンプル: 動的選択肢対応
    court = SelectField('コート', 
        validators=[DataRequired()], 
        choices=[('', 'まず体育館を選択してください')],
        validate_choice=False  # ★ JavaScriptの値を受け入れる
    )

    max_participants = IntegerField('参加人数制限', 
        validators=[
            DataRequired(),
            NumberRange(min=1, max=50, message='1人から50人までの間で設定してください')
        ],
        default=15,
        render_kw={"min": "1", "max": "50", "type": "number"}
    )
    
    start_time = SelectField('開始時間', validators=[DataRequired()], choices=[
        ('', '選択してください')] + 
        [(f"{h:02d}:00", f"{h:02d}:00") for h in range(9, 23)]
    )
    
    end_time = SelectField('終了時間', validators=[DataRequired()], choices=[
        ('', '選択してください')] + 
        [(f"{h:02d}:00", f"{h:02d}:00") for h in range(10, 24)]
    )
    
    status = SelectField('ステータス', choices=[
        ('active', '有効'),
        ('deleted', '削除済'),
        ('cancelled', '中止')
    ], default='active')

    detail = StringField('詳細（個別ページのみ表示）')
    is_pinned = BooleanField('トップに固定表示する')

    team_id = HiddenField('チームID')

    submit = SubmitField('📅 スケジュール登録')

# ★ シンプル: 必要最小限の定数
COURT_CAPACITY = {
    # 越谷市立地域スポーツセンター
    'A面(3面)': 20,
    'B面(3面)': 20,
    'AB両面(6面)': 40,
    
    # 越谷市立総合体育館
    '第一体育室(2面)': 16,
    '第一体育室(6面)': 48,
    '第二体育室(3面)': 24,
    '第二体育室(6面)': 48,
    
    # ウィングハット
    'メインコート': 20,
    'サブコート': 12,
}

# ★ シンプル: ヘルパー関数
def get_court_capacity(court):
    """コートの収容人数を取得"""
    return COURT_CAPACITY.get(court, 20)  # デフォルト20人

def get_venue_short_name(venue):
    """体育館の短縮名を取得"""
    mapping = {
        '越谷市立地域スポーツセンター': '北越谷',
        '越谷市立総合体育館': '総合体育館',
        'ウィングハット': 'ウィングハット'
    }
    return mapping.get(venue, venue)
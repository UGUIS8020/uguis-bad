from flask_wtf import FlaskForm
from wtforms import DateField, StringField, IntegerField, SelectField, SubmitField, ValidationError
from wtforms.validators import DataRequired, NumberRange


class ScheduleForm(FlaskForm):
    date = DateField('日付', validators=[DataRequired()])
    day_of_week = StringField('曜日', render_kw={'readonly': True})  # 自動入力用
    
    venue = SelectField('会場', validators=[DataRequired()], choices=[
        ('', '選択してください'),
        ('北越谷 A面', '北越谷 A面'),
        ('北越谷 B面', '北越谷 B面'),
        ('北越谷 AB面', '北越谷 AB面'),
        ('総合体育館 第一 2面', '総合体育館 第一 2面'),
        ('総合体育館 第一 6面', '総合体育館 第一 6面'),
        ('総合体育館 第二 3面', '総合体育館 第二 3面'),
        ('ウィングハット', 'ウィングハット')
    ])

    max_participants = IntegerField('参加人数制限', 
        validators=[
            DataRequired(),
            NumberRange(min=1, max=50, message='1人から50人までの間で設定してください')
        ],
        default=10,
        render_kw={
            "min": "1",
            "max": "50",
            "type": "number"
        }
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
    
    submit = SubmitField('登録')

    def validate_max_participants(self, field):
        """
        会場に応じた参加人数の上限をチェック
        """
        venue = self.venue.data
        if venue:
            max_allowed = {
                '北越谷 A面': 20,
                '北越谷 B面': 20,
                '北越谷 AB面': 40,
                '総合体育館 第一 2面': 16,
                '総合体育館 第一 6面': 48,
                '総合体育館 第二 3面': 24,
                'ウィングハット': 32
            }.get(venue)
            
            if max_allowed and field.data > max_allowed:
                raise ValidationError(f'この会場の最大参加可能人数は{max_allowed}人です')
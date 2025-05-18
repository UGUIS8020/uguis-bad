from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import login_required, current_user
from datetime import datetime
from botocore.exceptions import ClientError
from .forms import ScheduleForm
from utils.db import get_schedule_table, get_schedules_with_formatting
import logging
import uuid
from flask import jsonify



logger = logging.getLogger(__name__)
# Blueprintの作成
bp = Blueprint('schedule', __name__)

def init_app(app, db, cache):
    # ここでapp, db, cacheに依存する初期化を行う
    pass


@bp.route("/admin/schedules", methods=['GET', 'POST'])
@login_required
def admin_schedules():
    if not current_user.administrator:
        flash('管理者権限が必要です', 'warning')
        return redirect(url_for('index'))

    form = ScheduleForm()  # フォームを追加
    
    if form.validate_on_submit():
        try:
            schedule_table = get_schedule_table()
            if not schedule_table:
                raise ValueError("Schedule table is not initialized")

            schedule_data = {
                'schedule_id': str(uuid.uuid4()),
                'date': form.date.data.isoformat(),
                'day_of_week': form.day_of_week.data,
                'venue': form.venue.data,
                'start_time': form.start_time.data,
                'end_time': form.end_time.data,
                'max_participants': form.max_participants.data,
                'created_at': datetime.now().isoformat(),
                'participants_count': 0,
                'status': 'active'
            }

            schedule_table.put_item(Item=schedule_data)            
            flash('スケジュールが登録されました', 'success')
            return redirect(url_for('schedules'))

        except Exception as e:
            logger.error(f"Error registering schedule: {e}")
            flash('スケジュールの登録中にエラーが発生しました', 'error')

    try:
        schedule_table = get_schedule_table()
        logger.info(f"Schedule table retrieved: {schedule_table}")

        response = schedule_table.scan()
        logger.info(f"Scan response: {response}")

        all_schedules = response.get('Items', [])
        schedules = sorted(
            all_schedules,
            key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d').date()
        )
        
        return render_template(
            "schedule/schedules.html", 
            schedules=schedules,
            form=form  # フォームをテンプレートに渡す
        )
        
    except Exception as e:
        logger.error(f"Error getting admin schedules: {str(e)}")
        flash('スケジュールの取得中にエラーが発生しました', 'error')
        return redirect(url_for('index'))


@bp.route("/edit_schedule/<schedule_id>", methods=['GET', 'POST'])
@login_required
def edit_schedule(schedule_id):
    if not current_user.administrator:
        flash('管理者権限が必要です', 'warning')
        return redirect(url_for('index'))

    logging.debug(f"Fetching schedule for ID: {schedule_id}")
    form = ScheduleForm()
    table = get_schedule_table()

    try:
        # スケジュールの取得（スキャンではなくGetItemを使用）
        schedule = None
        schedules = table.query(
            KeyConditionExpression='schedule_id = :sid',
            ExpressionAttributeValues={
                ':sid': schedule_id
            }
        ).get('Items', [])
        
        if schedules:
            schedule = schedules[0]
        else:
            flash('スケジュールが見つかりません', 'error')
            return redirect(url_for('index'))
        
        if request.method == 'GET':
            form.date.data = datetime.strptime(schedule['date'], '%Y-%m-%d').date()
            form.day_of_week.data = schedule['day_of_week']
            form.venue.data = schedule['venue']
            form.start_time.data = schedule['start_time']
            form.end_time.data = schedule['end_time']
            form.status.data = schedule.get('status', 'active')
            form.max_participants.data = schedule.get('max_participants', 10)
            
        elif request.method == 'POST':
            if form.validate_on_submit():
                try:
                    # 参加者数のチェック
                    current_participants = schedule.get('participants', [])
                    if len(current_participants) > form.max_participants.data:
                        flash('参加人数制限は現在の参加者数より少なく設定できません。', 'error')
                        return render_template(
                            'edit_schedule.html',
                            form=form,
                            schedule=schedule,
                            schedule_id=schedule_id
                        )

                    # UpdateItemを使用して特定のフィールドを更新
                    table.update_item(
                        Key={
                            'schedule_id': schedule_id,
                            'date': schedule['date']  # DynamoDBのプライマリーキー
                        },
                        UpdateExpression="SET day_of_week = :dow, venue = :v, start_time = :st, "
                                       "end_time = :et, max_participants = :mp, "
                                       "updated_at = :ua, #status = :s",
                        ExpressionAttributeValues={
                            ':dow': form.day_of_week.data,
                            ':v': form.venue.data,
                            ':st': form.start_time.data,
                            ':et': form.end_time.data,
                            ':mp': form.max_participants.data,
                            ':ua': datetime.now().isoformat(),
                            ':s': form.status.data
                        },
                        ExpressionAttributeNames={
                            '#status': 'status'  # statusは予約語なので別名を使用
                        }
                    )                    
                    
                    flash('スケジュールを更新しました', 'success')
                    return redirect(url_for('index'))
                    
                except ClientError as e:                    
                    flash('スケジュールの更新中にエラーが発生しました', 'error')
            else:
                logging.error(f"Form validation errors: {form.errors}")
                flash('入力内容に問題があります', 'error')
            
    except ClientError as e:        
        flash('スケジュールの取得中にエラーが発生しました', 'error')
        return redirect(url_for('index'))
    
    return render_template(
        'schedule/edit_schedule.html', 
        form=form, 
        schedule=schedule, 
        schedule_id=schedule_id
    )



# @bp.route("/delete_schedule/<schedule_id>", methods=['POST'])
# def delete_schedule(schedule_id):
#     try:
#         # フォームから date を取得
#         date = request.form.get('date')

#         if not date:            
#             flash('日付が不足しています。', 'error')
#             return redirect(url_for('index'))

#         # DynamoDB テーブルを取得
#         table = get_schedule_table()        
        
#         # schedule_id と date を使ってステータスを更新
#         update_response = table.update_item(
#             Key={
#                 'schedule_id': schedule_id,
#                 'date': date
#             },
#             UpdateExpression="SET #status = :status, updated_at = :updated_at",
#             ExpressionAttributeNames={
#                 '#status': 'status'  # statusは予約語なので#を使用
#             },
#             ExpressionAttributeValues={
#                 ':status': 'deleted',
#                 ':updated_at': datetime.now().isoformat()
#             },
#             ReturnValues="ALL_NEW"  # 更新後の項目を返す
#         )        
        
#         flash('スケジュールを削除しました', 'success')

#         # キャッシュをリセット
#         cache.delete_memoized(get_schedules_with_formatting)

#     except ClientError as e:        
#         flash('スケジュールの更新中にエラーが発生しました', 'error')

#     except Exception as e:        
#         flash('スケジュールの更新中にエラーが発生しました', 'error')

#     return redirect(url_for('index'))

@bp.route("/delete_schedule/<schedule_id>", methods=['POST'])
@login_required
def delete_schedule(schedule_id):
    # from app import cache の代わりに
    from flask import current_app
    
    if not current_user.administrator:
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'success': False, 'message': '管理者権限が必要です'})
        flash('管理者権限が必要です', 'warning')
        return redirect(url_for('index'))
        
    try:
        date = request.form.get('date')
        if not date:
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({'success': False, 'message': '日付が不足しています'})
            flash('日付が不足しています。', 'error')
            return redirect(url_for('index'))

        table = get_schedule_table()
        delete_type = request.form.get('delete_type', 'soft')
        
        if delete_type == 'permanent':
            # 完全削除
            delete_response = table.delete_item(
                Key={
                    'schedule_id': schedule_id,
                    'date': date
                }
            )
            message = 'スケジュールを完全に削除しました'
        else:
            # 論理削除
            update_response = table.update_item(
                Key={
                    'schedule_id': schedule_id,
                    'date': date
                },
                UpdateExpression="SET #status = :status, updated_at = :updated_at",
                ExpressionAttributeNames={
                    '#status': 'status'
                },
                ExpressionAttributeValues={
                    ':status': 'deleted',
                    ':updated_at': datetime.now().isoformat()
                }
            )
            message = 'スケジュールを削除しました'

        # キャッシュをリセット - current_appを使用
        if hasattr(current_app, 'cache'):
            current_app.cache.delete_memoized(get_schedules_with_formatting)
        
        # AJAX リクエストとHTMLリクエストで異なるレスポンスを返す
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'success': True, 'message': message})
        else:
            flash(message, 'success')
            return redirect(url_for('schedule.admin_schedules'))

    except Exception as e:
        logger.error(f"Error deleting schedule: {str(e)}")  # str(e)を使用して確実に文字列化
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'success': False, 'message': 'スケジュールの削除中にエラーが発生しました'})
        flash('スケジュールの削除中にエラーが発生しました', 'error')
        return redirect(url_for('schedule.admin_schedules'))
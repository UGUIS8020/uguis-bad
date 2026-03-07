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
    logger.info("🔐 スケジュール管理ページにアクセス")
    
    if not current_user.administrator:
        flash('管理者権限が必要です', 'warning')
        return redirect(url_for('index'))

    form = ScheduleForm()
    logger.debug(f"📝 フォーム初期化: {form}")

    # ★★★ 緊急追加: POSTリクエストの場合のログ出力 ★★★
    if request.method == 'POST':
        print("=" * 60)  # print を使って確実に出力
        print("🚨 緊急デバッグ: POSTリクエスト受信")
        print(f"📋 フォームデータ: {dict(request.form)}")
        print(f"🔍 court フィールド: '{request.form.get('court')}'")
        print(f"🔍 venue フィールド: '{request.form.get('venue')}'")
        print(f"🔍 date フィールド: '{request.form.get('date')}'")
        
        # フォームエラーを事前チェック
        form_valid = form.validate()
        print(f"🔍 form.validate(): {form_valid}")
        
        if not form_valid:
            print("❌ フォームエラー:")
            for field, errors in form.errors.items():
                print(f"   {field}: {errors}")
        
        # courtフィールドが存在するかチェック
        if hasattr(form, 'court'):
            print(f"courtフィールド存在: {form.court}")
            print(f"courtフィールド値: '{form.court.data}'")
            print(f"courtフィールド選択肢: {form.court.choices}")
        else:
            print("❌ courtフィールドが存在しません！")
        
        print("=" * 60)

    if form.validate_on_submit():
        logger.info("フォームバリデーション成功")
        try:
            schedule_table = get_schedule_table()
            if not schedule_table:
                raise ValueError("Schedule table is not initialized")

            schedule_id = str(uuid.uuid4())

            # ★ court フィールドの安全な取得
            court_value = ''
            if hasattr(form, 'court') and form.court.data:
                court_value = form.court.data
            else:
                # court フィールドがない場合のフォールバック
                court_value = request.form.get('court', 'unknown')
            comment_value = request.form.get('comment', '').strip()

            schedule_data = {
                'schedule_id': schedule_id,
                'date': form.date.data.isoformat(),
                'day_of_week': form.day_of_week.data,
                'venue': form.venue.data,
                'court': form.court.data,   # ★ 安全に取得
                'start_time': form.start_time.data,
                'end_time': form.end_time.data,
                'max_participants': form.max_participants.data,
                'comment': comment_value,
                'created_at': datetime.now().isoformat(),
                'participants_count': 0,
                'status': form.status.data
            }

            logger.info(f"🗂️ 登録データ: {schedule_data}")
            print(f"🗂️ 登録データ: {schedule_data}")  # print でも出力

            schedule_table.put_item(Item=schedule_data)
            logger.info(f"スケジュール登録成功（ID: {schedule_id}）")
            print(f"スケジュール登録成功（ID: {schedule_id}）")
            flash('スケジュールが登録されました', 'success')
            return redirect(url_for('schedule.admin_schedules'))

        except Exception as e:
            logger.exception("❌ スケジュール登録時にエラー発生")
            print(f"❌ スケジュール登録時にエラー発生: {e}")
            flash('スケジュールの登録中にエラーが発生しました', 'error')
    else:
        if request.method == 'POST':
            logger.debug("⚠️ フォームバリデーション失敗")
            print("⚠️ フォームバリデーション失敗")
            for field, errors in form.errors.items():
                logger.debug(f"❌ {field}: {errors}")
                print(f"❌ {field}: {errors}")

    try:
        schedule_table = get_schedule_table()
        response = schedule_table.scan()
        all_schedules = response.get('Items', [])

        logger.info(f"スケジュール取得件数: {len(all_schedules)} 件")

        schedules = sorted(
            all_schedules,
            key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d').date()
        )

        return render_template(
            "schedule/schedules.html",
            schedules=schedules,
            form=form
        )
    except Exception as e:
        logger.exception("❌ スケジュールの取得中にエラー発生")
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
            # ★ コメントの初期値設定を追加
            if hasattr(form, 'comment'):
                form.comment.data = schedule.get('comment', '')
            
        elif request.method == 'POST':
            print("=" * 60)  # デバッグ用
            print("🚨 編集画面POSTリクエスト受信")
            print(f"📋 フォームデータ: {dict(request.form)}")
            print(f"🔍 comment フィールド: '{request.form.get('comment')}'")
            print("=" * 60)
            
            if form.validate_on_submit():
                try:
                    # 参加者数のチェック
                    current_participants = schedule.get('participants', [])
                    if len(current_participants) > form.max_participants.data:
                        flash('参加人数制限は現在の参加者数より少なく設定できません。', 'error')
                        return render_template(
                            'schedule/edit_schedule.html',
                            form=form,
                            schedule=schedule,
                            schedule_id=schedule_id
                        )

                    # ★ コメントの取得
                    comment_value = ''
                    if hasattr(form, 'comment') and form.comment.data:
                        comment_value = form.comment.data.strip()
                    else:
                        # フォームにcommentフィールドがない場合の安全な取得
                        comment_value = request.form.get('comment', '').strip()

                    # UpdateItemを使用して特定のフィールドを更新（コメントを追加）
                    table.update_item(
                        Key={
                            'schedule_id': schedule_id,
                            'date': schedule['date']  # DynamoDBのプライマリーキー
                        },
                        UpdateExpression="SET day_of_week = :dow, venue = :v, start_time = :st, "
                                       "end_time = :et, max_participants = :mp, "
                                       "updated_at = :ua, #status = :s, #comment = :c",  # ★ commentを追加
                        ExpressionAttributeValues={
                            ':dow': form.day_of_week.data,
                            ':v': form.venue.data,
                            ':st': form.start_time.data,
                            ':et': form.end_time.data,
                            ':mp': form.max_participants.data,
                            ':ua': datetime.now().isoformat(),
                            ':s': form.status.data,
                            ':c': comment_value  # ★ コメント値を追加
                        },
                        ExpressionAttributeNames={
                            '#status': 'status',  # statusは予約語なので別名を使用
                            '#comment': 'comment'  # ★ commentも予約語なので別名を追加
                        }
                    )                    
                    
                    print(f"スケジュール更新成功（ID: {schedule_id}）")
                    flash('スケジュールを更新しました', 'success')
                    return redirect(url_for('index'))
                    
                except ClientError as e:
                    print(f"❌ DynamoDB更新エラー: {e}")
                    flash('スケジュールの更新中にエラーが発生しました', 'error')
            else:
                print("⚠️ 編集フォームバリデーション失敗")
                print(f"❌ フォームエラー: {form.errors}")
                logging.error(f"Form validation errors: {form.errors}")
                flash('入力内容に問題があります', 'error')
            
    except ClientError as e:        
        flash('スケジュールの取得中にエラーが発生しました', 'error')
        return redirect(url_for('index'))
    
    # ★ テンプレートに渡すscheduleデータにcommentが含まれていることを確認
    if 'comment' not in schedule:
        schedule['comment'] = ''
    
    return render_template(
        'schedule/edit_schedule.html', 
        form=form, 
        schedule=schedule, 
        schedule_id=schedule_id
    )

@bp.route("/delete_schedule/<schedule_id>", methods=['POST'])
@login_required
def delete_schedule(schedule_id):    
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
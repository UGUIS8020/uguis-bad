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

def leader_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # 1. ログインしているか（current_userが有効か）
        if not current_user.is_authenticated:
            return redirect(url_for('auth.login'))
        
        # 2. 管理者(administrator=True) または roleがstaffなら許可
        # getattrを使うことで、属性が存在しない場合でもエラーにならず安全に判定できます
        is_admin = getattr(current_user, 'administrator', False)
        is_staff = getattr(current_user, 'role', '') == 'staff'
        
        if not (is_admin or is_staff):
            # 権限がない場合は403エラーを出すか、メッセージを出してリダイレクト
            flash('リーダーまたは管理者権限が必要です', 'danger')
            return redirect(url_for('main.index')) # メインページなどへ
            
        return f(*args, **kwargs)
    return decorated_function


@bp.route("/admin/schedules", methods=['GET', 'POST'])
@login_required
def admin_schedules():
    logger.info("スケジュール管理ページにアクセス")
    
    # --- 1. 権限チェックの修正 ---
    # システム管理者（True）でもなく、かつ ロールも admin でない場合は追い返す
    if not current_user.administrator and current_user.role != 'admin':
        flash('この機能を利用する権限がありません', 'warning')
        return redirect(url_for('index'))

    form = ScheduleForm()
    
    if form.validate_on_submit():
        logger.info("フォームバリデーション成功")
        try:
            schedule_table = get_schedule_table()
            schedule_id = uuid.uuid4().hex[:8]

            # --- 2. 保存データに team_id を追加 ---
            schedule_data = {
                'schedule_id': schedule_id,
                'team_id': current_user.team_id,
                'display_name': current_user.display_name,
                'date': form.date.data.isoformat(),
                'day_of_week': form.day_of_week.data,
                'venue': form.venue.data,
                'court': form.court.data,
                'start_time': form.start_time.data,
                'end_time': form.end_time.data,
                'max_participants': form.max_participants.data,
                'title': request.form.get('title', '').strip(),
                'comment': request.form.get('comment', '').strip(),
                'created_at': datetime.now().isoformat(),
                'participants_count': 0,
                'status': form.status.data,
                'is_pinned': form.is_pinned.data,
            }

            schedule_table.put_item(Item=schedule_data)
            flash('スケジュールが登録されました', 'success')
            return redirect(url_for('schedule.admin_schedules'))

        except Exception as e:
            logger.exception("❌ スケジュール登録時にエラー発生")
            flash('スケジュールの登録中にエラーが発生しました', 'error')

    # --- 3. 表示データの取得とフィルタリング ---
    try:
        schedule_table = get_schedule_table()
        response = schedule_table.scan()
        all_schedules = response.get('Items', [])

        # 権限に応じて表示するデータを分ける
        if current_user.administrator:
            # 渋谷さん（システム管理者）は全部見れる
            filtered_schedules = all_schedules
        else:
            # チーム管理者は自分の team_id の予定だけ
            filtered_schedules = [
                s for s in all_schedules 
                if s.get('team_id') == current_user.team_id
            ]

        # 削除済みを除外して日付順に並び替え
        active_schedules = [s for s in filtered_schedules if s.get('status') != 'deleted']
        schedules = sorted(
            active_schedules,
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


@bp.route("/admin/schedules/deleted", methods=['GET'])
@login_required
def deleted_schedules():
    if not current_user.administrator and current_user.role != 'admin':
        flash('この機能を利用する権限がありません', 'warning')
        return redirect(url_for('index'))

    try:
        schedule_table = get_schedule_table()
        response = schedule_table.scan()
        all_schedules = response.get('Items', [])

        if current_user.administrator:
            filtered = all_schedules
        else:
            filtered = [s for s in all_schedules if s.get('team_id') == current_user.team_id]

        deleted = [s for s in filtered if s.get('status') == 'deleted']
        schedules = sorted(deleted, key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d').date(), reverse=True)

        return render_template("schedule/deleted_schedules.html", schedules=schedules)

    except Exception as e:
        logger.exception("❌ 削除済みスケジュールの取得中にエラー発生")
        flash('スケジュールの取得中にエラーが発生しました', 'error')
        return redirect(url_for('schedule.admin_schedules'))


@bp.route("/edit_schedule/<schedule_id>", methods=['GET', 'POST'])
@login_required
def edit_schedule(schedule_id):
    # --- 1. アクセス権限のチェック ---
    # システム管理者でもなく、チーム管理者でもない場合は追い返す
    if not current_user.administrator and current_user.role != 'admin':
        flash('編集権限が必要です', 'warning')
        return redirect(url_for('index'))

    logging.debug(f"Fetching schedule for ID: {schedule_id}")
    form = ScheduleForm()
    table = get_schedule_table()

    try:
        # スケジュールの取得
        schedules = table.query(
            KeyConditionExpression='schedule_id = :sid',
            ExpressionAttributeValues={':sid': schedule_id}
        ).get('Items', [])
        
        if not schedules:
            flash('スケジュールが見つかりません', 'error')
            return redirect(url_for('index'))
        
        schedule = schedules[0]

        # --- 2. チーム管理者の場合、自分のチームの予定かチェック ---
        # システム管理者（渋谷さん）はスルー、チーム管理者はID一致を必須に
        if not current_user.administrator:
            if schedule.get('team_id') != current_user.team_id:
                flash('他チームの予定を編集することはできません', 'danger')
                return redirect(url_for('schedule.admin_schedules'))

        if request.method == 'GET':
            # 日付の初期値をセット
            form.date.data = datetime.strptime(schedule['date'], '%Y-%m-%d').date()
            
            # --- ここから追記：会場や時間をセットして「空欄」を防ぐ ---
            form.venue.data = schedule.get('venue', '')           # 会場
            form.court.data = schedule.get('court', '')           # コート
            form.start_time.data = schedule.get('start_time', '') # 開始時間
            form.end_time.data = schedule.get('end_time', '')     # 終了時間
            form.max_participants.data = int(schedule.get('max_participants', 10))
            form.day_of_week.data = schedule.get('day_of_week', '')
            form.title.data = schedule.get('title', '')
            form.detail.data = schedule.get('detail', '')
            form.status.data = schedule.get('status', 'active')
            form.is_pinned.data = schedule.get('is_pinned', False)

            # 備考（もしフォームに項目があれば）
            if hasattr(form, 'comment'):
                form.comment.data = schedule.get('comment', '')
            
        elif request.method == 'POST':
            if form.validate_on_submit():
                try:
                    # （中略：バリデーションと更新処理...）

                    new_date = form.date.data.strftime('%Y-%m-%d')
                    old_date = schedule['date']

                    adjusted_max_raw = request.form.get('adjusted_max', '').strip()
                    adjusted_max = int(adjusted_max_raw) if adjusted_max_raw else None

                    new_item = {
                        'schedule_id': schedule_id,
                        'date': new_date,
                        'title': request.form.get('title', ''),
                        'detail': request.form.get('detail', ''),
                        'day_of_week': form.day_of_week.data,
                        'venue': form.venue.data,
                        'start_time': form.start_time.data,
                        'end_time': form.end_time.data,
                        'max_participants': form.max_participants.data,
                        'status': form.status.data,
                        'comment': request.form.get('comment', ''),
                        'is_pinned': form.is_pinned.data,
                        'updated_at': datetime.now().isoformat(),
                        'updated_by': current_user.display_name,
                        'team_id': schedule.get('team_id', ''),
                        'created_at': schedule.get('created_at', ''),
                    }
                    if adjusted_max is not None:
                        new_item['adjusted_max'] = adjusted_max

                    if new_date != old_date:
                        # 日付が変わった場合：古いアイテム削除 → 新規作成
                        table.delete_item(Key={'schedule_id': schedule_id, 'date': old_date})
                        table.put_item(Item=new_item)
                    else:
                        # 日付が変わらない場合：通常のupdate
                        update_expr = (
                            "SET #title=:t, #detail=:d, day_of_week=:dow, venue=:v, start_time=:st, "
                            "end_time=:et, max_participants=:mp, "
                            "updated_at=:ua, #status=:s, #comment=:c, updated_by=:an, "
                            "is_pinned=:pin"
                        )
                        expr_values = {
                            ':t': request.form.get('title', ''),
                            ':d': request.form.get('detail', ''),
                            ':dow': form.day_of_week.data,
                            ':v': form.venue.data,
                            ':st': form.start_time.data,
                            ':et': form.end_time.data,
                            ':mp': form.max_participants.data,
                            ':ua': datetime.now().isoformat(),
                            ':s': form.status.data,
                            ':c': request.form.get('comment', ''),
                            ':an': current_user.display_name,
                            ':pin': form.is_pinned.data,
                        }
                        if adjusted_max is not None:
                            update_expr += ", adjusted_max=:am"
                            expr_values[':am'] = adjusted_max
                        else:
                            update_expr += " REMOVE adjusted_max"

                        table.update_item(
                            Key={'schedule_id': schedule_id, 'date': old_date},
                            UpdateExpression=update_expr,
                            ExpressionAttributeNames={
                                '#title': 'title',
                                '#detail': 'detail',
                                '#status': 'status',
                                '#comment': 'comment'
                            },
                            ExpressionAttributeValues=expr_values,
                        )
                    
                    flash('スケジュールを更新しました', 'success')
                    # 編集後は一覧画面（admin_schedules）に戻るのが親切です
                    return redirect(url_for('schedule.admin_schedules'))
                    
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
    
    if not current_user.administrator and current_user.role != 'admin':
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
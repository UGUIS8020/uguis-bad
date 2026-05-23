from flask import Blueprint, render_template_string, redirect, request, flash, jsonify
from flask_login import current_user, login_required
import os
import requests

admin_oauth = Blueprint('admin_oauth', __name__)

SITE_URL = 'https://uguis-bad.shibuya8020.com'
ENV_PATH = '/var/www/uguis_bad/.env'

def _is_admin():
    return current_user.is_authenticated and getattr(current_user, 'administrator', False)

def _update_env(key, value):
    with open(ENV_PATH, 'r') as f:
        lines = f.readlines()
    new_lines = []
    updated = False
    for line in lines:
        if line.startswith(f'{key}='):
            new_lines.append(f'{key}={value}\n')
            updated = True
        else:
            new_lines.append(line)
    if not updated:
        new_lines.append(f'{key}={value}\n')
    with open(ENV_PATH, 'w') as f:
        f.writelines(new_lines)

TEMPLATE = """
<!doctype html>
<html>
<head><meta charset="utf-8"><title>Token管理</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5/dist/css/bootstrap.min.css">
</head>
<body class="p-4">
<h2>SNSトークン管理</h2>
{% with messages = get_flashed_messages(with_categories=true) %}
{% for cat, msg in messages %}
<div class="alert alert-{{ cat }}">{{ msg }}</div>
{% endfor %}
{% endwith %}

<div class="card mb-4">
<div class="card-header bg-dark text-white">Threadsトークン再取得</div>
<div class="card-body">
  <p><strong>Step 1:</strong> 下のボタンをクリックしてThreadsを認証（リダイレクトURIがMeta Developer Consoleに登録済みの場合）</p>
  <a href="{{ threads_auth_url }}" class="btn btn-dark mb-3">Threadsで認証する</a>
  <hr>
  <p><strong>Step 2:</strong> または <a href="https://developers.facebook.com/tools/explorer/" target="_blank">Meta Graph API Explorer</a>
  で短期トークンを取得して貼り付け（権限: threads_basic, threads_content_publish）</p>
  <form method="POST" action="/admin/oauth/threads/exchange">
    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
    <div class="mb-2">
      <input name="short_token" class="form-control" placeholder="短期Threadsトークンを貼り付け" required>
    </div>
    <button class="btn btn-secondary">長期トークンに変換して保存</button>
  </form>
</div>
</div>

<div class="card mb-4">
<div class="card-header text-white" style="background:#6f42c1">Instagramトークン再取得</div>
<div class="card-body">
  <p><a href="https://developers.facebook.com/tools/explorer/" target="_blank">Meta Graph API Explorer</a>
  で短期トークンを取得して貼り付けてください（権限: instagram_basic, instagram_content_publish）</p>
  <form method="POST" action="/admin/oauth/instagram/exchange">
    <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
    <div class="mb-2">
      <input name="short_token" class="form-control" placeholder="短期Instagramトークンを貼り付け" required>
    </div>
    <div class="mb-2">
      <input name="app_id" class="form-control" placeholder="App ID" value="{{ threads_app_id }}">
    </div>
    <div class="mb-2">
      <input name="app_secret" class="form-control" placeholder="App Secret" type="password">
    </div>
    <button class="btn btn-primary">長期トークンに変換して保存</button>
  </form>
</div>
</div>

<div class="card">
<div class="card-header">現在のトークン状態</div>
<div class="card-body">
<table class="table table-sm">
<tr><th>Threads</th><td id="th-status">確認中...</td></tr>
<tr><th>Instagram</th><td id="ig-status">確認中...</td></tr>
</table>
</div>
</div>

<script>
fetch('/admin/oauth/token_status').then(r=>r.json()).then(d=>{
  document.getElementById('th-status').innerHTML = d.threads
    ? '<span class="text-success">有効</span>'
    : '<span class="text-danger">無効 - 上から再取得してください</span>';
  document.getElementById('ig-status').innerHTML = d.instagram
    ? '<span class="text-success">有効</span>'
    : '<span class="text-danger">無効 - 上から再取得してください</span>';
});
</script>
</body>
</html>
"""


@admin_oauth.route('/admin/oauth')
@login_required
def oauth_index():
    if not _is_admin():
        return '管理者のみアクセス可', 403
    app_id = os.getenv('THREADS_APP_ID', '')
    redirect_uri = f'{SITE_URL}/admin/oauth/threads/callback'
    threads_auth_url = (
        f'https://threads.net/oauth/authorize'
        f'?client_id={app_id}'
        f'&redirect_uri={redirect_uri}'
        f'&scope=threads_basic,threads_content_publish'
        f'&response_type=code'
    )
    from flask_wtf.csrf import generate_csrf
    return render_template_string(TEMPLATE,
        threads_auth_url=threads_auth_url,
        threads_app_id=app_id,
        csrf_token=generate_csrf)


@admin_oauth.route('/admin/oauth/threads/callback')
@login_required
def threads_callback():
    if not _is_admin():
        return '管理者のみ', 403
    code = request.args.get('code')
    if not code:
        flash(f'エラー: code パラメータがありません', 'danger')
        return redirect('/admin/oauth')
    app_id = os.getenv('THREADS_APP_ID', '')
    app_secret = os.getenv('THREADS_APP_SECRET', '')
    redirect_uri = f'{SITE_URL}/admin/oauth/threads/callback'
    r = requests.post('https://graph.threads.net/oauth/access_token', data={
        'client_id': app_id,
        'client_secret': app_secret,
        'grant_type': 'authorization_code',
        'redirect_uri': redirect_uri,
        'code': code,
    })
    if not r.ok:
        flash(f'短期トークン取得失敗: {r.text}', 'danger')
        return redirect('/admin/oauth')
    short_token = r.json().get('access_token')
    r2 = requests.get('https://graph.threads.net/access_token', params={
        'grant_type': 'th_exchange_token',
        'client_secret': app_secret,
        'access_token': short_token,
    })
    if not r2.ok:
        flash(f'長期トークン変換失敗: {r2.text}', 'danger')
        return redirect('/admin/oauth')
    data2 = r2.json()
    long_token = data2.get('access_token')
    expires_in = data2.get('expires_in', 0)
    _update_env('THREADS_ACCESS_TOKEN', long_token)
    flash(f'Threadsトークンを更新しました（有効期限: 約{expires_in//86400}日）', 'success')
    return redirect('/admin/oauth')


@admin_oauth.route('/admin/oauth/threads/exchange', methods=['POST'])
@login_required
def threads_exchange():
    if not _is_admin():
        return '管理者のみ', 403
    short_token = (request.form.get('short_token') or '').strip()
    if not short_token:
        flash('トークンを入力してください', 'warning')
        return redirect('/admin/oauth')
    app_secret = os.getenv('THREADS_APP_SECRET', '')
    r = requests.get('https://graph.threads.net/access_token', params={
        'grant_type': 'th_exchange_token',
        'client_secret': app_secret,
        'access_token': short_token,
    })
    if not r.ok:
        flash(f'長期トークン変換失敗: {r.text}', 'danger')
        return redirect('/admin/oauth')
    data = r.json()
    long_token = data.get('access_token')
    expires_in = data.get('expires_in', 0)
    _update_env('THREADS_ACCESS_TOKEN', long_token)
    flash(f'Threadsトークンを更新しました（有効期限: 約{expires_in//86400}日）', 'success')
    return redirect('/admin/oauth')


@admin_oauth.route('/admin/oauth/instagram/exchange', methods=['POST'])
@login_required
def instagram_exchange():
    if not _is_admin():
        return '管理者のみ', 403
    short_token = (request.form.get('short_token') or '').strip()
    app_id = (request.form.get('app_id') or os.getenv('THREADS_APP_ID', '')).strip()
    app_secret = (request.form.get('app_secret') or os.getenv('THREADS_APP_SECRET', '')).strip()
    if not short_token:
        flash('トークンを入力してください', 'warning')
        return redirect('/admin/oauth')
    r = requests.get('https://graph.instagram.com/access_token', params={
        'grant_type': 'ig_exchange_token',
        'client_id': app_id,
        'client_secret': app_secret,
        'access_token': short_token,
    })
    if not r.ok:
        flash(f'Instagram長期トークン変換失敗: {r.text}', 'danger')
        return redirect('/admin/oauth')
    data = r.json()
    long_token = data.get('access_token')
    expires_in = data.get('expires_in', 0)
    _update_env('INSTAGRAM_ACCESS_TOKEN', long_token)
    flash(f'Instagramトークンを更新しました（有効期限: 約{expires_in//86400}日）', 'success')
    return redirect('/admin/oauth')


@admin_oauth.route('/admin/oauth/token_status')
@login_required
def token_status():
    threads_ok = False
    ig_ok = False
    try:
        t = os.getenv('THREADS_ACCESS_TOKEN', '')
        r = requests.get('https://graph.threads.net/v1.0/me',
                         params={'fields': 'id', 'access_token': t}, timeout=5)
        threads_ok = r.ok
    except Exception:
        pass
    try:
        t = os.getenv('INSTAGRAM_ACCESS_TOKEN', '')
        r = requests.get('https://graph.instagram.com/v21.0/me',
                         params={'fields': 'id', 'access_token': t}, timeout=5)
        ig_ok = r.ok
    except Exception:
        pass
    return jsonify({'threads': threads_ok, 'instagram': ig_ok})

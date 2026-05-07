#!/bin/bash
# cronを全て書き直して空行と絶対パス問題を解消する
crontab - << 'EOF'
15 3 * * 1 /bin/bash -lc 'cd /var/www/uguis_bad && /var/www/uguis_bad/venv/bin/python -m uguu.reconcile_practice_count >> /var/www/uguis_bad/reconcile_practice_count.log 2>&1'
0 7 * * * /bin/bash -lc 'cd /var/www/uguis_bad && /var/www/uguis_bad/venv/bin/python post_schedule_x.py --mode today >> /var/www/uguis_bad/post_schedule_x.log 2>&1'
0 9 * * * /bin/bash -lc 'cd /var/www/uguis_bad && /var/www/uguis_bad/venv/bin/python post_schedule_x.py --mode 3days >> /var/www/uguis_bad/post_schedule_x.log 2>&1'
0 3 1 * * /bin/bash -lc 'cd /var/www/uguis_bad && /var/www/uguis_bad/venv/bin/python refresh_threads_token.py >> /var/www/uguis_bad/refresh_threads_token.log 2>&1'
30 3 1 * * /bin/bash -lc 'cd /var/www/uguis_bad && /var/www/uguis_bad/venv/bin/python refresh_instagram_token.py >> /var/www/uguis_bad/refresh_instagram_token.log 2>&1'
0 8 * * 1 /bin/bash -lc '/var/www/uguis_bad/venv/bin/python /var/www/uguis_bad/post_badnews.py --mode post >> /var/www/uguis_bad/post_badnews.log 2>&1'
EOF
echo "crontab更新完了"
crontab -l

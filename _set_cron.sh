#!/bin/bash
LINE="0 8 * * 1 /bin/bash -lc 'cd /var/www/uguis_bad && /var/www/uguis_bad/venv/bin/python post_badnews.py --mode post >> /var/www/uguis_bad/post_badnews.log 2>&1'"
(crontab -l 2>/dev/null | grep -v "post_badnews"; echo "$LINE") | crontab -
echo "done"
crontab -l

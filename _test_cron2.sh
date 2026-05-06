#!/bin/bash
rm -f /tmp/cron_test.log
MINUTE=$(( $(date +%M) + 2 ))
HOUR=$(date +%H)
if [ $MINUTE -ge 60 ]; then
    MINUTE=$(( MINUTE - 60 ))
    HOUR=$(( HOUR + 1 ))
fi
# MAILTO=""を先頭に入れてメール送信エラーを防ぐ
(echo 'MAILTO=""'; crontab -l | grep -v 'MAILTO\|cron_test'; echo "$MINUTE $HOUR * * * echo 'cron_test_ok' >> /tmp/cron_test.log 2>&1") | crontab -
echo "現在時刻: $(date)"
echo "テストcron: $MINUTE $HOUR * * *"
echo "2分後に確認します"

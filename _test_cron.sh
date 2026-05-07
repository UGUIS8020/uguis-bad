#!/bin/bash
MINUTE=$(( $(date +%M) + 2 ))
HOUR=$(date +%H)
if [ $MINUTE -ge 60 ]; then
    MINUTE=$(( MINUTE - 60 ))
    HOUR=$(( HOUR + 1 ))
fi
TEST_LINE="$MINUTE $HOUR * * * echo 'cron_test_ok' >> /tmp/cron_test.log 2>&1"
echo "現在時刻: $(date)"
echo "テストcron: $TEST_LINE"
(crontab -l; echo "$TEST_LINE") | crontab -
echo "テストcronを設定しました。2分後に /tmp/cron_test.log を確認してください"

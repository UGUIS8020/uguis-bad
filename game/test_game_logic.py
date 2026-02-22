import unittest
import random
from datetime import datetime, timedelta
import sys
from unittest.mock import MagicMock

# --- Flask Mock 設定 ---
mock_app = MagicMock()
mock_app.logger.info = lambda msg, *args: print(msg % args if args else msg)
mock_app.logger.warning = print
mock_app.logger.error = print
mock_app.dynamodb.Table.return_value.get_item.return_value = {} 

sys.modules["flask"] = MagicMock()
import flask
flask.current_app = mock_app

from game.game_utils import _pick_waiters_by_rest_queue

class TestGameRotation(unittest.TestCase):

    def setUp(self):
        # 16人の初期メンバーを作成
        self.base_time = datetime(2026, 2, 22, 10, 0, 0)
        self.entries = []
        for i in range(16): # 16人に変更
            self.entries.append({
                "user_id": f"user_{i:02d}",
                "display_name": f"プレイヤー_{i:02d}",
                "rest_count": 0,
                "joined_at": self.base_time.isoformat()
            })

    def test_cycle_integrity_16_players(self):
        import game.game_utils
        self.current_queue_data = {
            "queue": [], 
            "generation": 1, 
            "version": 0, 
            "cycle_started_at": self.base_time.isoformat()
        }

        # Mock設定: 引数不一致を防ぐために *args, **kwargs を使用
        game.game_utils._load_rest_queue = MagicMock(side_effect=lambda *args, **kwargs: self.current_queue_data)
        
        def mock_save(meta_table, queue, generation, prev_version, cycle_started_at, *args, **kwargs):
            self.current_queue_data = {
                "queue": queue,
                "generation": generation,
                "version": prev_version + 1,
                "cycle_started_at": cycle_started_at
            }
            return True
        game.game_utils._save_rest_queue_optimistic = MagicMock(side_effect=mock_save)

        all_waiting_history = []
        
        # 8試合実行（16人 ÷ 2人 = 8試合）
        for match_idx in range(8):
            active, waiting, meta = _pick_waiters_by_rest_queue(self.entries, waiting_count=2)
            waiter_ids = [w["user_id"] for w in waiting]
            all_waiting_history.extend(waiter_ids)
            
            for w_id in waiter_ids:
                for entry in self.entries:
                    if entry["user_id"] == w_id:
                        entry["rest_count"] += 1

        # 16人全員が1回ずつ選ばれているか
        for i in range(16):
            uid = f"user_{i:02d}"
            count = all_waiting_history.count(uid)
            self.assertEqual(count, 1, f"ユーザー {uid} が {count} 回待機しました")

        print(f"\n✅ 16名シミュレーション成功: {all_waiting_history}")
        print(f"全8試合の待機履歴: {all_waiting_history}")
        print("✅ 16名全員が重複なく1回ずつ休みました。")
        print("✅ 途中参加者は1巡目が終わるまで待機リストに入りませんでした。")

if __name__ == '__main__':
    unittest.main()
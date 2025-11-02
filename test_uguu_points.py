# test_uguu_points.py

from datetime import datetime, timedelta
from collections import defaultdict

class UguuPointsCalculator:
    """テスト用のクラス"""
    
    def __init__(self, mock_data=None):
        self.mock_data = mock_data or {}
    
    def get_user_participation_history_with_timestamp(self, user_id):
        """モックデータを返す"""
        return self.mock_data.get(user_id, [])
    
    # def calculate_uguu_points(self, user_id):
    #     """
    #     うぐポイントを計算
    #     - 参加ポイント: 前々日の23:59までに参加ボタンを押した場合100ポイント
    #     - 連続ポイント: 60日以内の連続参加で100ポイント
    #     - 月間ボーナス: 5/10/15/20回参加ごとにボーナス
    #     """
    #     try:
    #         from collections import defaultdict
    #         from datetime import datetime, timedelta
            
    #         print(f"\n[DEBUG] うぐポイント計算開始 - user_id: {user_id}")
    #         print("=" * 80)
            
    #         # 参加履歴を取得
    #         participation_history = self.get_user_participation_history_with_timestamp(user_id)
            
    #         if not participation_history:
    #             print(f"[DEBUG] 参加履歴なし - user_id: {user_id}")
    #             return {
    #                 'uguu_points': 0,
    #                 'participation_points': 0,
    #                 'streak_points': 0,
    #                 'monthly_bonus_points': 0,
    #                 'total_participation': 0,
    #                 'last_participation_date': None,
    #                 'current_streak_start': None,
    #                 'current_streak_count': 0,
    #                 'monthly_bonuses': {},
    #                 'early_registration_count': 0
    #             }
            
    #         # 参加履歴を処理
    #         participation_records = []
    #         for record in participation_history:
    #             try:
    #                 event_date = datetime.strptime(record['event_date'], '%Y-%m-%d')
    #                 registered_at = datetime.strptime(record['registered_at'], '%Y-%m-%d %H:%M:%S')
                    
    #                 participation_records.append({
    #                     'event_date': event_date,
    #                     'registered_at': registered_at
    #                 })
    #             except (ValueError, KeyError) as e:
    #                 print(f"[WARN] 不正なレコード形式: {record}, エラー: {e}")
            
    #         # イベント日付順にソート
    #         participation_records.sort(key=lambda x: x['event_date'])
            
    #         # 総参加回数
    #         total_participation = len(participation_records)
            
    #         # ポイント内訳の初期化
    #         participation_points = 0
    #         streak_points = 0
    #         early_registration_count = 0
            
    #         # 連続参加カウンター
    #         current_streak_start = participation_records[0]['event_date']
    #         current_streak_count = 1
            
    #         # 最初の参加の処理
    #         first_record = participation_records[0]
    #         if self._is_early_registration(first_record):
    #             participation_points += 100
    #             early_registration_count += 1
    #             print(f"[DEBUG] 初回参加（事前登録） - 参加ポイント: +100")
    #         else:
    #             print(f"[DEBUG] 初回参加（当日登録） - 参加ポイント: +0")
            
    #         # 2回目以降の参加を処理
    #         for i in range(1, len(participation_records)):
    #             previous_record = participation_records[i - 1]
    #             current_record = participation_records[i]
                
    #             # 前回の参加日からの日数差を計算
    #             days_diff = (current_record['event_date'] - previous_record['event_date']).days
                
    #             print(f"\n[DEBUG] === 参加 {i+1}回目 ===")
    #             print(f"[DEBUG] 連続チェック - 前回: {previous_record['event_date'].strftime('%Y-%m-%d')}, "
    #                   f"今回: {current_record['event_date'].strftime('%Y-%m-%d')}, 日数差: {days_diff}日")
                
    #             # 参加ポイントの判定
    #             if self._is_early_registration(current_record):
    #                 participation_points += 100
    #                 early_registration_count += 1
    #                 print(f"[DEBUG] 事前登録 - 参加ポイント: +100")
    #             else:
    #                 print(f"[DEBUG] 当日登録 - 参加ポイント: +0")
                
    #             # 連続ポイントの判定
    #             if days_diff <= 60:
    #                 current_streak_count += 1
    #                 streak_points += 100
    #                 print(f"[DEBUG] 連続参加 - カウント: {current_streak_count}, 連続ポイント: +100")
    #             else:
    #                 print(f"[DEBUG] 連続リセット - 60日超過: {days_diff}日, 連続ポイント: +0")
    #                 current_streak_count = 1
    #                 current_streak_start = current_record['event_date']
            
    #         # 月間参加ボーナス計算
    #         monthly_participation = defaultdict(int)
    #         monthly_bonuses = {}
    #         monthly_bonus_points = 0
            
    #         for record in participation_records:
    #             month_key = record['event_date'].strftime('%Y-%m')
    #             monthly_participation[month_key] += 1
            
    #         print(f"\n[DEBUG] === 月間ボーナス計算 ===")
    #         for month, count in sorted(monthly_participation.items()):
    #             print(f"[DEBUG] 月別参加回数 - {month}: {count}回")
            
    #         for month, count in monthly_participation.items():
    #             monthly_bonuses[month] = {
    #                 'participation_count': count,
    #                 'bonus_points': 0
    #             }
                
    #             if count >= 5:
    #                 monthly_bonuses[month]['bonus_points'] += 500
    #             if count >= 10:
    #                 monthly_bonuses[month]['bonus_points'] += 1000
    #             if count >= 15:
    #                 monthly_bonuses[month]['bonus_points'] += 1500
    #             if count >= 20:
    #                 monthly_bonuses[month]['bonus_points'] += 2000
                
    #             if monthly_bonuses[month]['bonus_points'] > 0:
    #                 print(f"[DEBUG] {month} - {count}回参加 → ボーナス: {monthly_bonuses[month]['bonus_points']}ポイント")
                
    #             monthly_bonus_points += monthly_bonuses[month]['bonus_points']
            
    #         # 総ポイント計算
    #         uguu_points = participation_points + streak_points + monthly_bonus_points
            
    #         result = {
    #             'uguu_points': uguu_points,
    #             'participation_points': participation_points,
    #             'streak_points': streak_points,
    #             'monthly_bonus_points': monthly_bonus_points,
    #             'total_participation': total_participation,
    #             'early_registration_count': early_registration_count,
    #             'last_participation_date': participation_records[-1]['event_date'].strftime('%Y-%m-%d') if participation_records else None,
    #             'current_streak_start': current_streak_start.strftime('%Y-%m-%d') if current_streak_start else None,
    #             'current_streak_count': current_streak_count,
    #             'monthly_bonuses': monthly_bonuses
    #         }
            
    #         print(f"\n[DEBUG] === 最終結果 ===")
    #         print(f"  参加ポイント: {participation_points}P (事前参加: {early_registration_count}/{total_participation}回)")
    #         print(f"  連続ポイント: {streak_points}P")
    #         print(f"  月間ボーナス: {monthly_bonus_points}P")
    #         print(f"  ━━━━━━━━━━━━━━━━━━")
    #         print(f"  合計: {uguu_points}P")
    #         print("=" * 80)
            
    #         return result
            
    #     except Exception as e:
    #         print(f"[ERROR] うぐポイント計算エラー: {str(e)}")
    #         import traceback
    #         traceback.print_exc()
    #         return {
    #             'uguu_points': 0,
    #             'participation_points': 0,
    #             'streak_points': 0,
    #             'monthly_bonus_points': 0,
    #             'total_participation': 0,
    #             'early_registration_count': 0,
    #             'last_participation_date': None,
    #             'current_streak_start': None,
    #             'current_streak_count': 0,
    #             'monthly_bonuses': {}
    #         }
    
    def _is_early_registration(self, record):
        """事前参加（前々日の23:59までに登録）かどうかを判定"""
        event_date = record['event_date']
        registered_at = record['registered_at']
        
        # 前々日の23:59:59を計算
        two_days_before = event_date - timedelta(days=2)
        deadline = two_days_before.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        is_early = registered_at <= deadline
        
        print(f"    事前参加判定 - イベント日: {event_date.strftime('%Y-%m-%d')}, "
              f"登録日時: {registered_at.strftime('%Y-%m-%d %H:%M:%S')}, "
              f"締切: {deadline.strftime('%Y-%m-%d %H:%M:%S')}, "
              f"→ {'✓ 事前参加' if is_early else '✗ 当日登録'}")
        
        return is_early


# ===== テストケース =====

def test_case_1():
    """テストケース1: 基本的な事前参加と連続参加"""
    print("\n" + "=" * 80)
    print("テストケース1: 基本的な事前参加と連続参加")
    print("=" * 80)
    
    mock_data = {
        'user_001': [
            {'event_date': '2025-01-10', 'registered_at': '2025-01-08 10:00:00'},  # 事前参加
            {'event_date': '2025-01-15', 'registered_at': '2025-01-13 15:00:00'},  # 事前参加 + 連続
            {'event_date': '2025-01-20', 'registered_at': '2025-01-18 20:00:00'},  # 事前参加 + 連続
        ]
    }
    
    calculator = UguuPointsCalculator(mock_data)
    result = calculator.calculate_uguu_points('user_001')
    
    print("\n期待される結果:")
    print("  参加ポイント: 300P (3回 × 100P)")
    print("  連続ポイント: 200P (2回目と3回目)")
    print("  月間ボーナス: 0P")
    print("  合計: 500P")
    
    assert result['participation_points'] == 300, f"参加ポイントが不正: {result['participation_points']}"
    assert result['streak_points'] == 200, f"連続ポイントが不正: {result['streak_points']}"
    assert result['uguu_points'] == 500, f"合計ポイントが不正: {result['uguu_points']}"
    print("\n✓ テスト1 合格！")


def test_case_2():
    """テストケース2: 当日登録（ポイントなし）"""
    print("\n" + "=" * 80)
    print("テストケース2: 当日登録（ポイントなし）")
    print("=" * 80)
    
    mock_data = {
        'user_002': [
            {'event_date': '2025-01-10', 'registered_at': '2025-01-10 09:00:00'},  # 当日登録
            {'event_date': '2025-01-15', 'registered_at': '2025-01-15 10:00:00'},  # 当日登録 + 連続
        ]
    }
    
    calculator = UguuPointsCalculator(mock_data)
    result = calculator.calculate_uguu_points('user_002')
    
    print("\n期待される結果:")
    print("  参加ポイント: 0P (全て当日登録)")
    print("  連続ポイント: 100P (2回目)")
    print("  月間ボーナス: 0P")
    print("  合計: 100P")
    
    assert result['participation_points'] == 0, f"参加ポイントが不正: {result['participation_points']}"
    assert result['streak_points'] == 100, f"連続ポイントが不正: {result['streak_points']}"
    assert result['uguu_points'] == 100, f"合計ポイントが不正: {result['uguu_points']}"
    print("\n✓ テスト2 合格！")


def test_case_3():
    """テストケース3: 連続リセット（60日超過）"""
    print("\n" + "=" * 80)
    print("テストケース3: 連続リセット（60日超過）")
    print("=" * 80)
    
    mock_data = {
        'user_003': [
            {'event_date': '2025-01-10', 'registered_at': '2025-01-08 10:00:00'},  # 事前参加
            {'event_date': '2025-03-15', 'registered_at': '2025-03-13 10:00:00'},  # 事前参加（64日後、リセット）
            {'event_date': '2025-03-20', 'registered_at': '2025-03-18 10:00:00'},  # 事前参加 + 連続
        ]
    }
    
    calculator = UguuPointsCalculator(mock_data)
    result = calculator.calculate_uguu_points('user_003')
    
    print("\n期待される結果:")
    print("  参加ポイント: 300P (3回 × 100P)")
    print("  連続ポイント: 100P (3回目のみ、2回目は連続リセット)")
    print("  月間ボーナス: 0P")
    print("  合計: 400P")
    
    assert result['participation_points'] == 300, f"参加ポイントが不正: {result['participation_points']}"
    assert result['streak_points'] == 100, f"連続ポイントが不正: {result['streak_points']}"
    assert result['uguu_points'] == 400, f"合計ポイントが不正: {result['uguu_points']}"
    print("\n✓ テスト3 合格！")


def test_case_4():
    """テストケース4: 月間ボーナス（5回達成）"""
    print("\n" + "=" * 80)
    print("テストケース4: 月間ボーナス（5回達成）")
    print("=" * 80)
    
    mock_data = {
        'user_004': [
            {'event_date': '2025-01-05', 'registered_at': '2025-01-03 10:00:00'},
            {'event_date': '2025-01-10', 'registered_at': '2025-01-08 10:00:00'},
            {'event_date': '2025-01-15', 'registered_at': '2025-01-13 10:00:00'},
            {'event_date': '2025-01-20', 'registered_at': '2025-01-18 10:00:00'},
            {'event_date': '2025-01-25', 'registered_at': '2025-01-23 10:00:00'},
        ]
    }
    
    calculator = UguuPointsCalculator(mock_data)
    result = calculator.calculate_uguu_points('user_004')
    
    print("\n期待される結果:")
    print("  参加ポイント: 500P (5回 × 100P)")
    print("  連続ポイント: 400P (2~5回目)")
    print("  月間ボーナス: 500P (5回達成)")
    print("  合計: 1400P")
    
    assert result['participation_points'] == 500, f"参加ポイントが不正: {result['participation_points']}"
    assert result['streak_points'] == 400, f"連続ポイントが不正: {result['streak_points']}"
    assert result['monthly_bonus_points'] == 500, f"月間ボーナスが不正: {result['monthly_bonus_points']}"
    assert result['uguu_points'] == 1400, f"合計ポイントが不正: {result['uguu_points']}"
    print("\n✓ テスト4 合格！")


def test_case_5():
    """テストケース5: 前日登録（23:59を1分過ぎた）"""
    print("\n" + "=" * 80)
    print("テストケース5: 境界値テスト - 23:59を過ぎた登録")
    print("=" * 80)
    
    mock_data = {
        'user_005': [
            {'event_date': '2025-01-10', 'registered_at': '2025-01-08 23:59:59'},  # ギリギリ事前参加
            {'event_date': '2025-01-15', 'registered_at': '2025-01-13 00:00:00'},  # 1秒過ぎて当日登録扱い
        ]
    }
    
    calculator = UguuPointsCalculator(mock_data)
    result = calculator.calculate_uguu_points('user_005')
    
    print("\n期待される結果:")
    print("  参加ポイント: 100P (1回目のみ)")
    print("  連続ポイント: 100P (2回目)")
    print("  合計: 200P")
    
    assert result['participation_points'] == 100, f"参加ポイントが不正: {result['participation_points']}"
    assert result['streak_points'] == 100, f"連続ポイントが不正: {result['streak_points']}"
    assert result['uguu_points'] == 200, f"合計ポイントが不正: {result['uguu_points']}"
    print("\n✓ テスト5 合格！")

  def test_case_6():
    """テストケース6: 7日前登録（200ポイント）"""
    print("\n" + "=" * 80)
    print("テストケース6: 7日前登録（超早期）")
    print("=" * 80)
    
    mock_data = {
        'user_006': [
            {'event_date': '2025-01-15', 'registered_at': '2025-01-08 10:00:00'},  # 7日前（200P）
            {'event_date': '2025-01-20', 'registered_at': '2025-01-18 10:00:00'},  # 前々日（100P）+ 連続
            {'event_date': '2025-01-25', 'registered_at': '2025-01-25 09:00:00'},  # 当日（0P）+ 連続
        ]
    }
    
    calculator = UguuPointsCalculator(mock_data)
    result = calculator.calculate_uguu_points('user_006')
    
    print("\n期待される結果:")
    print("  参加ポイント: 300P (200 + 100 + 0)")
    print("  連続ポイント: 200P (2回目と3回目)")
    print("  合計: 500P")
    
    assert result['participation_points'] == 300, f"参加ポイントが不正: {result['participation_points']}"
    assert result['streak_points'] == 200, f"連続ポイントが不正: {result['streak_points']}"
    assert result['super_early_registration_count'] == 1, f"7日前登録回数が不正: {result['super_early_registration_count']}"
    assert result['uguu_points'] == 500, f"合計ポイントが不正: {result['uguu_points']}"
    print("\n✓ テスト6 合格！")


def test_case_7():
    """テストケース7: 境界値テスト（7日前ちょうど）"""
    print("\n" + "=" * 80)
    print("テストケース7: 境界値テスト（7日前の23:59）")
    print("=" * 80)
    
    mock_data = {
        'user_007': [
            {'event_date': '2025-01-15', 'registered_at': '2025-01-08 23:59:59'},  # ギリギリ200P
            {'event_date': '2025-01-20', 'registered_at': '2025-01-13 00:00:00'},  # 1秒過ぎて100P扱い
        ]
    }
    
    calculator = UguuPointsCalculator(mock_data)
    result = calculator.calculate_uguu_points('user_007')
    
    print("\n期待される結果:")
    print("  参加ポイント: 300P (200 + 100)")
    print("  連続ポイント: 100P")
    print("  合計: 400P")
    
    assert result['participation_points'] == 300, f"参加ポイントが不正: {result['participation_points']}"
    assert result['streak_points'] == 100, f"連続ポイントが不正: {result['streak_points']}"
    assert result['uguu_points'] == 400, f"合計ポイントが不正: {result['uguu_points']}"
    print("\n✓ テスト7 合格！")


# ===== テスト実行 =====
if __name__ == '__main__':
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "うぐポイント計算システム テスト" + " " * 24 + "║")
    print("╚" + "=" * 78 + "╝")
    
    try:
        test_case_1()
        test_case_2()
        test_case_3()
        test_case_4()
        test_case_5()
        
        print("\n" + "=" * 80)
        print("✓✓✓ 全てのテストが合格しました！ ✓✓✓")
        print("=" * 80)
        
    except AssertionError as e:
        print(f"\n✗✗✗ テスト失敗: {e} ✗✗✗")
    except Exception as e:
        print(f"\n✗✗✗ エラー発生: {e} ✗✗✗")
        import traceback
        traceback.print_exc()
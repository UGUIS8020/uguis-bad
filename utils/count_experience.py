def count_experience_levels(participants_info):
    beginners = sum(1 for p in participants_info 
                   if p.get('badminton_experience') in ['未経験', '1年未満'])
    return beginners

def can_join_schedule(schedule, user):
    # 参加者の経験レベル情報を取得
    participants_info = schedule.get('participants_info', [])
    beginner_count = count_experience_levels(participants_info)
    
    # ユーザーの経験レベルを確認
    user_is_beginner = user.get('badminton_experience') in ['未経験', '1年未満']
    
    # 初心者枠のチェック
    if user_is_beginner and beginner_count >= 2:
        return False, "初心者枠が満員です"
        
    return True, None
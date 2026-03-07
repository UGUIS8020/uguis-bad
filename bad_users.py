import sys
sys.path.insert(0, '.')
from app import create_app
from uguu.dynamo import DynamoDB
import json
from decimal import Decimal

def json_default(o):
    if isinstance(o, Decimal):
        return int(o) if o % 1 == 0 else float(o)
    return str(o)

app = create_app()

with app.app_context():
    db = DynamoDB()

    items = []
    resp = db.users_table.scan()
    items.extend(resp.get('Items', []))

    while 'LastEvaluatedKey' in resp:
        resp = db.users_table.scan(ExclusiveStartKey=resp['LastEvaluatedKey'])
        items.extend(resp.get('Items', []))

    print(f"ユーザー総数: {len(items)}人")

    users = items

    print(f"=== ユーザー一覧 ({len(users)}人) ===")
    for user in users:
        print(f"\nuser_id       : {user.get('user#user_id')}")
        print(f"display_name  : {user.get('display_name', 'なし')}")
        print(f"user_name  : {user.get('display_name', 'なし')}")
        print(f"gender        : {user.get('gender', 'なし')}")
        
        print(f"date_of_birth : {user.get('date_of_birth', 'なし')}")        

    print("\n[DONE]")

    print("\n=== skill情報のみのユーザー ===")
    skill_only = [
        u for u in users
        if not u.get('display_name') and not u.get('email')
        and (u.get('skill_score') or u.get('skill_sigma'))
    ]
    print(f"該当件数: {len(skill_only)}人")
    for u in skill_only:
        print(f"  user_id: {u.get('user#user_id')}")
        print(f"  skill_score: {u.get('skill_score')}")
        print(f"  skill_sigma: {u.get('skill_sigma')}")
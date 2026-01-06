import os
from dotenv import load_dotenv  # ← 追加
load_dotenv()                   # ← .env を読み込む

import boto3

region = os.getenv('AWS_REGION', 'ap-northeast-1')
table_name = os.getenv('PART_HISTORY_TABLE', 'bad-users-history')  # 必要なら環境変数名を合わせる

# プロファイルを使うなら（任意）：AWS_PROFILE=dev などを .env に入れておく
profile = os.getenv('AWS_PROFILE')
if profile:
    session = boto3.Session(profile_name=profile, region_name=region)
    client = session.client('dynamodb')
else:
    client = boto3.client('dynamodb', region_name=region)

desc = client.describe_table(TableName=table_name)['Table']
print('Region:', region)
print('TableName:', table_name)
print('KeySchema:', desc['KeySchema'])
print('AttributeDefinitions:', desc['AttributeDefinitions'])
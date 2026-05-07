import boto3, os
from dotenv import load_dotenv
load_dotenv('/var/www/uguis_bad/.env')
table = boto3.resource('dynamodb', region_name='ap-northeast-1').Table('bad_schedules')
for d in ['2026-05-07','2026-05-08','2026-05-09','2026-05-10','2026-05-11']:
    r = table.scan(FilterExpression='#d = :d', ExpressionAttributeNames={'#d':'date'}, ExpressionAttributeValues={':d':d})
    for s in r['Items']:
        print(d, s.get('status'), s.get('venue',''), s.get('court',''), s.get('title',''))

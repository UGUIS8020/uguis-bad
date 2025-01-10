import boto3
import os

dynamodb = boto3.resource('dynamodb',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)
table_name = os.getenv("TABLE_NAME_USER")
table = dynamodb.Table(table_name)

response = table.get_item(Key={'user#user_id': '7e2dbd84-3495-478c-a5b3-b4aa1fe8845b'})
print(response.get('Item'))
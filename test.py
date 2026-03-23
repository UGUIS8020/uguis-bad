import boto3

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
table = dynamodb.Table("bad-users")

item = table.get_item(
    Key={"user#user_id": "4c7f822d-ff39-4797-9b7b-8ebc205490f5"}
).get("Item")
print("現在のskill_score:", item.get("skill_score"))
print("現在のskill_sigma:", item.get("skill_sigma"))
import boto3

dynamodb = boto3.resource("dynamodb", region_name="ap-northeast-1")
table = dynamodb.Table("bad-users")

to_delete = [
    "user#4c7f822d-ff39-4797-9b7b-8ebc205490f5",
    "user#e70ca207-e29b-4dc7-881f-e49654a9fa9f",
    "user#12e3ef52-f443-4d7e-9776-59ce912e68c3",
]

for pk in to_delete:
    table.delete_item(Key={"user#user_id": pk})
    print(f"削除: {pk}")

print("完了")
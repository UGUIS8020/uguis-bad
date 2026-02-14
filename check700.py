import os
import boto3

region = os.getenv("AWS_REGION", "ap-northeast-1")
d = boto3.client("dynamodb", region_name=region)

for name in ["bad-game-matches", "bad-game-match_entries"]:
    t = d.describe_table(TableName=name)["Table"]
    print("\n===", name, "===")
    print("KeySchema:", t["KeySchema"])
    print("AttributeDefinitions:", t["AttributeDefinitions"])
# scripts/check_match_table.py
import os
import boto3

REGION = os.getenv("AWS_REGION", "ap-northeast-1")
TABLE  = os.getenv("DYNAMO_MATCH_TABLE", "bad-game-match_entries")

def main():
    print(f"AWS_REGION = {REGION}")
    print(f"TABLE      = {TABLE}")

    # どのAWSアカウントの資格情報で叩いているか
    sts = boto3.client("sts", region_name=REGION)
    ident = sts.get_caller_identity()
    print(f"CallerIdentity: {ident['Arn']} (Account={ident['Account']})")

    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    t = dynamodb.Table(TABLE)

    # テーブル定義 & item数（概算）
    client = boto3.client("dynamodb", region_name=REGION)
    desc = client.describe_table(TableName=TABLE)["Table"]
    print(f"TableStatus = {desc.get('TableStatus')}")
    print(f"ItemCount   = {desc.get('ItemCount')} (eventually consistent)")
    print("KeySchema   =", desc.get("KeySchema"))
    print("GSIs        =", [g.get("IndexName") for g in desc.get("GlobalSecondaryIndexes", [])] if desc.get("GlobalSecondaryIndexes") else [])

    # 実データを少しだけscan（最大10件）
    resp = t.scan(Limit=10)
    items = resp.get("Items", [])
    print(f"Scan Count  = {resp.get('Count')}  ScannedCount = {resp.get('ScannedCount')}")
    print("Sample Items:")
    for i, it in enumerate(items, 1):
        print(f"--- {i} ---")
        print(it)

    if not items:
        print("=> この環境のこのテーブルは、少なくとも先頭10件は空です（Count=0の可能性大）")

if __name__ == "__main__":
    main()

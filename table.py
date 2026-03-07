import boto3
from botocore.exceptions import ClientError

AWS_REGION = "ap-northeast-1"
TABLE_NAME = "ugu_points_v2"

dynamodb = boto3.client("dynamodb", region_name=AWS_REGION)


def create_table():
    try:
        response = dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {
                    "AttributeName": "user_id",
                    "KeyType": "HASH"   # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "user_id",
                    "AttributeType": "S"
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        print(f"[OK] creating table: {TABLE_NAME}")
        print(response)

        waiter = dynamodb.get_waiter("table_exists")
        waiter.wait(TableName=TABLE_NAME)
        print(f"[DONE] table is active: {TABLE_NAME}")

    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "ResourceInUseException":
            print(f"[INFO] table already exists: {TABLE_NAME}")
        else:
            raise


if __name__ == "__main__":
    create_table()
import boto3
from botocore.exceptions import ClientError

def send_text_email(to_address, subject, body):
    client = boto3.client('ses', region_name='ap-northeast-1')

    # .strip() を付けて前後の空白を確実に消去します
    SENDER = "info@shibuya8020.com".strip()
    recipient = to_address.strip()

    try:
        response = client.send_email(
            Destination={
                'ToAddresses': [recipient],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': "UTF-8",
                        'Data': body,
                    },
                },
                'Subject': {
                    'Charset': "UTF-8",
                    'Data': subject,
                },
            },
            Source=SENDER,
        )
        return response['MessageId']
    except ClientError as e:
        print(f"エラーが発生しました: {e.response['Error']['Message']}")
        return None

if __name__ == "__main__":
    # ここに自分のメールアドレスを入れる際、クォーテーション内にスペースがないか注意してください
    target_email = "shibuya8020@icloud.com" 
    
    result = send_text_email(target_email, "テスト", "正常に送信されました。")
    if result:
        print(f"送信成功！ ID: {result}")
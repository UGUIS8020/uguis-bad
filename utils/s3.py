import boto3
from werkzeug.utils import secure_filename
import uuid
import os
from PIL import Image
from io import BytesIO

def resize_image(image_file, max_size=(800, 800)):
    """画像を指定されたサイズに縮小"""
    try:
        # PILイメージとして開く
        img = Image.open(image_file)
        
        # EXIF情報に基づいて画像を回転
        if hasattr(img, '_getexif') and img._getexif() is not None:
            orientation = dict(img._getexif().items()).get(274)
            if orientation:
                rotate_values = {3: 180, 6: 270, 8: 90}
                if orientation in rotate_values:
                    img = img.rotate(rotate_values[orientation], expand=True)

        # アスペクト比を保持しながらリサイズ
        img.thumbnail(max_size, Image.Resampling.LANCZOS)
        
        # BytesIOオブジェクトに保存
        output = BytesIO()
        
        # 元の形式を保持
        format = image_file.content_type.split('/')[-1].upper()
        if format == 'JPG':
            format = 'JPEG'
        
        # 画質を指定して保存（JPEG形式の場合）
        if format == 'JPEG':
            img.save(output, format=format, quality=85, optimize=True)
        else:
            img.save(output, format=format)
        
        output.seek(0)
        return output
        
    except Exception as e:
        print(f"Error resizing image: {e}")
        return None

def upload_image_to_s3(file):
    if not file:
        return None
        
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )
    bucket_name = os.getenv("S3_BUCKET")  # 環境変数から取得
    file_name = f"posts/{uuid.uuid4()}-{secure_filename(file.filename)}"
    
    try:
        # 画像を縮小
        resized_image = resize_image(file)
        if not resized_image:
            return None
            
        # S3にアップロード
        s3.upload_fileobj(
            resized_image,
            bucket_name,
            file_name,
            ExtraArgs={'ContentType': file.content_type}
        )
        return f"https://{bucket_name}.s3.amazonaws.com/{file_name}"
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return None
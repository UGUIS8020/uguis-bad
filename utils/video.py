import os
import subprocess
import tempfile

# 静的ビルドのffmpeg。本番は /usr/local/bin/ffmpeg に配置済み（システムのパッケージ管理は触っていない）。
# ローカル開発機など未導入の環境では圧縮をスキップし、元ファイルをそのまま使う。
FFMPEG_PATH = os.getenv('FFMPEG_PATH', '/usr/local/bin/ffmpeg')


def is_ffmpeg_available() -> bool:
    return bool(FFMPEG_PATH) and os.path.exists(FFMPEG_PATH)


def compress_video(input_path: str) -> str | None:
    """
    サーバー側で動画を圧縮する。
    ブラウザ側では -c copy（無劣化の単純カット）のみ済ませてある前提で、
    ここで初めて 720p への縮小・デインターレース・圧縮を行う。

    戻り値: 圧縮後ファイルのパス（失敗時はNone。呼び出し側は元ファイルへのフォールバックを想定）
    """
    if not is_ffmpeg_available():
        return None

    fd, output_path = tempfile.mkstemp(suffix='.mp4')
    os.close(fd)

    cmd = [
        FFMPEG_PATH, '-y',
        '-i', input_path,
        '-vf', "yadif,scale='if(gt(iw,ih),min(iw,1280),-2)':'if(gt(iw,ih),-2,min(ih,1280))'",
        '-c:v', 'libx264',
        '-preset', 'fast',
        '-crf', '24',
        '-c:a', 'aac',
        '-b:a', '128k',
        '-movflags', '+faststart',
        output_path,
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            timeout=300,  # 720p・最大60秒の動画を想定。medium プリセットでも余裕を持たせる
        )
        if result.returncode != 0:
            print(f"[compress_video] ffmpeg failed: {result.stderr.decode(errors='replace')[-2000:]}")
            _cleanup(output_path)
            return None
        if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            _cleanup(output_path)
            return None
        return output_path
    except subprocess.TimeoutExpired:
        print("[compress_video] ffmpeg timed out")
        _cleanup(output_path)
        return None
    except Exception as e:
        print(f"[compress_video] error: {e}")
        _cleanup(output_path)
        return None


def _cleanup(path: str):
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception:
        pass

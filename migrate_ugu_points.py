from datetime import datetime, timezone
from uuid import uuid4
from decimal import Decimal
import os, argparse
from datetime import datetime, timezone
import boto3
from decimal import Decimal

# 追加
from dotenv import load_dotenv
load_dotenv()  # .env を読み込む

AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
UGU_POINTS_TABLE = os.getenv("DYNAMO_UGU_POINTS_TABLE", "ugu_points")
HISTORY_TABLE = os.getenv("DYNAMO_BAD_USERS_HISTORY", "bad-users-history")

# ★ 明示的にセッションを作成（どれかが設定されていればOK）
session = boto3.Session(
    profile_name=os.getenv("AWS_PROFILE") or None,              # CLI プロファイルを使う場合
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),           # 環境変数（固定キー or 一時キー）
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),           # 一時クレデンシャルなら必要
    region_name=AWS_REGION,
)

ddb = session.resource("dynamodb")
src = ddb.Table(UGU_POINTS_TABLE)
dst = ddb.Table(HISTORY_TABLE)

def _iso_to_dt(s: str | None):
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s[:len(fmt)], fmt)
                break
            except Exception:
                dt = None
        if dt is None:
            return None
    # tz 無しは UTC を付与、tz ありは UTC に正規化
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _to_int(x):
    if x is None: return 0
    if isinstance(x, (int, float, Decimal)): return int(x)
    try: return int(str(x))
    except: return 0

def _scan_all(tbl):
    items, kw = [], {}
    while True:
        resp = tbl.scan(**kw)
        items += resp.get("Items", [])
        lek = resp.get("LastEvaluatedKey")
        if not lek: break
        kw["ExclusiveStartKey"] = lek
    return items

def main(apply: bool, include_future: bool, limit: int | None):
    # セッションを再利用（上の session を使う）
    ddb = session.resource("dynamodb")
    src = ddb.Table(UGU_POINTS_TABLE)
    dst = ddb.Table(HISTORY_TABLE)

    rows = _scan_all(src)
    if limit is not None:
        rows = rows[:limit]

    print(f"[INFO] scanned={len(rows)} from {UGU_POINTS_TABLE}  ->  {HISTORY_TABLE}")
    now = datetime.now(timezone.utc)

    migrated = skipped_future = skipped_zero = failed = 0

    for it in rows:
        user_id   = it.get("user_id")
        point_id  = it.get("point_id") or f"POINT#{uuid4()}"
        points    = _to_int(it.get("points"))
        reason    = it.get("reason") or "管理人付与"
        created   = _iso_to_dt(it.get("created_at")) or now
        effective = _iso_to_dt(it.get("effective_at"))

        if points == 0:
            skipped_zero += 1
            continue

        # ← 両方 aware なので安全
        if effective and not include_future and effective > now:
            skipped_future += 1
            continue

        event_dt = effective or created
        event_date = event_dt.date().isoformat()

        # joined_at は「タイプ#時刻#txid」で一意＆時系列
        tx_now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        joined_at = f"points#earn#{tx_now}#{point_id}"

        item = {
            "user_id": user_id,
            "joined_at": joined_at,
            "tx_id": point_id,
            "kind": "earn",                          # earn/spend/adjust
            "delta_points": int(points),             # 付与は正の値
            "event_date": event_date,
            "reason": reason,
            "created_at": tx_now,                    # 変換時刻
            "entity_type": "point_transaction",
            "version": 1,
        }

        if not apply:
            print(f"[DRYRUN] {user_id} {event_date} +{points}P tx={point_id}")
            migrated += 1
            continue

        try:
            dst.put_item(
                Item=item,
                ConditionExpression=(
                    "attribute_not_exists(user_id) AND attribute_not_exists(joined_at)"
                )
            )
            migrated += 1
        except Exception as e:
            failed += 1
            print(f"[ERROR] put_item failed user={user_id} tx={point_id}: {e}")

    print(f"[DONE] migrated={migrated} failed={failed} "
          f"skipped_future={skipped_future} skipped_zero={skipped_zero}")
    if not apply:
        print("[NOTE] DRY-RUN。--apply を付ければ書き込みます。")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="実際に書き込む（省略時はDRY-RUN）")
    ap.add_argument("--include-future", action="store_true", help="effective_at が未来の付与も含める")
    ap.add_argument("--limit", type=int, default=None, help="先頭N件のみ移行（テスト用）")
    args = ap.parse_args()
    main(apply=args.apply, include_future=args.include_future, limit=args.limit)

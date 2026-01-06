from uuid import uuid4
from datetime import datetime, timezone

KIND_EARN   = "earn"
KIND_SPEND  = "spend"
KIND_ADJUST = "adjust"

def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

def build_point_tx(*, user_id: str, kind: str, delta_points: int,
                   event_date: str, reason: str|None=None, source: str|None=None,
                   schedule_id: str|None=None, created_by: str|None=None, meta: dict|None=None):
    assert kind in {KIND_EARN, KIND_SPEND, KIND_ADJUST}
    if kind == KIND_EARN:  assert delta_points > 0
    if kind == KIND_SPEND: assert delta_points < 0

    now_iso = _now_iso()
    tx_id   = str(uuid4())
    joined_at = f"points#{kind}#{now_iso}#{tx_id}"

    item = {
        "user_id": user_id,
        "joined_at": joined_at,
        "tx_id": tx_id,
        "kind": kind,
        "delta_points": int(delta_points),
        "event_date": event_date,   # 'YYYY-MM-DD'
        "created_at": now_iso,
        "entity_type": "point_transaction",
        "version": 1,
    }
    if reason:      item["reason"] = reason
    if source:      item["source"] = source
    if schedule_id: item["schedule_id"] = schedule_id
    if created_by:  item["created_by"] = created_by
    if meta:        item["meta"] = meta
    return item

def put_point_tx(table, item):
    table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(user_id) AND attribute_not_exists(joined_at)"
    )

def record_earn(table, *, user_id: str, points: int, event_date: str,
                reason=None, source=None, schedule_id=None, created_by=None, meta=None):
    put_point_tx(table, build_point_tx(
        user_id=user_id, kind=KIND_EARN, delta_points=+int(points),
        event_date=event_date, reason=reason, source=source,
        schedule_id=schedule_id, created_by=created_by, meta=meta
    ))

def record_spend(history_table, *, user_id: str, points_used: int,
                 event_date: str, payment_type: str = "event_participation",
                 reason: str | None = None, created_by: str | None = None):
    """
    bad-users-history に消費(spend)トランザクションを1件保存する。
    """
    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    tx_id = str(uuid4())
    joined_at = f"points#spend#{now}#{tx_id}"

    item = {
        "user_id": user_id,
        "joined_at": joined_at,
        "tx_id": tx_id,
        "kind": "spend",                     # earn|spend|adjust
        "delta_points": -int(points_used),   # 残高計算用（負）
        "points_used": int(points_used),     # 互換のため（正）
        "payment_type": payment_type,
        "event_date": event_date,
        "reason": reason or f"{event_date}の参加費",
        "created_at": now,
        "entity_type": "point_transaction",
        "version": 1,
    }

    # 一意化（同じPK/SKの重複防止）
    history_table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(user_id) AND attribute_not_exists(joined_at)"
    )
    return item
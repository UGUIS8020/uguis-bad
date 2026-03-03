"""
bad_schedules テーブルの participants / tara_participants から
"user#UUID" 形式のエントリを削除するクリーニングスクリプト

実行方法:
    python clean_participants.py           # ← ドライラン（確認のみ）
    python clean_participants.py --apply   # ← 実際に更新
"""

import argparse
import boto3
from boto3.dynamodb.conditions import Attr

TABLE_NAME = "bad_schedules"
REGION = "ap-northeast-1"

def clean_list(items: list) -> tuple[list, bool]:
    """user# プレフィックスのエントリを除去し、(新リスト, 変更あり) を返す"""
    if not items:
        return items, False

    cleaned = []
    changed = False
    for item in items:
        if isinstance(item, str) and item.startswith("user#"):
            changed = True  # このエントリは除去
        else:
            cleaned.append(item)

    return cleaned, changed


def main(apply: bool):
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(TABLE_NAME)

    # 全スキャン
    response = table.scan()
    items = response.get("Items", [])
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))

    print(f"総スケジュール数: {len(items)}")

    update_count = 0
    for item in items:
        schedule_id = item["schedule_id"]
        date = item["date"]

        participants = item.get("participants", [])
        tara = item.get("tara_participants", [])

        new_participants, p_changed = clean_list(participants)
        new_tara, t_changed = clean_list(tara)

        if not p_changed and not t_changed:
            continue

        update_count += 1
        print(f"\n--- schedule_id: {schedule_id} / date: {date} ---")
        if p_changed:
            removed = [x for x in participants if isinstance(x, str) and x.startswith("user#")]
            print(f"  participants 削除対象: {removed}")
        if t_changed:
            removed = [x for x in tara if isinstance(x, str) and x.startswith("user#")]
            print(f"  tara_participants 削除対象: {removed}")

        if apply:
            update_expr_parts = []
            expr_values = {}

            if p_changed:
                update_expr_parts.append("participants = :p")
                expr_values[":p"] = new_participants

            if t_changed:
                update_expr_parts.append("tara_participants = :t")
                expr_values[":t"] = new_tara

            # participants_count も更新
            if p_changed:
                update_expr_parts.append("participants_count = :pc")
                expr_values[":pc"] = len(new_participants)

            table.update_item(
                Key={"schedule_id": schedule_id, "date": date},
                UpdateExpression="SET " + ", ".join(update_expr_parts),
                ExpressionAttributeValues=expr_values,
            )
            print(f"  → 更新完了")

    if update_count == 0:
        print("\n修正が必要なスケジュールはありませんでした。")
    else:
        if apply:
            print(f"\n{update_count} 件のスケジュールを更新しました。")
        else:
            print(f"\n{update_count} 件のスケジュールに修正が必要です。")
            print("実際に更新するには: python clean_participants.py --apply")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="実際に更新を実行する")
    args = parser.parse_args()

    if args.apply:
        print("=== 本番モード: DynamoDBを更新します ===\n")
    else:
        print("=== ドライランモード: 確認のみ（更新しません） ===\n")

    main(apply=args.apply)
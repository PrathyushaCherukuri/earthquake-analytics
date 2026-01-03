import os, json, base64, time, uuid
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
ddb = boto3.client("dynamodb")
sns = boto3.client("sns")
sqs = boto3.client("sqs")

RAW_BUCKET = os.environ["RAW_BUCKET"]
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/earthquakes")
DDB_TABLE = os.environ["DDB_TABLE"]

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
DLQ_URL = os.environ["DLQ_URL"]

ALERT_MAG_THRESHOLD = 4.0
MIN_VALID_MAG = 0.5


def _safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default


def _safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def send_to_dlq(reason, record):
    sqs.send_message(
        QueueUrl=DLQ_URL,
        MessageBody=json.dumps({
            "reason": reason,
            "record": record
        })
    )


def lambda_handler(event, context):
    records = event.get("Records", [])
    if not records:
        return {"status": "ok", "message": "No records"}

    decoded = []
    alerts_sent = 0
    ddb_writes = 0
    ddb_skipped = 0
    dlq_sent = 0

    for r in records:
        try:
            raw = base64.b64decode(r["kinesis"]["data"]).decode("utf-8")
            obj = json.loads(raw)
            decoded.append(obj)

            feature = obj.get("feature", {})
            props = feature.get("properties", {})
            geom = feature.get("geometry", {})
            coords = geom.get("coordinates", [None, None, None])

            quake_id = feature.get("id")
            if not quake_id:
                send_to_dlq("missing_quake_id", obj)
                dlq_sent += 1
                continue

            mag = _safe_float(props.get("mag"))
            if mag < MIN_VALID_MAG:
                send_to_dlq("magnitude_below_threshold", obj)
                dlq_sent += 1
                continue

            updated_ms = _safe_int(props.get("updated"))
            event_time_ms = _safe_int(props.get("time"))

            try:
                ddb.update_item(
                    TableName=DDB_TABLE,
                    Key={"quake_id": {"S": quake_id}},
                    UpdateExpression="""
                        SET #updated = :u,
                            event_time_ms = :t,
                            mag = :mag,
                            #place = :place,
                            #title = :title,
                            #url = :url,
                            lon = :lon,
                            lat = :lat,
                            depth_km = :dep
                    """,
                    ConditionExpression="attribute_not_exists(#updated) OR #updated < :u",
                    ExpressionAttributeNames={
                        "#updated": "updated",
                        "#place": "place",
                        "#title": "title",
                        "#url": "url",
                    },
                    ExpressionAttributeValues={
                        ":u": {"N": str(updated_ms)},
                        ":t": {"N": str(event_time_ms)},
                        ":mag": {"N": str(mag)},
                        ":place": {"S": str(props.get("place", ""))},
                        ":title": {"S": str(props.get("title", ""))},
                        ":url": {"S": str(props.get("url", ""))},
                        ":lon": {"N": str(coords[0] or 0)},
                        ":lat": {"N": str(coords[1] or 0)},
                        ":dep": {"N": str(coords[2] or 0)},
                    },
                )

                ddb_writes += 1

                # 🚨 SNS ONLY ON SUCCESSFUL NEW UPDATE
                if mag >= ALERT_MAG_THRESHOLD:
                    sns.publish(
                        TopicArn=SNS_TOPIC_ARN,
                        Subject=f"Earthquake Alert | M {mag}",
                        Message=(
                            f"🚨 Earthquake Alert 🚨\n\n"
                            f"Magnitude: {mag}\n"
                            f"Location: {props.get('place')}\n"
                            f"Event Time: {event_time_ms}\n"
                            f"URL: {props.get('url')}"
                        ),
                    )
                    alerts_sent += 1

            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    ddb_skipped += 1
                else:
                    send_to_dlq("ddb_error", obj)
                    dlq_sent += 1

        except Exception as e:
            send_to_dlq("parse_error", r)
            dlq_sent += 1

    # Write raw batch to S3
    now = datetime.now(timezone.utc)
    key = f"{RAW_PREFIX}/dt={now:%Y-%m-%d}/hour={now:%H}/batch_{int(time.time())}_{uuid.uuid4().hex}.jsonl"

    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body="\n".join(json.dumps(x) for x in decoded).encode("utf-8"),
        ContentType="application/json",
    )

    return {
        "status": "ok",
        "records": len(records),
        "ddb_writes": ddb_writes,
        "ddb_skipped": ddb_skipped,
        "alerts_sent": alerts_sent,
        "dlq_sent": dlq_sent,
        "s3_key": key,
    }

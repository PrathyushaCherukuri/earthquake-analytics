import json
import os
import time
import urllib.request
import boto3

kinesis = boto3.client("kinesis")

STREAM_NAME = os.environ["KINESIS_STREAM_NAME"]
USGS_URL = os.environ.get(
    "USGS_URL",
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
)

def fetch_usgs():
    with urllib.request.urlopen(USGS_URL, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))

def lambda_handler(event, context):
    data = fetch_usgs()
    features = data.get("features", [])

    if not features:
        return {"status": "ok", "message": "No features found", "records_sent": 0}

    # Build PutRecords payload (max 500 records per call)
    entries = []
    for f in features:
        quake_id = f.get("id", "unknown")
        payload = {
            "source": "usgs",
            "ingested_at_epoch_ms": int(time.time() * 1000),
            "feature": f
        }
        entries.append({
            "Data": json.dumps(payload).encode("utf-8"),
            "PartitionKey": quake_id  # stable key helps ordering per earthquake id
        })

    sent = 0
    failed = 0

    # Kinesis PutRecords batch size limit = 500
    for i in range(0, len(entries), 500):
        batch = entries[i:i+500]
        resp = kinesis.put_records(StreamName=STREAM_NAME, Records=batch)
        sent += len(batch)
        failed += resp.get("FailedRecordCount", 0)

        # If you want, you can log individual failed records here later

    return {
        "status": "ok",
        "records_sent": sent,
        "failed": failed
    }

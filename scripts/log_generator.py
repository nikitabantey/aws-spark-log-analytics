import boto3
import random
import time
import json

kinesis_client = boto3.client("kinesis", region_name="us-east-1")
STREAM_NAME = "log-stream"

def generate_log():
    logs = [
        {"level": "INFO", "message": "User logged in", "user": "user123"},
        {"level": "ERROR", "message": "Database connection failed", "user": "admin"},
        {"level": "WARN", "message": "High memory usage detected", "user": "server1"},
    ]
    return random.choice(logs)

while True:
    log_entry = generate_log()
    kinesis_client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(log_entry),
        PartitionKey="partition-1"
    )
    print(f"Sent: {log_entry}")
    time.sleep(2)  # Send a log every 2 seconds
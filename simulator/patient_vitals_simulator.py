import time
import json
import random
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT")
TOPIC_ID = os.getenv("PUBSUB_TOPIC")
PATIENT_COUNT = int(os.getenv("PATIENT_COUNT", 20))
STREAM_INTERVAL = int(os.getenv("STREAM_INTERVAL", 2))
ERROR_RATE = float(os.getenv("ERROR_RATE", 0.1))  # fraction of error records

# Generate list of patient IDs
patient_ids = [f"P{i:03d}" for i in range(1, PATIENT_COUNT + 1)]


def generate_labeled_vitals(error_rate=ERROR_RATE):
    """
    Generate one patient vital record with a chance to inject errors.
    Returns (record, injected_error), where injected_error is None for a clean record,
    or a label like 'missing_field:timestamp' / 'negative_value' / 'out_of_range'.
    The label is used by tests and the data quality report to measure rule coverage.
    """
    record = {
        "patient_id": random.choice(patient_ids),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "heart_rate": round(random.uniform(60, 120), 1),
        "spo2": round(random.uniform(90, 100), 1),
        "temperature": round(random.uniform(36.0, 39.0), 1),
        "bp_systolic": round(random.uniform(100, 140), 1),
        "bp_diastolic": round(random.uniform(60, 90), 1)
    }
    injected = None

    # Inject errors based on error_rate
    if random.random() < error_rate:
        error_type = random.choice(["missing_field", "negative_value", "out_of_range"])
        if error_type == "missing_field":
            # remove a random field
            field_to_remove = random.choice(list(record.keys()))
            record[field_to_remove] = None
            injected = f"missing_field:{field_to_remove}"
        elif error_type == "negative_value":
            record["heart_rate"] = -1
            injected = "negative_value"
        elif error_type == "out_of_range":
            record["spo2"] = 150  # invalid SpO2
            injected = "out_of_range"
    return record, injected


def generate_vitals():
    """Generate one patient vital record with a chance to inject errors."""
    record, _ = generate_labeled_vitals()
    return record


if __name__ == "__main__":
    from google.cloud import pubsub_v1

    # Pub/Sub client
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    print("Starting patient vitals simulator with error injection... Press Ctrl+C to stop.")

    while True:
        vitals = generate_vitals()
        message_json = json.dumps(vitals)
        future = publisher.publish(topic_path, message_json.encode("utf-8"))
        future.result()
        print(f"Published: {message_json}")
        time.sleep(STREAM_INTERVAL)

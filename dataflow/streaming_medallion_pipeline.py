import os
import json
from dotenv import load_dotenv
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows

from quality_rules import ValidateAndRoute, QUARANTINE_TAG

# ------------------- Load Environment Variables -------------------
load_dotenv()
PROJECT_ID = os.getenv("GCP_PROJECT")
PUBSUB_SUBSCRIPTION = os.getenv("PUBSUB_SUBSCRIPTION")
BRONZE_PATH = os.getenv("BRONZE_PATH")
SILVER_PATH = os.getenv("SILVER_PATH")
# Records that fail a data quality rule land here with their failure reasons.
# Defaults to a 'quarantine/' folder next to the Silver folder if not set.
QUARANTINE_PATH = os.getenv("QUARANTINE_PATH") or (
    SILVER_PATH.rstrip("/").rsplit("/", 1)[0] + "/quarantine/" if SILVER_PATH else None
)
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")
TEMP_LOCATION = os.getenv("TEMP_LOCATION")
STAGING_LOCATION = os.getenv("STAGING_LOCATION")
REGION = os.getenv("REGION")

# setup.py ships quality_rules.py to the Dataflow workers
SETUP_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "setup.py")


# ------------------- Enrichment -------------------
def enrich_record(record):
    record["risk_score"] = (
        (record["heart_rate"]/200)*0.4 +
        (record["temperature"]/40)*0.3 +
        (1 - record["spo2"]/100)*0.3
    )
    if record["risk_score"] < 0.3:
        record["risk_level"] = "Low"
    elif record["risk_score"] < 0.6:
        record["risk_level"] = "Moderate"
    else:
        record["risk_level"] = "High"
    return record


# ------------------- Gold aggregation -------------------
def extract_for_aggregation(record):
    return (record["patient_id"], record)


def aggregate_records(key_values):
    patient_id, records = key_values
    records = list(records)
    count = len(records)
    avg_heart_rate = sum(r["heart_rate"] for r in records)/count
    avg_spo2 = sum(r["spo2"] for r in records)/count
    avg_temp = sum(r["temperature"] for r in records)/count
    risk_levels = [r["risk_level"] for r in records]
    if "High" in risk_levels:
        max_risk = "High"
    elif "Moderate" in risk_levels:
        max_risk = "Moderate"
    else:
        max_risk = "Low"
    return {
        "patient_id": patient_id,
        "avg_heart_rate": avg_heart_rate,
        "avg_spo2": avg_spo2,
        "avg_temperature": avg_temp,
        "max_risk_level": max_risk
    }


# ------------------- Pipeline -------------------
def run():
    pipeline_options = PipelineOptions(
        project=PROJECT_ID,
        runner="DataflowRunner",
        streaming=True,
        temp_location=TEMP_LOCATION,
        staging_location=STAGING_LOCATION,
        region="us-east1",
        machine_type="n1-standard-1",
        save_main_session=True,
        setup_file=SETUP_FILE,
        job_name="patient-vitals-medallion-pipeline",
    )
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:

        # ------------------- Bronze Layer -------------------
        bronze_data = (
            p
            | "Read from PubSub" >> beam.io.ReadFromPubSub(subscription=PUBSUB_SUBSCRIPTION)
            | "Decode to string" >> beam.Map(lambda x: x.decode("utf-8"))
            | "Window Bronze Data" >> beam.WindowInto(FixedWindows(60))  # 1-minute windows
        )

        # Write raw messages to Bronze GCS
        bronze_data | "Write Bronze to GCS" >> beam.io.WriteToText(
            BRONZE_PATH + "raw_data",
            file_name_suffix=".json"
        )

        # ------------------- Data Quality Gate -------------------
        # Every message is checked against the rules in quality_rules.py.
        # Clean records continue to Silver; failed records are quarantined with their reasons.
        routed = (
            bronze_data
            | "Validate and Route" >> beam.ParDo(ValidateAndRoute()).with_outputs(QUARANTINE_TAG, main="valid")
        )

        routed[QUARANTINE_TAG] | "Write Quarantine to GCS" >> beam.io.WriteToText(
            QUARANTINE_PATH + "failed_records",
            file_name_suffix=".json"
        )

        # ------------------- Silver Layer -------------------
        silver_data = (
            routed.valid
            | "Enrich with Risk Score" >> beam.Map(enrich_record)
            | "Window Silver Data" >> beam.WindowInto(FixedWindows(60))
        )

        # Write Silver layer to GCS as JSON lines
        silver_data | "Serialize Silver" >> beam.Map(json.dumps) | "Write Silver to GCS" >> beam.io.WriteToText(
            SILVER_PATH + "cleaned_data",
            file_name_suffix=".json"
        )

        # ------------------- Gold Layer -------------------
        gold_data = (
            silver_data
            | "Key by patient_id" >> beam.Map(extract_for_aggregation)
            | "Group by patient_id" >> beam.GroupByKey()
            | "Aggregate per patient" >> beam.Map(aggregate_records)
        )

        # Write Gold to BigQuery
        gold_data | "Write Gold to BigQuery" >> beam.io.WriteToBigQuery(
            BIGQUERY_TABLE,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )


if __name__ == "__main__":
    run()

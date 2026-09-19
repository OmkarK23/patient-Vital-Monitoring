# Patient Vitals Monitoring System

A real-time healthcare data engineering pipeline built on Google Cloud Platform. Simulated patient vitals (heart rate, SpO2, body temperature, blood pressure) stream through Pub/Sub, get processed in real time by a Dataflow (Apache Beam) pipeline following the **Medallion Architecture**, land in BigQuery, and are visualized live in Power BI.

This project mirrors how enterprise healthcare systems handle IoT-style streaming data for real-time patient monitoring and decision-making.

## Architecture

![Patient Vitals Monitoring Pipeline Architecture](/home/omkarkalekar2323/patient-vital-monitoring/architecture-diagram.svg)

**Flow:**
1. A Python simulator generates synthetic patient vitals (with a configurable error-injection rate) and publishes them as JSON to a Pub/Sub topic.
2. A streaming Dataflow pipeline (Apache Beam) reads from a Pub/Sub subscription and processes messages through three layers:
   - **Bronze** — raw, unprocessed messages written to GCS in 60-second windows, for full auditability.
   - **Silver** — records parsed, validated (range checks on vitals), and enriched with a computed risk score and risk level, written to GCS.
   - **Gold** — records aggregated per patient (average heart rate, SpO2, temperature, and worst risk level observed) and written to BigQuery.
3. Power BI connects to the Gold BigQuery table for live/near-real-time dashboards.

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Google Cloud Pub/Sub |
| Stream Processing | Apache Beam on Google Cloud Dataflow |
| Storage (Bronze/Silver) | Google Cloud Storage |
| Storage (Gold) | Google BigQuery |
| Visualization | Power BI |
| Language | Python |
| Infra & Auth | GCP IAM, Cloud Shell |

## Repository Structure

```
patient-vital-monitoring/
├── simulator/
│   └── patient_vitals_simulator.py   # Generates and publishes synthetic vitals
├── dataflow/
│   └── streaming_medallion_pipeline.py  # Beam pipeline: Bronze -> Silver -> Gold
├── .gitignore
└── README.md
```

Each subfolder expects its own `.env` file (not committed — see `.gitignore`) with the relevant project, topic/subscription, and storage config.

## Medallion Layer Details

**Bronze** — Raw Pub/Sub messages, decoded and windowed, written as-is to `gs://<bucket>/bronze/`. No filtering or transformation — this is the system of record for what was actually received.

**Silver** — Bronze data parsed as JSON, filtered to drop records with missing fields or out-of-range vitals (heart rate, SpO2, temperature), then enriched with:
- `risk_score` — weighted combination of heart rate, temperature, and SpO2 deviation from normal
- `risk_level` — Low / Moderate / High, derived from the risk score

**Gold** — Silver data grouped by `patient_id` within each window and aggregated into average vitals plus the maximum (worst) risk level observed, written to BigQuery for downstream analytics and dashboards.

## Running It

```bash
# 1. Start the simulator (publishes to Pub/Sub)
cd simulator
python patient_vitals_simulator.py

# 2. In a separate terminal, run the streaming pipeline on Dataflow
cd dataflow
python streaming_medallion_pipeline.py
```

The Dataflow job runs continuously as a streaming job — monitor it in the [GCP Console](https://console.cloud.google.com/dataflow) and cancel it when done to avoid ongoing compute charges.

## Dashboard

_Add a screenshot of your Power BI dashboard here:_

```
![Power BI Dashboard](./docs/dashboard-screenshot.png)
```

## Author

**Omkar Kalekar**
[LinkedIn](https://linkedin.com/in/omkar-kalekar) · [Portfolio](https://omkark23.github.io/portfolio-website)
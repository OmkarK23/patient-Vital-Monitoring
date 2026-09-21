# Data Dictionary

Every field in each layer of the pipeline: what it means, where it comes from, what makes it valid, and how sensitive it would be if this pipeline carried real patient data.

This project uses **synthetic data only**. The classification column shows how each field would be treated under HIPAA in a real deployment, so the handling rules are designed in from the start rather than added later.

## Classification levels

| Level | Meaning | Handling in a real deployment |
|---|---|---|
| **PHI** | Protected Health Information: health data that is identifiable, or an identifier or date tied to a patient's care | Encrypted, access limited to named clinical and data roles, every read logged, retained only as long as policy requires |
| **Derived PHI** | Computed from PHI and still linked to a patient identifier | Same controls as PHI |
| **Internal** | Operational metadata with no patient information | Standard internal access |

Under HIPAA, health data linked to an identifier is PHI, and dates tied to an individual (except the year) count as identifiers. That is why `timestamp` is classified as PHI below.

## Bronze layer (GCS: `bronze/raw_data*.json`)

Raw Pub/Sub messages exactly as received, one JSON message per line. Nothing is filtered or changed. This is the system of record for what arrived.

| Field | Type | Definition | Classification |
|---|---|---|---|
| (whole message) | string | Raw message payload from the vitals device or simulator | PHI |

## Silver layer (GCS: `silver/cleaned_data*.json`)

Records that passed every error-level data quality rule, plus the computed risk fields. One JSON object per line.

| Field | Type | Definition | Valid values (rule) | Classification | Source |
|---|---|---|---|---|---|
| `patient_id` | string | Pseudonymous patient identifier | `P` followed by 3 digits (DQ-01, DQ-03) | PHI | Device / simulator |
| `timestamp` | string | Time the reading was taken, UTC | ISO-8601 `YYYY-MM-DDTHH:MM:SSZ`, not more than 5 min in the future; older than 24h is flagged (DQ-01, DQ-04, DQ-11, DQ-12) | PHI | Device / simulator |
| `heart_rate` | float | Heart rate, beats per minute | Numeric, above 0 and below 200 (DQ-01, DQ-02, DQ-05) | PHI | Device / simulator |
| `spo2` | float | Blood oxygen saturation, percent | Numeric, above 0 and at most 100 (DQ-01, DQ-02, DQ-06) | PHI | Device / simulator |
| `temperature` | float | Body temperature, degrees Celsius | Numeric, 30 to 45 inclusive (DQ-01, DQ-02, DQ-07) | PHI | Device / simulator |
| `bp_systolic` | float | Systolic blood pressure, mmHg | Numeric, 50 to 250 inclusive, greater than diastolic (DQ-01, DQ-02, DQ-08, DQ-10) | PHI | Device / simulator |
| `bp_diastolic` | float | Diastolic blood pressure, mmHg | Numeric, 30 to 150 inclusive (DQ-01, DQ-02, DQ-09, DQ-10) | PHI | Device / simulator |
| `risk_score` | float | Weighted score: 40% heart rate / 200, 30% temperature / 40, 30% (1 - SpO2 / 100) | 0 or greater | Derived PHI | Pipeline (`enrich_record`) |
| `risk_level` | string | Bucketed risk score | `Low` (< 0.3), `Moderate` (0.3 to < 0.6), `High` (>= 0.6) | Derived PHI | Pipeline (`enrich_record`) |

## Gold layer (BigQuery: `healthcare.patient_risk_analytics`)

One row per patient per 60-second window, aggregated from Silver. This is the table the Power BI dashboard reads.

| Field | Type | Definition | Classification | Source |
|---|---|---|---|---|
| `patient_id` | STRING | Pseudonymous patient identifier | PHI | Silver `patient_id` |
| `avg_heart_rate` | FLOAT | Mean heart rate in the window | Derived PHI | Silver `heart_rate` |
| `avg_spo2` | FLOAT | Mean SpO2 in the window | Derived PHI | Silver `spo2` |
| `avg_temperature` | FLOAT | Mean temperature in the window | Derived PHI | Silver `temperature` |
| `max_risk_level` | STRING | Worst risk level seen in the window (`High` > `Moderate` > `Low`) | Derived PHI | Silver `risk_level` |

Gold does not carry blood pressure or a window timestamp yet (see Known limitations in the README).

## Quarantine (GCS: `quarantine/failed_records*.json`)

Messages that failed at least one error-level rule. Nothing is dropped silently: each record keeps the original message and every reason it failed, so failures can be investigated and replayed.

| Field | Type | Definition | Classification |
|---|---|---|---|
| `raw_message` | string | The original message, unchanged | PHI |
| `failures` | array | One entry per failed rule: `rule_id`, `dimension`, `severity`, `detail` | PHI (the `detail` text can contain field values) |
| `quarantined_at` | string | UTC time the record was quarantined | Internal |

## Data quality rules

Rules are defined in [`dataflow/quality_rules.py`](../dataflow/quality_rules.py). **Error** rules send a record to quarantine. **Warn** rules let it through and count it in the pipeline metrics.

| Rule | Dimension | Severity | Check |
|---|---|---|---|
| DQ-00 | validity | error | Message is a valid JSON object |
| DQ-01 | completeness | error | All required fields are present and non-null |
| DQ-02 | validity | error | Vital sign fields are numeric |
| DQ-03 | validity | error | `patient_id` matches the `P###` format |
| DQ-04 | validity | error | `timestamp` is ISO-8601 UTC |
| DQ-05 | validity | error | `heart_rate` above 0 and below 200 |
| DQ-06 | validity | error | `spo2` above 0 and at most 100 |
| DQ-07 | validity | error | `temperature` 30 to 45 C |
| DQ-08 | validity | error | `bp_systolic` 50 to 250 mmHg |
| DQ-09 | validity | error | `bp_diastolic` 30 to 150 mmHg |
| DQ-10 | consistency | error | `bp_systolic` greater than `bp_diastolic` |
| DQ-11 | timeliness | error | `timestamp` not more than 5 minutes in the future |
| DQ-12 | timeliness | warn | `timestamp` not older than 24 hours |

The ranges catch data errors (sensor faults, bad payloads), not clinical alerts. A heart rate of 190 is valid data that should reach the risk scoring, not quarantine. In a real deployment, the clinical data owner would sign off on these ranges.

## Ownership (intended roles for a real deployment)

This is a solo portfolio project, so these are the roles a real deployment would assign, not people:

| Role | Responsible for | In this pipeline |
|---|---|---|
| **Data owner** (clinical operations lead) | Deciding what the data means, who can access it, the valid ranges, retention | Approves changes to `quality_rules.py` ranges and to Gold access |
| **Data steward** (clinical informatics analyst) | Keeping definitions current, reviewing quarantined records, raising quality issues | Maintains this document; reviews the quarantine folder and quality metrics |
| **Data custodian** (data engineering) | Implementing storage, pipeline, security controls | Runs the pipeline, IAM, buckets, BigQuery dataset |

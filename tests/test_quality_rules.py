import json
import os
import random
import sys
from datetime import datetime, timedelta, timezone

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "dataflow"))
sys.path.insert(0, os.path.join(ROOT, "simulator"))

from quality_rules import RULES, QUARANTINE_TAG, TIMESTAMP_FORMAT, ValidateAndRoute, validate  # noqa: E402
from patient_vitals_simulator import generate_labeled_vitals  # noqa: E402

NOW = datetime(2026, 9, 20, 12, 0, 0, tzinfo=timezone.utc)


def clean_record(**overrides):
    record = {
        "patient_id": "P001",
        "timestamp": NOW.strftime(TIMESTAMP_FORMAT),
        "heart_rate": 80.0,
        "spo2": 97.0,
        "temperature": 37.0,
        "bp_systolic": 120.0,
        "bp_diastolic": 80.0,
    }
    record.update(overrides)
    return record


def failed_rules(record, now=NOW):
    return {f["rule_id"] for f in validate(record, now)}


# ------------------- Individual rules -------------------

def test_clean_record_passes():
    assert validate(clean_record(), NOW) == []


def test_missing_timestamp_is_caught():
    # The original validation did not check timestamp, so these records reached Silver
    assert failed_rules(clean_record(timestamp=None)) == {"DQ-01"}


def test_each_missing_field_reports_completeness_only():
    for field in clean_record():
        assert failed_rules(clean_record(**{field: None})) == {"DQ-01"}, field


def test_non_numeric_and_boolean_vitals_rejected():
    assert "DQ-02" in failed_rules(clean_record(heart_rate="80"))
    assert "DQ-02" in failed_rules(clean_record(spo2=True))


def test_patient_id_format():
    assert failed_rules(clean_record(patient_id="12345")) == {"DQ-03"}
    assert failed_rules(clean_record(patient_id=42)) == {"DQ-03"}


def test_timestamp_format():
    assert failed_rules(clean_record(timestamp="20/09/2026 12:00")) == {"DQ-04"}


def test_vital_ranges():
    assert failed_rules(clean_record(heart_rate=-1)) == {"DQ-05"}
    assert failed_rules(clean_record(heart_rate=200)) == {"DQ-05"}
    assert failed_rules(clean_record(spo2=150)) == {"DQ-06"}
    assert failed_rules(clean_record(spo2=0)) == {"DQ-06"}
    assert failed_rules(clean_record(temperature=29.9)) == {"DQ-07"}
    assert failed_rules(clean_record(bp_systolic=300, bp_diastolic=80)) == {"DQ-08"}
    assert failed_rules(clean_record(bp_diastolic=20)) == {"DQ-09"}


def test_range_boundaries_that_should_pass():
    assert validate(clean_record(spo2=100, temperature=30), NOW) == []
    assert validate(clean_record(temperature=45), NOW) == []


def test_bp_consistency():
    assert failed_rules(clean_record(bp_systolic=80, bp_diastolic=80)) == {"DQ-10"}
    assert failed_rules(clean_record(bp_systolic=70, bp_diastolic=90)) == {"DQ-10"}


def test_future_timestamp_is_error_but_small_skew_allowed():
    future = (NOW + timedelta(hours=1)).strftime(TIMESTAMP_FORMAT)
    skew = (NOW + timedelta(minutes=2)).strftime(TIMESTAMP_FORMAT)
    assert failed_rules(clean_record(timestamp=future)) == {"DQ-11"}
    assert validate(clean_record(timestamp=skew), NOW) == []


def test_stale_timestamp_is_warning_only():
    stale = (NOW - timedelta(days=2)).strftime(TIMESTAMP_FORMAT)
    failures = validate(clean_record(timestamp=stale), NOW)
    assert [(f["rule_id"], f["severity"]) for f in failures] == [("DQ-12", "warn")]


def test_rule_ids_unique_and_dimensions_known():
    ids = [r.rule_id for r in RULES]
    assert len(ids) == len(set(ids))
    assert {r.dimension for r in RULES} <= {"completeness", "validity", "consistency", "timeliness"}
    assert {r.severity for r in RULES} <= {"error", "warn"}


# ------------------- Against the simulator -------------------

def test_all_injected_simulator_errors_caught_with_no_false_positives():
    random.seed(7)
    for _ in range(5000):
        record, injected = generate_labeled_vitals(error_rate=0.2)
        record = json.loads(json.dumps(record))
        blocked = any(f["severity"] == "error" for f in validate(record))
        assert blocked == bool(injected), (record, injected)


# ------------------- Beam routing -------------------

def test_dofn_routes_valid_and_quarantined_records():
    good = json.dumps(clean_record(timestamp=datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT)))
    bad_range = json.dumps(clean_record(spo2=150))
    bad_json = "{not json"

    with TestPipeline() as p:
        routed = (
            p
            | beam.Create([good, bad_range, bad_json])
            | beam.ParDo(ValidateAndRoute()).with_outputs(QUARANTINE_TAG, main="valid")
        )
        assert_that(
            routed.valid | "patient ids" >> beam.Map(lambda r: r["patient_id"]),
            equal_to(["P001"]),
            label="valid",
        )
        assert_that(
            routed[QUARANTINE_TAG] | "reasons" >> beam.Map(
                lambda s: (json.loads(s)["raw_message"], [f["rule_id"] for f in json.loads(s)["failures"]])
            ),
            equal_to([(bad_range, ["DQ-06"]), (bad_json, ["DQ-00"])]),
            label="quarantine",
        )


def test_docs_list_every_rule():
    with open(os.path.join(ROOT, "docs", "data_dictionary.md")) as fh:
        doc = fh.read()
    missing = [r.rule_id for r in RULES if r.rule_id not in doc]
    assert not missing, f"data_dictionary.md is missing rules: {missing}"

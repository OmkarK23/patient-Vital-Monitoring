"""
Data quality rules for the patient vitals stream.

Every rule has an ID, a data quality dimension, a severity, and a plain-language
description, so the rule set can be reviewed like a policy document and changed
in one place.

Severity:
  error -> the record is quarantined (kept out of Silver/Gold, written with its failure reasons)
  warn  -> the record passes, but the failure is counted in pipeline metrics

This module has two parts:
  1. Pure-Python rules and `validate()` (no Beam dependency beyond the DoFn below)
  2. `ValidateAndRoute`, a Beam DoFn that routes records to the valid or quarantine output
"""

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Optional

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.pvalue import TaggedOutput

# ------------------- Record contract -------------------

REQUIRED_FIELDS = [
    "patient_id",
    "timestamp",
    "heart_rate",
    "spo2",
    "temperature",
    "bp_systolic",
    "bp_diastolic",
]
NUMERIC_FIELDS = ["heart_rate", "spo2", "temperature", "bp_systolic", "bp_diastolic"]
PATIENT_ID_PATTERN = re.compile(r"^P\d{3}$")
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

MAX_FUTURE_SKEW = timedelta(minutes=5)  # tolerate small device clock drift
STALE_AFTER = timedelta(hours=24)       # late but plausible data is kept, only flagged

QUARANTINE_TAG = "quarantine"


def _num(record, field):
    """Return the field as a number, or None if it is missing or not numeric."""
    value = record.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value


def _parse_ts(record):
    try:
        return datetime.strptime(record.get("timestamp"), TIMESTAMP_FORMAT).replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


# ------------------- Rule checks -------------------
# Each check returns None when the record passes, or a short detail string when it fails.
# Checks that depend on another field skip (return None) when that field is missing
# or malformed, so one bad field produces one clear failure instead of several.

def _check_required(record, now):
    missing = [f for f in REQUIRED_FIELDS if record.get(f) is None]
    return f"missing or null: {', '.join(missing)}" if missing else None


def _check_numeric_types(record, now):
    bad = [f for f in NUMERIC_FIELDS if record.get(f) is not None and _num(record, f) is None]
    return f"not numeric: {', '.join(bad)}" if bad else None


def _check_patient_id(record, now):
    pid = record.get("patient_id")
    if pid is None:
        return None
    return None if isinstance(pid, str) and PATIENT_ID_PATTERN.match(pid) else f"invalid patient_id: {pid!r}"


def _check_timestamp_format(record, now):
    if record.get("timestamp") is None:
        return None
    return None if _parse_ts(record) else f"unparseable timestamp: {record.get('timestamp')!r}"


def _range_check(field, low, high, low_inclusive, high_inclusive):
    def check(record, now):
        value = _num(record, field)
        if value is None:
            return None
        ok_low = value >= low if low_inclusive else value > low
        ok_high = value <= high if high_inclusive else value < high
        return None if (ok_low and ok_high) else f"{field}={value} outside allowed range"
    return check


def _check_bp_consistency(record, now):
    sys_bp, dia_bp = _num(record, "bp_systolic"), _num(record, "bp_diastolic")
    if sys_bp is None or dia_bp is None:
        return None
    return None if sys_bp > dia_bp else f"bp_systolic ({sys_bp}) not greater than bp_diastolic ({dia_bp})"


def _check_not_future(record, now):
    ts = _parse_ts(record)
    if ts is None:
        return None
    return None if ts <= now + MAX_FUTURE_SKEW else f"timestamp {record['timestamp']} is in the future"


def _check_not_stale(record, now):
    ts = _parse_ts(record)
    if ts is None:
        return None
    return None if now - ts <= STALE_AFTER else f"timestamp {record['timestamp']} older than 24h"


# ------------------- Rule registry -------------------

@dataclass(frozen=True)
class Rule:
    rule_id: str
    dimension: str
    severity: str
    description: str
    check: Callable[[dict, datetime], Optional[str]]


RULES: List[Rule] = [
    Rule("DQ-01", "completeness", "error", "All required fields are present and non-null", _check_required),
    Rule("DQ-02", "validity", "error", "Vital sign fields are numeric", _check_numeric_types),
    Rule("DQ-03", "validity", "error", "patient_id matches the P### identifier format", _check_patient_id),
    Rule("DQ-04", "validity", "error", "timestamp is ISO-8601 UTC (YYYY-MM-DDTHH:MM:SSZ)", _check_timestamp_format),
    Rule("DQ-05", "validity", "error", "heart_rate is between 0 and 200 bpm (exclusive)",
         _range_check("heart_rate", 0, 200, False, False)),
    Rule("DQ-06", "validity", "error", "spo2 is above 0 and at most 100 percent",
         _range_check("spo2", 0, 100, False, True)),
    Rule("DQ-07", "validity", "error", "temperature is between 30 and 45 C (inclusive)",
         _range_check("temperature", 30, 45, True, True)),
    Rule("DQ-08", "validity", "error", "bp_systolic is between 50 and 250 mmHg (inclusive)",
         _range_check("bp_systolic", 50, 250, True, True)),
    Rule("DQ-09", "validity", "error", "bp_diastolic is between 30 and 150 mmHg (inclusive)",
         _range_check("bp_diastolic", 30, 150, True, True)),
    Rule("DQ-10", "consistency", "error", "bp_systolic is greater than bp_diastolic", _check_bp_consistency),
    Rule("DQ-11", "timeliness", "error", "timestamp is not more than 5 minutes in the future", _check_not_future),
    Rule("DQ-12", "timeliness", "warn", "timestamp is not older than 24 hours", _check_not_stale),
]

# Recorded when a message cannot be parsed into a JSON object at all.
PARSE_RULE = Rule("DQ-00", "validity", "error", "Message is a valid JSON object", lambda r, n: None)


def validate(record: dict, now: Optional[datetime] = None) -> List[dict]:
    """Run every rule against a parsed record. Returns a list of failures (empty means clean)."""
    now = now or datetime.now(timezone.utc)
    failures = []
    for rule in RULES:
        detail = rule.check(record, now)
        if detail:
            failures.append({
                "rule_id": rule.rule_id,
                "dimension": rule.dimension,
                "severity": rule.severity,
                "detail": detail,
            })
    return failures


def parse_message(message: str):
    """Parse a raw message. Returns (record, failure); exactly one of them is None."""
    try:
        record = json.loads(message)
    except (json.JSONDecodeError, TypeError) as exc:
        return None, f"invalid JSON: {exc}"
    if not isinstance(record, dict):
        return None, f"expected a JSON object, got {type(record).__name__}"
    return record, None


# ------------------- Beam transform -------------------

class ValidateAndRoute(beam.DoFn):
    """
    Parses and validates each raw message.
      main output        -> clean records (dicts), continue to Silver
      'quarantine' output -> JSON strings holding the raw message and every failure reason
    Beam counters under the 'data_quality' namespace show pass/fail volumes per rule
    in the Dataflow console.
    """

    def __init__(self):
        self.records_in = Metrics.counter("data_quality", "records_in")
        self.records_valid = Metrics.counter("data_quality", "records_valid")
        self.records_quarantined = Metrics.counter("data_quality", "records_quarantined")
        self.rule_counters = {
            r.rule_id: Metrics.counter("data_quality", f"failed_{r.rule_id}")
            for r in RULES + [PARSE_RULE]
        }

    def process(self, message):
        self.records_in.inc()
        record, parse_error = parse_message(message)

        if parse_error:
            failures = [{
                "rule_id": PARSE_RULE.rule_id,
                "dimension": PARSE_RULE.dimension,
                "severity": PARSE_RULE.severity,
                "detail": parse_error,
            }]
        else:
            failures = validate(record)

        for failure in failures:
            self.rule_counters[failure["rule_id"]].inc()

        if any(f["severity"] == "error" for f in failures):
            self.records_quarantined.inc()
            yield TaggedOutput(QUARANTINE_TAG, json.dumps({
                "raw_message": message,
                "failures": failures,
                "quarantined_at": datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT),
            }))
        else:
            self.records_valid.inc()
            yield record

"""
Data quality report: runs the simulator offline (no Pub/Sub) and compares the original
validation logic with the rule set in dataflow/quality_rules.py on the same records.

Usage:
    python tools/data_quality_report.py --records 10000 --seed 42 --error-rate 0.1
"""

import argparse
import json
import os
import random
import sys
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "dataflow"))
sys.path.insert(0, os.path.join(ROOT, "simulator"))

from quality_rules import RULES, validate  # noqa: E402
from patient_vitals_simulator import generate_labeled_vitals  # noqa: E402


def legacy_is_valid_record(record):
    """The pipeline's validation before the data quality layer, reproduced unchanged as a baseline."""
    try:
        if record is None:
            return False
        p_id = record.get("patient_id")
        hr = record.get("heart_rate")
        spo2 = record.get("spo2")
        temp = record.get("temperature")
        bp_sys = record.get("bp_systolic")
        bp_dia = record.get("bp_diastolic")
        if p_id is None or hr is None or spo2 is None or temp is None or bp_sys is None or bp_dia is None:
            return False
        if not (0 < spo2 <= 100):
            return False
        if not (0 < hr < 200):
            return False
        if not (30 <= temp <= 45):
            return False
        return True
    except Exception:
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--error-rate", type=float, default=0.1)
    args = parser.parse_args()

    random.seed(args.seed)
    # Round-trip through JSON so records look exactly like Pub/Sub messages
    samples = [generate_labeled_vitals(args.error_rate) for _ in range(args.records)]
    samples = [(json.loads(json.dumps(rec)), label) for rec, label in samples]

    injected = [(r, l) for r, l in samples if l]
    clean = [(r, l) for r, l in samples if not l]

    new_failures = {id(r): validate(r) for r, _ in samples}

    def new_blocks(r):
        return any(f["severity"] == "error" for f in new_failures[id(r)])

    legacy_missed = [l for r, l in injected if legacy_is_valid_record(r)]
    new_missed = [l for r, l in injected if not new_blocks(r)]
    legacy_false_pos = sum(1 for r, _ in clean if not legacy_is_valid_record(r))
    new_false_pos = sum(1 for r, _ in clean if new_blocks(r))

    rule_hits = Counter(f["rule_id"] for fs in new_failures.values() for f in fs)
    dimension_hits = Counter(f["dimension"] for fs in new_failures.values() for f in fs)

    n_inj = len(injected)
    print(f"Records generated: {len(samples):,} (seed={args.seed}, error_rate={args.error_rate})")
    print(f"  Clean records:    {len(clean):,}")
    print(f"  Injected errors:  {n_inj:,}")
    print()
    print("Injected errors that reached Silver (not caught):")
    print(f"  Original validation: {len(legacy_missed):,} of {n_inj:,} ({len(legacy_missed) / n_inj:.1%})")
    print(f"  Quality rules:       {len(new_missed):,} of {n_inj:,} ({len(new_missed) / n_inj:.1%})")
    if legacy_missed:
        print(f"  Errors the original validation let through: {dict(Counter(legacy_missed))}")
    print()
    print("Clean records wrongly rejected (false positives):")
    print(f"  Original validation: {legacy_false_pos:,}")
    print(f"  Quality rules:       {new_false_pos:,}")
    print()
    print(f"Rules in effect: {len(RULES)}")
    print("Failures by rule:")
    for rule in RULES:
        print(f"  {rule.rule_id} [{rule.dimension}/{rule.severity}] {rule.description}: {rule_hits.get(rule.rule_id, 0):,}")
    print("Failures by dimension:", dict(dimension_hits))


if __name__ == "__main__":
    main()

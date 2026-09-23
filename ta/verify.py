"""TA ONLY. Compare Postgres alerts with the answer key.
python verify.py --key ../data/answer_key.csv"""
import argparse
import csv

import psycopg2

ap = argparse.ArgumentParser()
ap.add_argument("--key", required=True)
ap.add_argument("--pg", default="host=10.147.17.13 port=5432 dbname=fraud user=fraud password=fraud")  # <-- L3
a = ap.parse_args()

expected = {r["alert_id"] for r in csv.DictReader(open(a.key))}
with psycopg2.connect(a.pg) as c, c.cursor() as cur:
    cur.execute("SELECT alert_id FROM alerts")
    rows = [r[0] for r in cur.fetchall()]
got = set(rows)
print(f"expected {len(expected)} | in Postgres {len(rows)} | duplicates {len(rows) - len(got)}")
print("missing:", sorted(expected - got) or "none")
print("extra:  ", sorted(got - expected) or "none")
print("RESULT:", "PASS" if got == expected and len(rows) == len(got) else "FAIL")

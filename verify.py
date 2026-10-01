"""Compares outputs against the answer key (order-insensitive, numeric tolerance 0.011).
Usage: python verify.py team_01 [--dir .]   (run on VM4 for order_metrics.jsonl, on VM1 for the two CSVs)"""
import csv
import json
import os
import sys

team = sys.argv[1]
d = sys.argv[sys.argv.index("--dir") + 1] if "--dir" in sys.argv else "."
key = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "answer_keys", team)


def same(x, y):
    try:
        return abs(float(x) - float(y)) < 0.011
    except (TypeError, ValueError):
        return x == y


def check(name, got, exp, idcol):
    got = {r[idcol]: r for r in got}
    exp = {r[idcol]: r for r in exp}
    bad = [k for k in exp if k not in got or any(not same(got[k].get(c), v) for c, v in exp[k].items())]
    extra = [k for k in got if k not in exp]
    print(f"{name}: {'OK' if not bad and not extra else 'MISMATCH'} ({len(got)} rows, expected {len(exp)})"
          + (f" bad={bad[:5]}" if bad else "") + (f" extra={extra[:5]}" if extra else ""))


def jl(p):
    return [json.loads(l) for l in open(p) if l.strip()]


def cs(p):
    return list(csv.DictReader(open(p)))


for name, load, idcol in [("order_metrics.jsonl", jl, "order_id"), ("restaurant_summary.csv", cs, "restaurant_id"), ("city_summary.csv", cs, "city")]:
    if os.path.exists(os.path.join(d, name)):
        check(name, load(os.path.join(d, name)), load(os.path.join(key, name)), idcol)
    else:
        print(f"{name}: not found in {d}")

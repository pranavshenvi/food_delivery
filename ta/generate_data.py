"""TA ONLY. Generates part_A.csv, part_B.csv and answer_key.csv.
Usage: python generate_data.py --seed 42 --out ../data
Use a different --seed for the hidden demo dataset."""
import argparse
import csv
import os
import random
from datetime import datetime, timedelta, timezone

from rules import CardHistory, haversine_km

DATA_START = datetime(2026, 10, 5, 10, 0, 0)
SPAN_S = 900  # 15 minutes of event time
CITIES = {
    "Bengaluru": (12.9716, 77.5946), "Mumbai": (19.0760, 72.8777), "Delhi": (28.6139, 77.2090),
    "Chennai": (13.0827, 80.2707), "Kolkata": (22.5726, 88.3639), "Hyderabad": (17.3850, 78.4867),
    "Pune": (18.5204, 73.8567), "Ahmedabad": (23.0225, 72.5714), "Jaipur": (26.9124, 75.7873),
    "Lucknow": (26.8467, 80.9462), "Guwahati": (26.1445, 91.7362), "Kochi": (9.9312, 76.2673),
}
FIELDS = ["txn_id", "card_id", "amount", "merchant", "city", "lat", "lon", "event_time"]


def ts_ms(event_time):
    dt = datetime.fromisoformat(event_time).replace(tzinfo=timezone.utc)
    return (dt - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(milliseconds=1)


def txn(rng, card, t, amount, city, merchant=None, loc=None):
    lat, lon = loc or (CITIES[city][0] + rng.uniform(-0.01, 0.01), CITIES[city][1] + rng.uniform(-0.01, 0.01))
    return {"card_id": card, "amount": round(amount, 2), "merchant": merchant or f"M-{rng.randint(100, 999)}",
            "city": city, "lat": round(lat, 5), "lon": round(lon, 5),
            "event_time": (DATA_START + timedelta(seconds=t)).isoformat(timespec="milliseconds")}


def normal_times(rng, until):
    t, out = rng.uniform(0, 60), []
    while t < until:
        out.append(t)
        t += 20 + rng.expovariate(1 / 70)
    return out


def normal_amount(rng, mean):
    return min(max(mean * rng.lognormvariate(0, 0.35), 0.3 * mean), 2.5 * mean)


def make_card(rng, card, fraud):
    home = rng.choice(list(CITIES))
    mean = rng.uniform(200, 3000)
    if fraud == "geo":
        times = normal_times(rng, 600)
        rows = [txn(rng, card, t, normal_amount(rng, mean), home) for t in times]
        far = rng.choice([c for c in CITIES if haversine_km(*CITIES[home], *CITIES[c]) > 1000])
        rows.append(txn(rng, card, min(times[-1] + rng.uniform(180, 290), SPAN_S - 1), normal_amount(rng, mean), far))
        return rows
    times = normal_times(rng, SPAN_S)
    while fraud == "spike" and len(times) < 7:
        times = normal_times(rng, SPAN_S)
    if fraud == "velocity":
        b = rng.uniform(240, 700)
        burst = [b]
        for _ in range(4):
            burst.append(burst[-1] + rng.uniform(2, 4))
        times = [t for t in times if not (b - 61 <= t <= burst[-1] + 61)]
        m = f"M-{rng.randint(100, 999)}"
        loc = (CITIES[home][0] + rng.uniform(-0.01, 0.01), CITIES[home][1] + rng.uniform(-0.01, 0.01))
        rows = [txn(rng, card, t, normal_amount(rng, mean), home) for t in times]
        rows += [txn(rng, card, t, normal_amount(rng, mean), home, m, loc) for t in burst]
        return rows
    rows = [txn(rng, card, t, normal_amount(rng, mean), home) for t in times]
    if fraud == "spike":
        i = rng.randint(4, len(rows) - 1)
        rows[i]["amount"] = round(mean * rng.uniform(8, 15), 2)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="../data")
    a = ap.parse_args()
    rng = random.Random(a.seed)
    os.makedirs(a.out, exist_ok=True)

    cards = {"A": list(range(1001, 1021)), "B": list(range(2001, 2021))}
    fraud_cards = rng.sample(cards["A"], 5) + rng.sample(cards["B"], 4)
    types = ["velocity"] * 3 + ["geo"] * 3 + ["spike"] * 3
    rng.shuffle(types)
    fraud = dict(zip(fraud_cards, types))

    all_rows = []
    for part, ids in cards.items():
        rows = [r for c in ids for r in make_card(rng, c, fraud.get(c))]
        rows.sort(key=lambda r: (r["event_time"], r["card_id"]))
        for i, r in enumerate(rows, 1):
            r["txn_id"] = f"{part}-{i:06d}"
        with open(os.path.join(a.out, f"part_{part}.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, FIELDS, lineterminator="\n")
            w.writeheader()
            w.writerows(rows)
        all_rows += rows

    # Reference detector: exactly the rules the Spark job uses, run in event-time order per card.
    hist, key = {}, []
    for r in sorted(all_rows, key=lambda r: (r["card_id"], r["event_time"], r["txn_id"])):
        h = hist.setdefault(r["card_id"], CardHistory())
        for rule, details in h.process(ts_ms(r["event_time"]), float(r["amount"]), float(r["lat"]), float(r["lon"]), r["city"]):
            key.append({"alert_id": f"{rule}:{r['txn_id']}", "rule": rule, "card_id": r["card_id"],
                        "txn_id": r["txn_id"], "event_time": r["event_time"], "details": details})
    key.sort(key=lambda k: k["event_time"])
    with open(os.path.join(a.out, "answer_key.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, ["alert_id", "rule", "card_id", "txn_id", "event_time", "details"], lineterminator="\n")
        w.writeheader()
        w.writerows(key)

    print(f"part_A: {sum(1 for r in all_rows if r['txn_id'][0] == 'A')} rows, "
          f"part_B: {sum(1 for r in all_rows if r['txn_id'][0] == 'B')} rows")
    print("injected:", {c: fraud[c] for c in sorted(fraud)})
    print(f"answer key: {len(key)} alerts")
    for k in key:
        print(f"  {k['event_time'][11:19]}  {k['alert_id']:<12} card {k['card_id']}  {k['details']}")


if __name__ == "__main__":
    main()

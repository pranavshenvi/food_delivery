"""Fraud rules. Shared by the Spark job (L4) and the TA's reference detector, so both give identical answers."""
import json
import math

R1_WINDOW_MS = 60_000   # velocity window
R1_COUNT = 5            # >= 5 txns in the window (incl. current) -> alert
R2_MAX_KMH = 900        # faster than this between consecutive txns -> alert
R3_FACTOR = 5           # amount > 5x avg of history -> alert
R3_MIN_HIST = 3         # need at least 3 earlier txns
R3_HIST = 20            # avg over last 20 txns


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(a))


class CardHistory:
    """One card's 'notebook page'."""

    def __init__(self, recent=None, last=None, amounts=None):
        self.recent = recent or []    # event times (ms) inside the last 60 s
        self.last = last              # [lat, lon, ts_ms, city] of previous txn
        self.amounts = amounts or []  # last 20 amounts

    def to_json(self):
        return json.dumps({"recent": self.recent, "last": self.last, "amounts": self.amounts})

    @classmethod
    def from_json(cls, s):
        d = json.loads(s)
        return cls(d["recent"], d["last"], d["amounts"])

    def process(self, ts_ms, amount, lat, lon, city):
        """Judge one txn against history, then add it to history. Returns [(rule, details)]."""
        alerts = []

        self.recent = [t for t in self.recent if t >= ts_ms - R1_WINDOW_MS] + [ts_ms]
        if len(self.recent) >= R1_COUNT:
            alerts.append(("R1", f"{len(self.recent)} txns in 60s"))

        if self.last is not None:
            plat, plon, pts, pcity = self.last
            km = haversine_km(plat, plon, lat, lon)
            dt_h = (ts_ms - pts) / 3_600_000
            speed = km / dt_h if dt_h > 0 else (math.inf if km > 1 else 0.0)
            if speed > R2_MAX_KMH:
                alerts.append(("R2", f"{pcity}->{city} {km:.0f} km in {(ts_ms - pts) / 1000:.0f}s ({speed:.0f} km/h)"))
        self.last = [lat, lon, ts_ms, city]

        if len(self.amounts) >= R3_MIN_HIST:
            avg = sum(self.amounts) / len(self.amounts)
            if amount > R3_FACTOR * avg:
                alerts.append(("R3", f"amount {amount:.2f} vs avg {avg:.2f}"))
        self.amounts = (self.amounts + [amount])[-R3_HIST:]

        return alerts

"""L4: python fraud_job.py   (rules.py must be in the same folder)
Batch Spark: reads the whole `transactions` topic in one shot (offsets earliest..latest, whatever
L1 has produced by the time you run this), runs the fraud rules over each card's full history, and
republishes alerts onto `flagged_transactions` for L1 to pick up with its own batch job."""
import time

import pandas as pd
import pyspark
from pyspark.sql import SparkSession, functions as F

from rules import CardHistory

BOOTSTRAP = "10.147.17.12:9092,10.147.17.13:9092"  # <-- ZeroTier IPs of L2, L3 VMs (the Kafka brokers)

TXN_SCHEMA = ("txn_id STRING, card_id INT, amount DOUBLE, merchant STRING, city STRING, "
              "lat DOUBLE, lon DOUBLE, event_time STRING")
ALERT_SCHEMA = "alert_id STRING, rule STRING, card_id INT, txn_id STRING, event_time STRING, details STRING"
ALERT_COLS = ["alert_id", "rule", "card_id", "txn_id", "event_time", "details"]


def detect(pdf):
    """Called once per card with that card's entire history for the run, in event-time order."""
    hist = CardHistory()
    pdf = pdf.sort_values(["ts_ms", "txn_id"])
    out = []
    for r in pdf.itertuples(index=False):
        for rule, details in hist.process(int(r.ts_ms), float(r.amount), float(r.lat), float(r.lon), r.city):
            out.append([f"{rule}:{r.txn_id}", rule, int(r.card_id), r.txn_id, r.event_time, details])
    return pd.DataFrame(out, columns=ALERT_COLS)


def main():
    scala = "2.13" if pyspark.__version__ >= "4" else "2.12"
    spark = (SparkSession.builder.appName("fraud-detector-batch")
             .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{pyspark.__version__}")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.addPyFile("rules.py")

    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", "transactions")
           .option("startingOffsets", "earliest")
           .option("endingOffsets", "latest")
           .load())
    txns = raw.select(F.from_json(F.col("value").cast("string"), TXN_SCHEMA).alias("t")).select("t.*")
    txns = txns.withColumn("ts_ms", F.expr("unix_millis(to_timestamp(event_time))"))

    alerts = txns.groupBy("card_id").applyInPandas(detect, schema=ALERT_SCHEMA)
    alerts.cache()
    rows = alerts.collect()
    print(f"{time.strftime('%H:%M:%S')} {len(rows)} alerts detected across all cards")

    if rows:
        (alerts.select(F.col("card_id").cast("string").alias("key"),
                        F.to_json(F.struct(*ALERT_COLS)).alias("value"))
               .write.format("kafka")
               .option("kafka.bootstrap.servers", BOOTSTRAP)
               .option("topic", "flagged_transactions")
               .save())

    for r in rows:
        print(f"    ALERT {r['alert_id']:<12} card {r['card_id']}  event {r['event_time'][11:19]}  {r['details']}")

    spark.stop()


if __name__ == "__main__":
    main()

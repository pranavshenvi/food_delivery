"""L4: python fraud_job.py   (rules.py must be in the same folder)"""
import os
import time

import pandas as pd
import psycopg2
import pyspark
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.streaming.state import GroupStateTimeout

from rules import CardHistory

BOOTSTRAP = "10.147.17.11:9092,10.147.17.12:9092,10.147.17.13:9092"  # <-- ZeroTier IPs of L1, L2, L3 VMs
PG = "host=10.147.17.13 port=5432 dbname=fraud user=fraud password=fraud"  # <-- L3 VM
CHECKPOINT = "./checkpoint"

TXN_SCHEMA = ("txn_id STRING, card_id INT, amount DOUBLE, merchant STRING, city STRING, "
              "lat DOUBLE, lon DOUBLE, event_time STRING")
ALERT_SCHEMA = "alert_id STRING, rule STRING, card_id INT, txn_id STRING, event_time STRING, details STRING"
ALERT_COLS = ["alert_id", "rule", "card_id", "txn_id", "event_time", "details"]


def detect(key, pdfs, state):
    """Called once per card per batch with that card's new txns. State = the card's notebook page."""
    hist = CardHistory.from_json(state.get[0]) if state.exists else CardHistory()
    pdf = pd.concat(list(pdfs)).sort_values(["ts_ms", "txn_id"])
    out = []
    for r in pdf.itertuples(index=False):
        for rule, details in hist.process(int(r.ts_ms), float(r.amount), float(r.lat), float(r.lon), r.city):
            out.append([f"{rule}:{r.txn_id}", rule, int(r.card_id), r.txn_id, r.event_time, details])
    state.update((hist.to_json(),))
    yield pd.DataFrame(out, columns=ALERT_COLS)


def build_alerts(txns):
    return (txns.withColumn("ts_ms", F.expr("unix_millis(to_timestamp(event_time))"))
            .groupBy("card_id")
            .applyInPandasWithState(detect, outputStructType=ALERT_SCHEMA, stateStructType="hist STRING",
                                    outputMode="append", timeoutConf=GroupStateTimeout.NoTimeout))


def upsert(df, batch_id):
    rows = [tuple(r) for r in df.collect()]
    new = []
    if rows:
        conn = psycopg2.connect(PG)
        try:
            with conn, conn.cursor() as cur:
                new = execute_values(cur, "INSERT INTO alerts (alert_id, rule, card_id, txn_id, event_time, details) "
                                          "VALUES %s ON CONFLICT (alert_id) DO NOTHING RETURNING alert_id",
                                     rows, fetch=True)
        finally:
            conn.close()
    dup = len(rows) - len(new)
    print(f"{time.strftime('%H:%M:%S')} Batch {batch_id}: {len(rows)} alerts"
          + (f" ({dup} already in Postgres, skipped)" if dup else ""))
    for r in rows:
        print(f"    ALERT {r[0]:<12} card {r[2]}  event {r[4][11:19]}  {r[5]}")


def main():
    scala = "2.13" if pyspark.__version__ >= "4" else "2.12"
    spark = (SparkSession.builder.appName("fraud-detector")
             .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{pyspark.__version__}")
             .config("spark.sql.shuffle.partitions", "6")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.addPyFile(os.path.join(os.path.dirname(os.path.abspath(__file__)), "rules.py"))

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", "transactions")
           .option("startingOffsets", "earliest")
           .load())
    txns = raw.select(F.from_json(F.col("value").cast("string"), TXN_SCHEMA).alias("t")).select("t.*")

    q = (build_alerts(txns).writeStream
         .foreachBatch(upsert)
         .option("checkpointLocation", CHECKPOINT)
         .trigger(processingTime="5 seconds")
         .start())

    while q.isActive:
        time.sleep(10)
        p = q.lastProgress
        if p:
            state_rows = p["stateOperators"][0]["numRowsTotal"] if p["stateOperators"] else 0
            print(f"{time.strftime('%H:%M:%S')} progress: batch {p['batchId']}, "
                  f"{p['numInputRows']} txns in last batch, cards in state {state_rows}")
    q.awaitTermination()


if __name__ == "__main__":
    main()

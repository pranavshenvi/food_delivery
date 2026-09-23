"""L4: python fraud_job.py   (rules.py must be in the same folder)
Streaming Spark: reads raw txns from `transactions`, detects fraud, republishes alerts onto
`flagged_transactions` for L1 to pick up as a batch job once the run is done."""
import time

import pandas as pd
import pyspark
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.streaming.state import GroupStateTimeout

from rules import CardHistory

BOOTSTRAP = "10.147.17.12:9092,10.147.17.13:9092"  # <-- ZeroTier IPs of L2, L3 VMs (the Kafka brokers)
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


def republish(df, batch_id):
    rows = df.collect()
    if rows:
        (df.select(F.col("card_id").cast("string").alias("key"),
                    F.to_json(F.struct(*ALERT_COLS)).alias("value"))
           .write.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("topic", "flagged_transactions")
           .save())
    print(f"{time.strftime('%H:%M:%S')} Batch {batch_id}: {len(rows)} alerts")
    for r in rows:
        print(f"    ALERT {r['alert_id']:<12} card {r['card_id']}  event {r['event_time'][11:19]}  {r['details']}")


def main():
    scala = "2.13" if pyspark.__version__ >= "4" else "2.12"
    spark = (SparkSession.builder.appName("fraud-detector")
             .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{pyspark.__version__}")
             .config("spark.sql.shuffle.partitions", "6")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.addPyFile("rules.py")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", "transactions")
           .option("startingOffsets", "earliest")
           .load())
    txns = raw.select(F.from_json(F.col("value").cast("string"), TXN_SCHEMA).alias("t")).select("t.*")

    q = (build_alerts(txns).writeStream
         .foreachBatch(republish)
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

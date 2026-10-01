"""VM4 -- Spark Job 1 (order enrichment): orders_landed.jsonl -> order_metrics.jsonl.
Usage: python spark_job1.py [--input orders_landed.jsonl] [--output order_metrics.jsonl]"""
import argparse
import glob
import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

ap = argparse.ArgumentParser()
ap.add_argument("--input", default="orders_landed.jsonl")
ap.add_argument("--output", default="order_metrics.jsonl")
a = ap.parse_args()

spark = (SparkSession.builder.master("local[*]").appName("job1-order-enrichment")
         .config("spark.hadoop.fs.defaultFS", "file:///")  # local filesystem only, never HDFS
         .config("spark.sql.ansi.enabled", "false")  # bad timestamps/numbers -> null instead of an exception
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

TS = ["order_timestamp", "accepted_timestamp", "picked_up_timestamp", "expected_delivery_timestamp", "delivered_timestamp"]
STR = ["order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city", "status"]
NUM = ["distance_km", "order_amount", "delivery_fee"]
schema = StructType([StructField(c, StringType()) for c in STR + TS] + [StructField(c, DoubleType()) for c in NUM])

raw = spark.read.text("file://" + os.path.abspath(a.input)).where(F.trim("value") != "")
# malformed JSON -> from_json returns a null struct -> dropped by the order_id check below
parsed = raw.select(F.from_json("value", schema).alias("r")).select("r.*").where(F.col("order_id").isNotNull())

# exact duplicates counted once
df = parsed.dropDuplicates()

for c in TS:
    df = df.withColumn(c, F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss"))

valid = F.lit(True)
for c in STR + TS + NUM:
    valid &= F.col(c).isNotNull()
for c in STR:
    valid &= F.trim(F.col(c)) != ""
df = df.where(valid & (F.col("distance_km") > 0) & (F.col("order_amount") > 0) & (F.col("status") == "DELIVERED"))


def minutes(end, start):
    return (F.unix_timestamp(end) - F.unix_timestamp(start)) / 60.0


df = (df
      .withColumn("preparation_time_min", minutes("picked_up_timestamp", "accepted_timestamp"))
      .withColumn("delivery_time_min", minutes("delivered_timestamp", "picked_up_timestamp"))
      .withColumn("fulfillment_time_min", minutes("delivered_timestamp", "order_timestamp"))
      .withColumn("delivery_speed_kmph", F.col("distance_km") / (F.col("delivery_time_min") / 60.0))
      .withColumn("delay_min", F.greatest(F.lit(0.0), minutes("delivered_timestamp", "expected_delivery_timestamp")))
      .withColumn("delivery_status", F.when(F.col("delay_min") == 0, "ON_TIME").otherwise("DELAYED"))
      .withColumn("delay_category",
                  F.when(F.col("delay_min") == 0, "ON_TIME")
                  .when(F.col("delay_min") <= 15, "SLIGHT_DELAY")
                  .when(F.col("delay_min") <= 30, "MODERATE_DELAY")
                  .otherwise("SEVERE_DELAY")))

for c in ["preparation_time_min", "delivery_time_min", "fulfillment_time_min", "delivery_speed_kmph", "delay_min"]:
    df = df.withColumn(c, F.round(c, 2))

out = df.select("order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city", "order_amount",
                "delivery_fee", "distance_km", "preparation_time_min", "delivery_time_min", "fulfillment_time_min",
                "delivery_speed_kmph", "delay_min", "delivery_status", "delay_category").orderBy("order_id")

tmp = a.output + ".tmp_dir"
shutil.rmtree(tmp, ignore_errors=True)
out.coalesce(1).write.json(tmp)
shutil.move(glob.glob(os.path.join(tmp, "part-*.json"))[0], a.output)
shutil.rmtree(tmp)
print(f"wrote {out.count()} records to {a.output}")
spark.stop()

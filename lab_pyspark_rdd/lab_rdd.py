import re
import time
from pathlib import Path
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Spark initialization
spark = SparkSession.builder.appName("lab-pyspark-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

data_dir = Path(__file__).resolve().parent

# 1. RDD creation and partitions
users_lines = sc.textFile(str(data_dir / "users.csv"))
orders_lines = sc.textFile(str(data_dir / "orders.csv"))

print("Users partitions:", users_lines.getNumPartitions())
print("Orders partitions:", orders_lines.getNumPartitions())
print("Repartitioned:", orders_lines.repartition(8).getNumPartitions())

# 2. Lazy evaluation and lineage
header = orders_lines.first()
orders_data = orders_lines.filter(
    lambda line: line != header and bool(line.strip())
)

print("RDD lineage:")
print(orders_data.toDebugString().decode())
print("Orders count:", orders_data.count())

# 3. Multiline addresses
print("Raw user lines:", users_lines.count())

uuid_pattern = re.compile(
    r"^[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"
)

users_records = users_lines.filter(
    lambda line: uuid_pattern.match(line) is not None
)
print("Valid user records:", users_records.count())

# 4. Manual parsing
def parse_order(line):
    fields = line.split(",")
    if len(fields) != 5:
        raise ValueError(f"Invalid order: {line}")
    order_id, user_id, date, quantity, product = fields
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }

orders_rdd = orders_data.map(parse_order).cache()
print("Parsed orders:", orders_rdd.count())

# 5. reduceByKey vs groupByKey
quantities = orders_rdd.map(
    lambda o: (o["user_id"], o["quantity"])
)

user_quantity = quantities.reduceByKey(add)
user_quantity_grouped = quantities.groupByKey().mapValues(sum)

print("reduceByKey:", user_quantity.take(3))
print("groupByKey:", user_quantity_grouped.take(3))

# 6. Manual deduplication
def most_recent(a, b):
    return a if a["date"] >= b["date"] else b

orders_dedup = (
    orders_rdd.map(lambda o: (o["order_id"], o))
    .reduceByKey(most_recent)
    .values()
)

print("Before dedup:", orders_rdd.count())
print("After dedup:", orders_dedup.count())

# 7. Joining orders and users
def parse_user(line):
    fields = line.split(",")
    return fields[0], fields[1]

users_kv = users_records.map(parse_user)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))

enriched = orders_kv.join(users_kv)
print("Joined sample:", enriched.take(3))

# EXERCISE 1: aggregateByKey and average quantity
stats = quantities.aggregateByKey(
    (0, 0),
    lambda acc, q: (acc[0] + 1, acc[1] + q),
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

averages = stats.mapValues(
    lambda x: x[1] / x[0] if x[0] else 0
)

print("Order count and quantity:", stats.take(3))
print("Average quantity per user:", averages.take(3))

# EXERCISE 2: missing user with leftOuterJoin
fake_order = {
    "order_id": "fake-order",
    "user_id": "unknown-user-id",
    "date": "2020-01-01",
    "quantity": 1,
    "product": "bread",
}

orders_with_fake = orders_kv.union(
    sc.parallelize([(fake_order["user_id"], fake_order)])
)

left_joined = orders_with_fake.leftOuterJoin(users_kv)
missing_users = left_joined.filter(lambda x: x[1][1] is None)

print("Orders without matching user:", missing_users.count())

# EXERCISE 3: DataFrame equivalent
df = (
    spark.read.option("header", True)
    .csv(str(data_dir / "orders.csv"))
    .withColumn("quantity", F.col("quantity").cast("int"))
)

df_result = df.groupBy("user_uuid").agg(
    F.sum("quantity").alias("total_quantity")
)

print("DataFrame result:")
df_result.show(5)
print("DataFrame execution plan:")
df_result.explain()

print("RDD lineage:")
print(user_quantity.toDebugString().decode())

# EXERCISE 4: cache performance comparison
def benchmark(label, rdd):
    start = time.perf_counter()
    result = rdd.count()
    elapsed = time.perf_counter() - start
    print(f"{label}: {result} records in {elapsed:.4f}s")

for use_cache in [False, True]:
    sample = orders_data.map(parse_order)
    if use_cache:
        sample = sample.cache()

    label = "Cached" if use_cache else "Uncached"

    for attempt in range(2):
        benchmark(f"{label} orders run {attempt + 1}", sample)
        totals = sample.map(
            lambda o: (o["user_id"], o["quantity"])
        ).reduceByKey(add)
        benchmark(f"{label} aggregation run {attempt + 1}", totals)

    if use_cache:
        sample.unpersist()

orders_rdd.unpersist()
spark.stop()

print("LAB PYSPARK RDD COMPLETED")

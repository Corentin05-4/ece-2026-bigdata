import os
from uuid import uuid4
from pyspark.sql import SparkSession

BUCKET = os.environ.get("LAB_BUCKET_NAME") or os.environ["KUBERNETES_NAMESPACE"]
ENDPOINT = os.environ["AWS_S3_ENDPOINT"].removeprefix("https://").removeprefix("http://").rstrip("/")
TOKEN = os.environ.get("AWS_SESSION_TOKEN", "")
PROVIDER = (
    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
    if TOKEN else "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

builder = (
    SparkSession.builder.appName("lab-iceberg-exploration").master("local[2]")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{ENDPOINT}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", PROVIDER)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
)

if TOKEN:
    builder = builder.config("spark.hadoop.fs.s3a.session.token", TOKEN)

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

ORDERS = "lab.silver.orders"
USERS = "lab.silver.users"

def latest_snapshot(table):
    row = spark.sql(f"""
        SELECT snapshot_id FROM {table}.snapshots
        ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1
    """).first()
    if row is None:
        raise RuntimeError(f"No Iceberg snapshot for {table}")
    return row["snapshot_id"]

def count_as_of(table, snapshot_id):
    return spark.sql(
        f"SELECT count(*) AS n FROM {table} VERSION AS OF {snapshot_id}"
    ).first()["n"]

try:
    # Create Iceberg tables if they do not already exist
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")

    for name in ("users", "orders"):
        table = f"lab.silver.{name}"
        if not spark.catalog.tableExists(table):
            parquet = f"s3a://{BUCKET}/silver/spark/{name}.parquet"
            spark.read.parquet(parquet).writeTo(table).create()
            print(f"Created {table}")

    # Explore the catalog and Iceberg metadata
    spark.sql("SHOW TABLES IN lab.silver").show(truncate=False)

    spark.sql(f"""
        SELECT count(*) AS orders, sum(quantity) AS total_quantity
        FROM {ORDERS}
    """).show()

    spark.sql(f"""
        SELECT snapshot_id, operation, committed_at
        FROM {ORDERS}.snapshots
    """).show(truncate=False)

    spark.sql(f"""
        SELECT file_format, record_count, file_size_in_bytes
        FROM {ORDERS}.files
    """).show(truncate=False)

    # Orders: append three records and test time travel
    orders_before = spark.table(ORDERS).count()
    orders_snapshot = latest_snapshot(ORDERS)
    orders_frame = spark.table(ORDERS)
    examples = orders_frame.limit(3).collect()

    if len(examples) != 3:
        raise RuntimeError("Expected at least 3 orders")

    new_orders = [
        {**r.asDict(), "order_id": str(uuid4())}
        for r in examples
    ]

    spark.createDataFrame(
        new_orders, orders_frame.schema
    ).writeTo(ORDERS).append()

    assert spark.table(ORDERS).count() == orders_before + 3
    assert count_as_of(ORDERS, orders_snapshot) == orders_before

    print("ORDERS TIME TRAVEL:", orders_before, "->", orders_before + 3)

    # Orders: schema evolution
    if "source" not in spark.table(ORDERS).columns:
        spark.sql(f"ALTER TABLE {ORDERS} ADD COLUMN source STRING")

    order_frame = spark.table(ORDERS)
    extra_order = order_frame.limit(1).first().asDict()
    extra_order.update(order_id=str(uuid4()), source="spark")

    spark.createDataFrame(
        [extra_order], order_frame.schema
    ).writeTo(ORDERS).append()

    assert spark.table(ORDERS).count() == orders_before + 4
    spark.sql(f"DESCRIBE {ORDERS}").show(truncate=False)

    # Exercise 1-3: users append and time travel
    users_before = spark.table(USERS).count()
    users_snapshot = latest_snapshot(USERS)
    users_frame = spark.table(USERS)
    examples = users_frame.limit(2).collect()

    if len(examples) != 2:
        raise RuntimeError("Expected at least 2 users")

    new_users = []
    for row in examples:
        data = row.asDict()
        suffix = uuid4().hex[:12]
        data["user_id"] = str(uuid4())
        data["username"] = f"iceberg_{suffix}"
        if "username_normalized" in data:
            data["username_normalized"] = data["username"]
        data["email"] = f"iceberg_{suffix}@example.test"
        new_users.append(data)

    spark.createDataFrame(
        new_users, users_frame.schema
    ).writeTo(USERS).append()

    assert spark.table(USERS).count() == users_before + 2
    assert count_as_of(USERS, users_snapshot) == users_before

    print("USERS TIME TRAVEL:", users_before, "->", users_before + 2)

    # Exercise 4: add country column and insert new user
    if "country" not in spark.table(USERS).columns:
        spark.sql(f"ALTER TABLE {USERS} ADD COLUMN country STRING")

    user_frame = spark.table(USERS)
    extra_user = user_frame.limit(1).first().asDict()
    suffix = uuid4().hex[:12]

    extra_user.update(
        user_id=str(uuid4()),
        username=f"iceberg_{suffix}",
        email=f"iceberg_{suffix}@example.test",
        country="US",
    )

    if "username_normalized" in extra_user:
        extra_user["username_normalized"] = extra_user["username"]

    spark.createDataFrame(
        [extra_user], user_frame.schema
    ).writeTo(USERS).append()

    assert spark.table(USERS).count() == users_before + 3
    spark.sql(f"DESCRIBE {USERS}").show(truncate=False)

    # Exercise 5: inspect snapshots for both tables
    for table in (ORDERS, USERS):
        print("SNAPSHOTS:", table)
        spark.sql(f"""
            SELECT snapshot_id, operation, committed_at
            FROM {table}.snapshots
            ORDER BY committed_at
        """).show(truncate=False)

    # Exercise 6: Parquet vs Iceberg
    parquet_count = spark.read.parquet(
        f"s3a://{BUCKET}/silver/spark/orders.parquet"
    ).count()

    iceberg_count = spark.table(ORDERS).count()

    print("PARQUET rows:", parquet_count)
    print("ICEBERG rows:", iceberg_count)

    print("LAB ICEBERG COMPLETED")

finally:
    spark.stop()

import json
import os
import re
import subprocess
import time
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# Environment and S3 credentials
BUCKET = os.environ.get("LAB_BUCKET_NAME") or os.environ["KUBERNETES_NAMESPACE"]
ENDPOINT = os.environ.get("AWS_S3_ENDPOINT", "s3.seaweedfs.adm.adaltas.cloud")
ENDPOINT = ENDPOINT.removeprefix("https://").removeprefix("http://").rstrip("/")

aws = {
    "AccessKeyId": os.environ["AWS_ACCESS_KEY_ID"],
    "SecretAccessKey": os.environ["AWS_SECRET_ACCESS_KEY"],
    "SessionToken": os.environ.get("AWS_SESSION_TOKEN", ""),
}

provider = (
    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
    if aws.get("SessionToken")
    else "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

builder = (
    SparkSession.builder.appName("lab-pyspark-silver").master("local[2]")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{ENDPOINT}")
    .config("spark.hadoop.fs.s3a.access.key", aws["AccessKeyId"])
    .config("spark.hadoop.fs.s3a.secret.key", aws["SecretAccessKey"])
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", provider)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
)

if aws.get("SessionToken"):
    builder = builder.config("spark.hadoop.fs.s3a.session.token", aws["SessionToken"])

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

try:
    # BRONZE: Read and cache CSV data
    users = spark.read.csv(
        f"s3a://{BUCKET}/bronze/users.csv",
        header=True, inferSchema=True, multiLine=True
    ).cache()

    orders = spark.read.csv(
        f"s3a://{BUCKET}/bronze/orders.csv",
        header=True, inferSchema=True, multiLine=True
    ).cache()

    print("BRONZE users/orders:", users.count(), orders.count())

    # SILVER: Clean orders and remove duplicates
    orders_typed = orders.select(
        F.col("uuid").cast("string").alias("order_id"),
        F.col("user_uuid").cast("string").alias("user_id"),
        F.col("date").cast("timestamp").alias("ordered_at"),
        F.col("quantity").cast("int").alias("quantity"),
        F.lower(F.trim("product")).alias("product"),
    )

    duplicated = orders_typed.unionByName(orders_typed.limit(3))
    window = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())

    stg_orders = (
        duplicated.withColumn("rn", F.row_number().over(window))
        .where("rn = 1")
        .drop("rn")
    )

    assert stg_orders.count() == orders_typed.count()
    print("DEDUP:", duplicated.count(), "->", stg_orders.count())

    # Equivalent Spark SQL transformation
    orders_typed.createOrReplaceTempView("orders_typed")

    sql_orders = spark.sql("""
        SELECT order_id, user_id, ordered_at, quantity, product
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY order_id ORDER BY ordered_at DESC
                   ) AS rn
            FROM orders_typed
        )
        WHERE rn = 1
    """)

    assert sql_orders.count() == stg_orders.count()
    stg_orders.createOrReplaceTempView("stg_orders")

    # SILVER: Clean users and addresses
    address = F.split("address", "\n")

    user_base = users.select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("raw_birthdate"),
        F.element_at(address, 1).alias("street"),
        F.element_at(address, 2).alias("line2"),
    ).select(
        "user_id", "username", "name", "sex", "email",
        "raw_birthdate", "street",
        F.when(
            F.regexp_extract("line2", r"^(.+), [A-Z]{2} [0-9]{5}$", 1) != "",
            F.regexp_extract("line2", r"^(.+), [A-Z]{2} [0-9]{5}$", 1)
        ).alias("city"),
        F.regexp_extract("line2", r"([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
        F.regexp_extract("line2", r"([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
    )

    # EXERCISE 1: Birthdate validation
    first = stg_orders.groupBy("user_id").agg(
        F.min(F.to_date("ordered_at")).alias("first_date")
    )

    joined = user_base.join(first, "user_id", "left")

    invalid = F.col("raw_birthdate").isNull() | (
        F.col("first_date").isNotNull()
        & (F.col("raw_birthdate") > F.col("first_date"))
    )

    stg_users = (
        joined.withColumn("birthdate_is_valid", ~invalid)
        .withColumn("birthdate", F.when(~invalid, F.col("raw_birthdate")))
        .withColumn("username_normalized", F.lower(F.trim("username")))
        .select(
            "user_id", "username", "username_normalized", "name",
            "sex", "email", "birthdate", "birthdate_is_valid",
            "street", "city", "state", "zip_code"
        )
    )

    user_base.createOrReplaceTempView("user_base")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW first_orders AS
        SELECT user_id, MIN(TO_DATE(ordered_at)) AS first_date
        FROM stg_orders
        GROUP BY user_id
    """)

    sql_users = spark.sql("""
        SELECT u.user_id, u.username,
               LOWER(TRIM(u.username)) AS username_normalized,
               u.name, u.sex, u.email,
               CASE
                 WHEN u.raw_birthdate IS NULL OR
                     (f.first_date IS NOT NULL AND
                      u.raw_birthdate > f.first_date)
                 THEN CAST(NULL AS DATE)
                 ELSE u.raw_birthdate
               END AS birthdate,
               CASE
                 WHEN u.raw_birthdate IS NULL OR
                     (f.first_date IS NOT NULL AND
                      u.raw_birthdate > f.first_date)
                 THEN false ELSE true
               END AS birthdate_is_valid,
               u.street, u.city, u.state, u.zip_code
        FROM user_base u
        LEFT JOIN first_orders f ON u.user_id = f.user_id
    """)

    assert stg_users.exceptAll(sql_users).count() == 0
    assert sql_users.exceptAll(stg_users).count() == 0
    print("EX1: DataFrame and SQL results agree")

    # EXERCISE 2: Native regex vs Python UDF vs pandas UDF
    from pyspark.sql.functions import pandas_udf, udf
    import pandas as pd

    pattern = re.compile(r"[A-Z]{2} ([0-9]{5})$")

    def extract_zip(value):
        match = pattern.search(value or "")
        return match.group(1) if match else ""

    python_udf = udf(extract_zip, StringType())

    @pandas_udf(StringType())
    def vectorized_zip(series: pd.Series) -> pd.Series:
        return series.fillna("").str.extract(
            r"[A-Z]{2} ([0-9]{5})$", expand=False
        ).fillna("")

    expressions = {
        "native": F.regexp_extract("address", r"[A-Z]{2} ([0-9]{5})$", 1),
        "python_udf": python_udf("address"),
        "pandas_udf": vectorized_zip("address"),
    }

    for label, expression in expressions.items():
        start = time.perf_counter()
        checksum = (
            users.select(expression.alias("zip"))
            .select(F.sum(F.length("zip")))
            .first()[0]
        )
        print(
            f"EX2 {label}: {time.perf_counter() - start:.3f}s, "
            f"checksum={checksum}"
        )

    # EXERCISE 3: Cache observations
    for label, frame in (("users", users), ("orders", orders)):
        start = time.perf_counter()
        count = frame.count()
        print(f"EX3 cached {label}: {count} rows in {time.perf_counter() - start:.3f}s")

    # PARQUET: Export and validation
    u_parquet = f"s3a://{BUCKET}/silver/spark/users.parquet"
    o_parquet = f"s3a://{BUCKET}/silver/spark/orders.parquet"

    stg_users.write.mode("overwrite").parquet(u_parquet)
    stg_orders.write.mode("overwrite").parquet(o_parquet)

    assert spark.read.parquet(u_parquet).count() == users.count()
    assert spark.read.parquet(o_parquet).count() == stg_orders.count()
    print("PARQUET validated")

    # ICEBERG: Create tables
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")

    stg_users.writeTo("lab.silver.users").createOrReplace()
    stg_orders.writeTo("lab.silver.orders").createOrReplace()

    assert spark.table("lab.silver.users").count() == users.count()
    assert spark.table("lab.silver.orders").count() == stg_orders.count()

    spark.sql("""
        SELECT snapshot_id, operation
        FROM lab.silver.orders.snapshots
    """).show(truncate=False)

    spark.sql("""
        SELECT file_format, record_count
        FROM lab.silver.orders.files
    """).show(truncate=False)

    print("ICEBERG validated")

    # EXERCISE 4: Iceberg append and time travel
    before = spark.table("lab.silver.orders").count()

    old_snapshot = spark.sql("""
        SELECT snapshot_id
        FROM lab.silver.orders.snapshots
        ORDER BY committed_at DESC LIMIT 1
    """).first()[0]

    example = stg_orders.first()

    fake = spark.createDataFrame(
        [(
            str(uuid.uuid4()),
            example["user_id"],
            datetime.now(timezone.utc),
            1,
            "bread",
        )],
        stg_orders.schema,
    )

    fake.writeTo("lab.silver.orders").append()

    assert spark.table("lab.silver.orders").count() == before + 1

    old_count = spark.sql(
        f"SELECT COUNT(*) AS n FROM lab.silver.orders "
        f"VERSION AS OF {old_snapshot}"
    ).first()[0]

    assert old_count == before

    print("EX4 Iceberg current / historical:", before + 1, old_count)

    fake.write.mode("append").parquet(o_parquet)
    print(
        "EX4 Parquet after append:",
        spark.read.parquet(o_parquet).count(),
        "(no time travel)"
    )

    stg_orders.write.mode("overwrite").parquet(o_parquet)

    print("LAB PYSPARK SILVER COMPLETED")

finally:
    spark.stop()

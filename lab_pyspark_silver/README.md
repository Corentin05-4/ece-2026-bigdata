# Lab - Bronze to Silver with PySpark

## Overview

Implementation of a Bronze-to-Silver data pipeline using Apache Spark,
Spark SQL, S3, Parquet and Apache Iceberg.

The objective is to transform raw datasets into cleaned, typed and
deduplicated data suitable for analytics.

## Technologies

- Python
- PySpark DataFrame API
- Spark SQL
- S3 Object Storage
- Apache Parquet
- Apache Iceberg

## Architecture

S3 Bronze CSV
    |
    v
PySpark DataFrames / Spark SQL
    |
    +-- Data cleaning
    +-- Type conversions
    +-- Order deduplication
    +-- Data validation
    |
    v
Silver Layer
    |
    +-- Parquet files on S3
    +-- Iceberg tables on S3

## Implementation

The `lab_silver.py` script includes:

1. Reading Bronze CSV datasets directly from S3
2. Cleaning and transforming users and orders
3. Deduplicating orders with window functions
4. Implementing equivalent transformations with Spark SQL
5. Writing Silver datasets as Parquet
6. Creating Apache Iceberg tables
7. Exploring Iceberg snapshots and metadata

## Exercises

- Exercise 1: Validate birthdates using DataFrames and Spark SQL.
- Exercise 2: Compare regular expressions, Python UDFs and pandas UDFs.
- Exercise 3: Analyze Spark caching performance.
- Exercise 4: Demonstrate Iceberg append operations and time travel.

## Execution

The lab requires an Onyxia PySpark environment with 4 GiB of memory
and valid S3 credentials.

```bash
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"

spark-submit \
  --master "local[2]" \
  --driver-memory 1g \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.default.parallelism=2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  lab_pyspark_silver/lab_silver.py
```

## Iceberg Metadata

The `warehouse/` directory contains the metadata collected from
the Iceberg tables:

- silver/users/metadata/
- silver/orders/metadata/

These files include table metadata, manifests and snapshot information.

The actual table data remains stored on S3 and is not committed to GitHub.

## Results

The pipeline was successfully executed on Onyxia after increasing
the service memory allocation.

The lab implements Bronze-to-Silver transformations and demonstrates
the differences between Parquet files and Iceberg tables.

## Conclusion

PySpark provides distributed processing capabilities for large datasets.

Parquet is an efficient columnar storage format, while Apache Iceberg
adds table metadata, snapshots and time-travel capabilities.

This lab complements the previous dbt/DuckDB implementation of
the Bronze-to-Silver pipeline.

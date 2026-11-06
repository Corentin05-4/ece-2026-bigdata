# Lab - Explore Iceberg Tables with Spark

## Objectives

Explore Apache Iceberg tables using PySpark, including metadata,
snapshots, time travel, schema evolution and comparison with Parquet.

## Technologies

- Apache Spark / PySpark
- Apache Iceberg
- Spark SQL
- S3 Object Storage
- Apache Parquet

## Implementation

The `lab_iceberg.py` script implements:

1. Iceberg catalog configuration
2. Table creation and inspection
3. Metadata and snapshot exploration
4. Appending new orders
5. Time travel to previous table versions
6. Schema evolution using ALTER TABLE

## Exercises

- Create or reuse the Iceberg users table.
- Append a new batch of users.
- Compare current and historical snapshots.
- Add a country column and insert new data.
- Inspect the S3 layout of users and orders.
- Compare Parquet, Iceberg and the Iceberg catalog.

## Parquet vs Iceberg vs Catalog

Parquet is a columnar file format that stores data.

Iceberg is a table format built on data files and metadata.
It supports snapshots, schema evolution and time travel.

An Iceberg catalog resolves table names and coordinates access
to the current table metadata.

## Execution

```bash
spark-submit --master "local[2]" \
  --driver-memory 1g \
  --conf spark.sql.shuffle.partitions=4 \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  lab_iceberg/lab_iceberg.py
```

## Notes

The lab reuses the S3 warehouse from the PySpark Silver lab.
Running the script again creates additional snapshots.

The warehouse is preserved because previous labs depend on it.
Iceberg metadata can be exported separately to GitHub.

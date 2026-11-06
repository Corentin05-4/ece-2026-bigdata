# Lab - Introduction to PySpark RDD API

## Overview

Introduction to distributed data processing using Apache Spark's Resilient Distributed Datasets (RDDs).

The lab processes user and order datasets from the Bronze layer.

## Technologies

- Python
- Apache Spark / PySpark
- RDD API
- S3 Object Storage

## Implementation

The `lab_rdd.py` script covers:

1. RDD creation and partitioning
2. Lazy evaluation and lineage
3. Manual CSV parsing
4. reduceByKey vs groupByKey
5. Order deduplication
6. Joining users and orders

## Exercises

- Exercise 1: Calculate order statistics using aggregateByKey.
- Exercise 2: Detect missing users using leftOuterJoin.
- Exercise 3: Compare RDD and DataFrame APIs.
- Exercise 4: Benchmark cached and uncached RDDs.

## Execution

```bash
spark-submit --master "local[*]" lab_pyspark_rdd/lab_rdd.py
```

The Bronze CSV datasets must be available locally in the lab directory.

## Results

Successfully executed on Onyxia.

- 2,829 orders processed
- 50 users identified
- Order deduplication performed
- 1 deliberately injected unmatched order detected
- All four exercises executed successfully

## Conclusion

The RDD API provides low-level control over distributed data
processing. DataFrames offer a more concise and optimized approach
for structured data operations.

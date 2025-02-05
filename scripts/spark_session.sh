spark-shell \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,\
software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3,\
software.amazon.awssdk:kms:2.20.40,\
software.amazon.awssdk:sts:2.20.40,\
software.amazon.awssdk:s3:2.20.40,\
software.amazon.awssdk:glue:2.20.40,\
software.amazon.awssdk:dynamodb:2.20.40 \
  --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog \
  --conf spark.sql.catalog.s3tablesbucket.warehouse=PASTE YOUR TABLE BUCKET ARN HERE! \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

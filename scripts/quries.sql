spark.sql("DESCRIBE TABLE s3tablesbucket.supermarket_data.sales").show()

spark.sql("DESCRIBE FORMATTED s3tablesbucket.supermarket_data.sales").show()

spark.sql("SELECT * FROM s3tablesbucket.supermarket_data.sales").show(5)

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.supermarket_data")

spark.sql("SHOW NAMESPACES IN s3tablesbucket").show()

spark.sql("""
  SELECT Branch, AVG(Total) AS avg_total_per_branch
  FROM s3tablesbucket.supermarket_data.sales
  GROUP BY Branch
""").show()

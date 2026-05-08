apiVersion: batch/v1
kind: Job
metadata:
  name: spark-iceberg-verify
  namespace: gdelt
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: spark-verify
          image: juxpkr/gdelt-spark:spark3.5.6-iceberg1.10.1-awsbundle-s3a-java17-arm64
          imagePullPolicy: IfNotPresent
          env:
            - name: MINIO_ENDPOINT
              value: "http://minio.gdelt.svc.cluster.local:9000"
            - name: MINIO_ROOT_USER
              value: "minioadmin"
            - name: MINIO_ROOT_PASSWORD
              value: "minioadmin"
            - name: NESSIE_URI
              value: "http://nessie.gdelt.svc.cluster.local:19120/iceberg"
          command: ["python"]
          args:
            - "-c"
            - |
              import os
              from pyspark.sql import SparkSession

              spark = (
                  SparkSession.builder
                  .appName("iceberg-verify")
                  .master("local[1]")
                  .config("spark.driver.memory", "2g")
                  .config("spark.jars", os.environ["SPARK_JARS"])

                  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

                  .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
                  .config("spark.sql.catalog.nessie.type", "rest")
                  .config("spark.sql.catalog.nessie.uri", os.environ["NESSIE_URI"])
                  .config("spark.sql.catalog.nessie.warehouse", "warehouse")

                  .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"])
                  .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
                  .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
                  .config("spark.hadoop.fs.s3a.path.style.access", "true")
                  .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                  .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                  .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                  .getOrCreate()
              )

              spark.sparkContext.setLogLevel("WARN")

              print("=== CATALOGS ===", flush=True)
              spark.sql("SHOW CATALOGS").show(truncate=False)

              print("=== NAMESPACES ===", flush=True)
              spark.sql("SHOW NAMESPACES IN nessie").show(truncate=False)

              print("=== TABLES IN BRONZE ===", flush=True)
              spark.sql("SHOW TABLES IN nessie.bronze").show(truncate=False)

              for table in ["gdelt_events", "gdelt_mentions", "gdelt_gkg"]:
                  full = f"nessie.bronze.{table}"

                  print(f"\\n=== {full}: schema ===", flush=True)
                  spark.sql(f"DESCRIBE TABLE {full}").show(100, truncate=False)

                  print(f"\\n=== {full}: count ===", flush=True)
                  spark.sql(f"SELECT COUNT(*) AS cnt FROM {full}").show(truncate=False)

                  print(f"\\n=== {full}: sample ===", flush=True)
                  spark.sql(f"SELECT * FROM {full} LIMIT 5").show(truncate=False)

                  print(f"\\n=== {full}: iceberg metadata tables ===", flush=True)
                  spark.sql(f"SELECT * FROM {full}.files LIMIT 10").show(truncate=False)

              spark.stop()
              print("=== VERIFY DONE ===", flush=True)
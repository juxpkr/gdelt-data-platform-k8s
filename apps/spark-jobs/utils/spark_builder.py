import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str, master: str = "local[2]", extra_packages: str | None = None) -> SparkSession:
    if not os.getenv("MINIO_ENDPOINT"):
        raise ValueError("MINIO_ENDPOINT 환경변수가 없습니다")

    nessie_uri = os.getenv("NESSIE_URI", "http://nessie.gdelt.svc.cluster.local:19120/iceberg")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.jars", os.environ["SPARK_JARS"])
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        # Iceberg extensions
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

        # Nessie REST catalog
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.type", "rest")
        .config("spark.sql.catalog.nessie.uri", nessie_uri)
        .config("spark.sql.catalog.nessie.warehouse", "warehouse")
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")

        # S3A / MinIO
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.session.timeZone", "UTC")
    )

    if extra_packages:
        builder = builder.config("spark.jars.packages", extra_packages)

    return builder.getOrCreate()

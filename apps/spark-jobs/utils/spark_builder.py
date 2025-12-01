import os
from pyspark.sql import SparkSession

def get_spark_session(app_name: str, master: str = None) -> SparkSession:
    """
    [Optimized for K8s 16GB Environment]
    - JAR 의존성 자동 해결 (Docker Image Classpath 활용)
    - MinIO I/O 최적화 (SSL Off, Fast Upload)
    - AQE 활성화 (메모리 효율성 증대)
    """
    # 환경변수 체크
    if not os.getenv("MINIO_ENDPOINT"):
        raise ValueError("'MINIO_ENDPOINT' 환경변수가 없습니다. values.yaml을 확인하세요")

    builder = (
        SparkSession.builder.appName(app_name)
        
        # MinIO (S3) 성능 최적화
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # 내부 통신이므로 SSL 꺼서 오버헤드 제거
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # 대용량 파일 업로드 속도 향상, S3A 커넥션 최적화 (대용량 I/O 안정성)
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100MB
        .config("spark.hadoop.fs.s3a.directory.marker.retention", "keep")
        .config("spark.hadoop.fs.s3a.committer.name", "directory") 
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")

        # Delta Lake 설정
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # Hive Metastore (Embedded Postgres)
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", "") # 임베디드 모드 강제
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", "jdbc:postgresql://postgres-custom:5432/metastore_db")
        .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")
        .config("spark.hadoop.javax.jdo.option.ConnectionUserName", "postgres")
        .config("spark.hadoop.javax.jdo.option.ConnectionPassword", "postgres")
        
        # DataNucleus 안정성 설정 (테이블/컬럼 자동 생성)
        .config("spark.hadoop.datanucleus.schema.autoCreateAll", "true")
        .config("spark.hadoop.datanucleus.schema.autoCreateTables", "true")
        .config("spark.hadoop.datanucleus.schema.autoCreateColumns", "true")
        .config("spark.hadoop.hive.metastore.schema.verification", "false")

        # 16GB에 맞게 튜닝 (AQE)
        # Spark가 실행 중에 데이터 크기를 보고 알아서 최적화함
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        # 기타 설정
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.session.timeZone", "UTC")
        .enableHiveSupport()
    )

    if master:
        builder = builder.master(master)

    # 로그 레벨 조정 (WARN)
    spark = builder.getOrCreate()
    #spark.sparkContext.setLogLevel("WARN")
    
    return spark
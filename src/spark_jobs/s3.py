import os

from pyspark.sql import SparkSession


def configure_s3(spark: SparkSession) -> None:
    """Point the S3A connector at MinIO locally, or real S3 when no endpoint is set."""
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    endpoint = os.getenv("S3_ENDPOINT", "")

    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.fast.upload", "true")

    if endpoint:
        hadoop_conf.set("fs.s3a.endpoint", endpoint)
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
        hadoop_conf.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        hadoop_conf.set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", ""))
        hadoop_conf.set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
    else:
        hadoop_conf.set(
            "fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )


def s3_path(layer: str, dataset: str) -> str:
    bucket = os.getenv("S3_BUCKET", "crypto-lake")
    return f"s3a://{bucket}/{layer}/{dataset}"

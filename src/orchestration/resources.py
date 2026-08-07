import os
from dataclasses import dataclass

import duckdb
from dagster import ConfigurableResource


@dataclass(frozen=True)
class LakeLayout:
    bucket: str
    endpoint: str


class LakeResource(ConfigurableResource):
    """DuckDB connection pre-configured to read and write the object store."""

    bucket: str = os.getenv("S3_BUCKET", "crypto-lake")
    endpoint: str = os.getenv("S3_ENDPOINT", "")
    access_key: str = os.getenv("AWS_ACCESS_KEY_ID", "")
    secret_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    region: str = os.getenv("AWS_REGION", "eu-west-3")

    def connect(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"SET s3_region='{self.region}';")

        if self.endpoint:
            host = self.endpoint.replace("http://", "").replace("https://", "")
            con.execute(f"SET s3_endpoint='{host}';")
            con.execute("SET s3_use_ssl=false;")
            con.execute("SET s3_url_style='path';")

        if self.access_key:
            con.execute(f"SET s3_access_key_id='{self.access_key}';")
            con.execute(f"SET s3_secret_access_key='{self.secret_key}';")

        return con

    def path(self, layer: str, dataset: str) -> str:
        return f"s3://{self.bucket}/{layer}/{dataset}"

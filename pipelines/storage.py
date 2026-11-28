import os
import logging
import boto3

from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from pathlib import Path

load_dotenv()

logger = logging.getLogger(__name__)


class StorageClient:

    def __init__(self):

        self.client = boto3.client(
            "s3",
            endpoint_url=os.environ["S3_ENDPOINT_URL"],
            aws_access_key_id=os.environ["S3_ACCESS_KEY"],
            aws_secret_access_key=os.environ["S3_SECRET_KEY"],
            region_name=os.getenv("S3_REGION", "us-east-1"),
            config=Config(
                s3={"addressing_style": "path"}
            )
        )

    def list_buckets(self) -> list[str]:

        response = self.client.list_buckets()

        return [
            bucket["Name"]
            for bucket in response.get("Buckets", [])
        ]


    def ensure_bucket_exists(self, bucket_name: str) -> None:

        try:
            self.client.head_bucket(Bucket=bucket_name)

            logger.info(f"Bucket existente: {bucket_name}")

        except ClientError as e:

            error_code = e.response["Error"]["Code"]

            if error_code in ("404", "NoSuchBucket"):

                self.client.create_bucket(Bucket=bucket_name)

                logger.info(f"Bucket criado: {bucket_name}")

            else:
                raise



    def list_objects(self, bucket_name: str, prefix: str = "") -> list[str]:

        paginator = self.client.get_paginator("list_objects_v2")

        pages = paginator.paginate(
            Bucket=bucket_name,
            Prefix=prefix
        )

        objects = [
            obj["Key"]
            for page in pages
            for obj in page.get("Contents", [])
        ]

        logger.info(f"Objetos encontrados: {len(objects)}")

        return objects


    def upload_file(self, bucket_name: str, object_name: str, file_path: str) -> None:

        if not Path(file_path).is_file():
            raise FileNotFoundError(f"Arquivo local não encontrado: {file_path}")

        self.client.upload_file(
            file_path,
            bucket_name,
            object_name
        )

        logger.info(f"Arquivo enviado: s3://{bucket_name}/{object_name}")

    def download_file(self, bucket_name: str, object_name: str, file_path: str) -> str:

        path = Path(file_path)

        path.parent.mkdir(parents=True, exist_ok=True)

        self.client.download_file(
            bucket_name,
            object_name,
            file_path
        )

        logger.info(f"Arquivo recuperado: {file_path}")

        return file_path
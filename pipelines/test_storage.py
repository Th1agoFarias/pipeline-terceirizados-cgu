import logging

from pathlib import Path
from pipelines.storage import StorageClient


logging.basicConfig(level=logging.INFO)

client = StorageClient()

bucket_name = "terceirizados"

client.ensure_bucket_exists(bucket_name)

objetos_existentes = set(
    client.list_objects(bucket_name, prefix="raw/")
)

for file_path in sorted(Path("data/raw").glob("*.parquet")):

    object_name = f"raw/{file_path.name}"

    if object_name in objetos_existentes:

        logging.info(f"Arquivo já existente: {object_name}")

        continue

    client.upload_file(
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=str(file_path)
    )
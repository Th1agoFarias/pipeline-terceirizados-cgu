import pandas as pd

from pathlib import Path
from prefect import task, get_run_logger
from prefect.cache_policies import NO_CACHE

from pipelines.constants import URL, BUCKET_NAME, RAW_PREFIX
from pipelines.scraper import ScrapingClient
from pipelines.storage import StorageClient


@task(name="Get links")
def get_links() -> dict:
    client = ScrapingClient(URL)
    
    return client.get_links()


@task(name="Extract Data")
def extract_data(url: str) -> pd.DataFrame:

    client = ScrapingClient(URL)
    content = client.download_data(url)
    df = client.read_file(content, url)

    return df


@task(name="Save Parquet")
def save_parquet(df: pd.DataFrame, ano: str, mes: str) -> Path:

    logger = get_run_logger()

    output_dir = Path("data/raw")

    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / f"terceirizados_{ano}-{mes}.parquet"

    df.to_parquet(
        file_path,
        engine="pyarrow",
        index=False
    )

    logger.info(f"Arquivo Parquet salvo: {file_path}")

    return file_path


@task(name="Checagem de arquivo existente")
def is_processed(ano: str, mes:str)->bool:
    file_path = Path(
        f"data/raw/terceirizados_{ano}-{mes}.parquet"
    )

    return file_path.is_file()


@task(name="Ensure Bucket", cache_policy=NO_CACHE)
def ensure_bucket() -> None:

    client = StorageClient()

    client.ensure_bucket_exists(BUCKET_NAME)


@task(name="List Objects", cache_policy=NO_CACHE)
def list_objects() -> set[str]:

    client = StorageClient()

    objects = client.list_objects(bucket_name=BUCKET_NAME, prefix=RAW_PREFIX)

    return set(objects)


@task(name="Upload Parquet", cache_policy=NO_CACHE)
def upload_parquet(file_path: Path) -> None:

    client = StorageClient()

    object_name = f"{RAW_PREFIX}{file_path.name}"

    client.upload_file(bucket_name=BUCKET_NAME, object_name=object_name, file_path=str(file_path))

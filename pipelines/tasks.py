import pandas as pd

from pathlib import Path
from prefect import task, get_run_logger

from pipelines.constants import URL
from pipelines.scraper import ScrapingClient



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
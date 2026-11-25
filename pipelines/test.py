import logging

from pipelines.constants import URL
from pipelines.scraper import ScrapingClient
from pipelines.tasks import save_parquet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


client = ScrapingClient(URL)

dados = client.get_links()

for ano in ["2023", "2025"]:

    url = dados[ano]["Janeiro"]

    content = client.download_data(url)

    df = client.read_file(content, url)

    save_parquet(df, ano, "01")
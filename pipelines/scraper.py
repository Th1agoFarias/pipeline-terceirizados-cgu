import re
import time
import io
import logging
import requests
import pandas as pd

from tqdm import tqdm
from bs4 import BeautifulSoup
from urllib.parse import urljoin


logger = logging.getLogger(__name__)


class ScrapingClient:

    def __init__(self, url: str):
        self.url = url

    def get_links(self) -> dict:

        logger.info("Iniciando mapeamento dos arquivos.")

        response = requests.get(self.url, timeout=60)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        dados = {}

        for h3 in soup.find_all("h3"):
            texto = h3.get_text(strip=True)

            if re.match(r"^\d{4}$", texto):
                ano = texto
                dados[ano] = {}

                ul = h3.find_next_sibling("ul")

                if ul:
                    for a in ul.find_all("a", href=True):
                        mes = a.get_text(strip=True).title()

                        if not mes:
                            continue

                        link = urljoin(self.url, a["href"])
                        dados[ano][mes] = link

        logger.info(f"Mapeamento concluído. Anos: {list(dados.keys())}")

        return dados

    def download_data(self, url: str, max_retries: int = 5) -> bytes:

        for attempt in range(1, max_retries + 1):

            try:
                logger.info(
                    f"Iniciando download. Tentativa {attempt}/{max_retries}"
                )

                with requests.get(
                    url,
                    stream=True,
                    timeout=120,
                    headers={"Accept-Encoding": "identity"}
                ) as response:

                    response.raise_for_status()

                    total = int(response.headers.get("Content-Length", 0))

                    with io.BytesIO() as content:

                        with tqdm(
                            total=total or None,
                            unit="B",
                            unit_scale=True,
                            desc="Download"
                        ) as progress:

                            for chunk in response.iter_content(chunk_size=8192):

                                if chunk:
                                    content.write(chunk)
                                    progress.update(len(chunk))

                        logger.info("Download concluído com sucesso.")

                        return content.getvalue()

            except requests.exceptions.RequestException as e:

                logger.warning(
                    f"Tentativa {attempt}/{max_retries}: {e}"
                )

                if attempt == max_retries:
                    logger.error("Todas as tentativas falharam.")
                    raise

                time.sleep(2 ** attempt)

    def read_file(self, content: bytes, url: str) -> pd.DataFrame:

        if url.lower().endswith(".xlsx"):

            df = pd.read_excel(
                io.BytesIO(content),
                engine="openpyxl",
                dtype=str
            )

        elif url.lower().endswith(".csv"):

            df = pd.read_csv(
                io.BytesIO(content),
                sep=";",
                encoding="latin-1",
                dtype=str
            )

        else:
            raise ValueError("Formato de arquivo não suportado")

        return df


    
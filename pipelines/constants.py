import os

from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = os.environ["S3_BUCKET"]
RAW_PREFIX = "raw/"

URL = "https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados"

MES_MAP = {
    "Janeiro": "01",
    "Maio": "05",
    "Setembro": "09"
}





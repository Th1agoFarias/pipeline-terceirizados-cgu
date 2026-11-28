from pathlib import Path
from prefect import flow, get_run_logger

from pipelines.constants import MES_MAP, RAW_PREFIX

from pipelines.tasks import (
    get_links,
    extract_data,
    save_parquet,
    is_processed,
    ensure_bucket,
    list_objects,
    upload_parquet
)


@flow(name="pipeline-raw-terceirizados", log_prints=True)
def raw_terceirizados_flow():

    logger = get_run_logger()

    ensure_bucket()

    objetos_existentes = list_objects()

    dados = get_links()

    for ano, periodos in sorted(dados.items()):

        for mes, url in periodos.items():

            if mes not in MES_MAP:
                continue

            if not url.lower().endswith((".csv", ".xlsx")):
                logger.warning(f"Formato não suportado: {ano}/{mes}")
                continue

            mes_num = MES_MAP[mes]

            filename = f"terceirizados_{ano}-{mes_num}.parquet"

            object_name = f"{RAW_PREFIX}{filename}"

            # Verifica se existe no RustFS
            if object_name in objetos_existentes:
                logger.info(f"Objeto já processado: {object_name}")
                continue

            # Verifica se existe localmente
            if is_processed(ano, mes_num):

                file_path = Path("data/raw") / filename

                logger.info(f"Reaproveitando arquivo local: {filename}")

            else:

                logger.info(f"Processando: {ano}/{mes}")

                df = extract_data(url)

                file_path = save_parquet(df, ano, mes_num)

            upload_parquet(file_path)

            objetos_existentes.add(object_name)

    logger.info("Pipeline de ingestão concluído.")


if __name__ == "__main__":

    raw_terceirizados_flow()
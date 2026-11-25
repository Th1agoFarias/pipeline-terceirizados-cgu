from prefect import flow, get_run_logger

from pipelines.constants import MES_MAP

from pipelines.tasks import (
    get_links,
    extract_data,
    save_parquet,
    is_processed
)


@flow(name="pipeline-raw-terceirizados", log_prints=True)
def raw_terceirizados_flow():

    logger = get_run_logger()

    dados = get_links()

    for ano, periodos in sorted(dados.items()):

        for mes, url in periodos.items():

            if mes not in MES_MAP:
                continue

            if not url.lower().endswith((".csv", ".xlsx")):

                logger.warning(
                    f"Formato não suportado: {ano}/{mes}"
                )

                continue

            if is_processed(ano, MES_MAP[mes]):

                logger.info(f"Arquivo já processado: {ano}/{mes}")

                continue

            logger.info(f"Processando: {ano}/{mes}")

            df = extract_data(url)

            save_parquet(df, ano, MES_MAP[mes])

    logger.info("Pipeline de ingestão concluído.")


if __name__ == "__main__":

    raw_terceirizados_flow()
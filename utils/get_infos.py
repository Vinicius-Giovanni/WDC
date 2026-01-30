"""
Módulo para a requisiçã de dados essenciais
"""

from pathlib import Path
from typing import Optional
from datetime import datetime
import pyarrow.parquet as pd
import pyarrow.compute as pc
from utils.config_logger import log_with_context
from config.pipeline_config import logger

@log_with_context(job='penultimate_date', logger=logger)
def penultimate_date(
    parquet_file: Path,
    column: str = 'data_criterio',
    format: str = '%d/%m/%Y'
) -> Optional[str]:
    """
    Retorna a penultima data distinta do banco de dados

    params:
    parquet_file: Path | Recebe o path do parquet
    column: str = 'data_criterio' | Recebe o nome da coluna que deve ser lida. Retorna por padrão 'data_criterio'
    format: str = '%d/%m/%Y | Retorna o formato da data que foi pega do banco
    """

    if not parquet_file.exists():
        logger.critical(
            'arquivo parquet nao encontrado ao fazer a requisicao da penultima data catalogada no banco'
        )
        return None

    max_date: Optional[datetime]=None
    second_max_date: Optional[datetime]=None

    try:
        parquet = pd.ParquetFile(parquet_file)

        for batch in parquet.iter_batches(columns=[column]):
            array = batch.column(0)

            array = pd.drop_null(array) # Remove nulos

            if len(array) == 0:
                del batch, array
                continue

            for value in array.to_pylist():
                if value is None:
                    continue

                if max_date is None or value > max_date:
                    if max_date != value:
                        second_max_date = max_date
                    
                    max_date = value

                elif value != max_date and (
                    second_max_date is None or value > second_max_date
                ):
                    second_max_date = value

            del batch, array

    except Exception as e:
        logger.critical(
            'erro ao processar arquivo parquet'
        )
        return None

    if second_max_date is None:
        logger.warning(
            'nao foi possivel determinar a penultima data'
        )
        return None
    
    return second_max_date.strftime(format)
"""
Módulo para a requisiçã de dados essenciais
"""

from pathlib import Path
from typing import Optional
from datetime import datetime, timedelta
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
    Calcula a penultima data distinta presente em uma coluna de um arquivo Parquet, utilizando processamento em streaming para minimar o uso de memória.

    A função percorre o arquivo Parquet em pequenos lotes (batches), mantendo em memória apenas as duas maiores datas encontradas até o momento.
    Nenhum conjunto completo de dados é armazenado

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

def _format_date(date: datetime, format: str) -> str:
    """
    Formata um objeto datetime para string
    Centraliza a responsabilidade de formatação

    params:
    date: datetime | Recebe o input de data
    format: str | Recebe o formato da data
    """
    return date.strftime(format)

def _relative_date(
        days_offset: int = 0,
        base_date: Optional[datetime] = None,
        format: str = '%d/%m/%Y'
):
    """
    Retorna uma data relativa à data base

    params:
    days_offset: int = 0 | Dias a subtrair ou somar
    base_date: Optional[datetime] = None | Permite a injeção de data para testes
    format: str = '%d/%m/%Y' | Defina o formato de retorno da data
    """

    base = base_date or datetime.now()
    target = base + timedelta(days=days_offset)

    return _format_date(target, format)

def today(format: str = '%d/%m/%Y', base_date: Optional[datetime] = None) -> str:
    """
    Retorna a data atual formatada.
    
    params:
    format: str = '%d/%m/%Y' | Defina o formato da data retornada
    base_date: Optional[datetime] = None | Permite injeção de data para testes
    """

    return _relative_date(0, base_date, format)

def yesterday(format: str = '%d/%m/%Y', base_date: Optional[datetime] = None) -> str:

    """
    Retorna a data de onte formatada.
    
    params:
    format: str = '%d/%m/%Y' | Defina o formato da data retornada
    base_date: Optional[datetime] = None | Permite a injeção de data para testes
    """
    return _relative_date(-1, base_date, format)

def business_date(
        format: str = '%d/%m/%Y',
        base_date: Optional[datetime] = None
) -> str:
    """
    Retorna a data considerando a regra de negógio:
    
    - Segunda a sexta -> dia anterior
    = Sábado a domingo -> última sexta-feira
    
    params:
    format: str = '%d/%m/%Y' | Retorna o formato da data que será retornada
    base_date: Optional[datetime] = None | Permite a injeção de data para testes
    """

    base = base or datetime.now()
    weekday = base.weekday() # segunda = 0, domingo = 6

    if weekday <= 4: # segunda a sexta
        target = base - timedelta(days=1)
    else:  # sábadp 5 ou domingo 6
        target = base - timedelta(days=weekday - 4)

    return _format_date(target, format)
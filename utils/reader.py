"""
File responsável pelas funções de leituras e escritas de arquivos
"""

from pathlib import Path
from typing import Mapping
import shutil
from utils.config_logger import log_with_context
from config.pipeline_config import logger
import pyarrow as pa
import pyarrow.parquet as pq

@log_with_context(job='move_files',logger=logger)
def move_files(file_router: Mapping[Path | str, Path | str]) -> None:
    """
    Move arquivos de múltiplos diretórios de origem para seus respectivos destinos
    
    - Cria diretórios de destino automáticamente
    - Ignora diretórios de origem inexistentes
    - Move arquivos de forma incremental (stream-safe)
    """

    for src, dest in file_router.items():
        src_dir = Path(src)
        dest_dir = Path(dest)

        if not src_dir.exists():
            logger.warning(
                'diretorio de origem inexistente'
            )
            continue
            
        dest_dir.mkdir(parents=True, exist_ok=True)

        for file_path in src_dir.iterdir():
            if not file_path.is_file():
                continue

            try:
                shutil.move(
                    str(file_path),
                    str(dest_dir / file_path.name)
                )

                logger.info(
                    'arquivo movido com sucesso'
                )

            except Exception as exc:
                logger.error(
                    'erro ao mover arquivo'
                )

@log_with_context(job='merge_parquet', logger=logger)
def merge_parquet(
    file_router: Mapping[Path | str, Path | str],
    batch_size: int = 50_000
) -> None:
    """
    Consolida arquivos Parquet de múltiplos diretórios (Silver -> Gold)
    usando escrita incremental com PYARROW (stream-safe)
    - Não carrega datasets inteiros na memória
    - Usa ParquetWriter(nível produção)

    params:
    file_router: Mapping[Path | str, Path | str] | Mapeia as origens e destinos declarados
    batch_sizes: int = 50_000 | Quantidade de linha que serão lidas a cada batch
    """

    for src, dest in file_router.items():
        src_dir = Path(src)
        dest_dir = Path(dest)
        dest_dir.mkdir(parens=True, exist_ok=True)

        output_file = dest_dir / f'{src_dir.name}_consolidated.parquet'

        files = [
            *dest_dir.glob('*.parquet'),
            *src_dir.glob('*.parquet')
        ]

        files = [f for f in files if f != output_file]

        if not files:
            logger.info(
                'nenhum parquet encontrado, pulando merge'
            )
            continue

        writer: pq.ParquetWriter | None = None
        total_rows = 0
        file_count = 0

        try:
            for parquet_file in files:
                parquet = pq.ParquetFile(parquet_file)

                for batch in parquet.iter_batches(batch_size=batch_size):
                    table = pa.Table.from_batches([batch])

                    if writer is None:
                        writer = pq.ParquetWriter(
                            output_file,
                            table.schema,
                            compression='snappy'
                        )

                    writer.write_table(table)
                    total_rows += table.num_rows

                file_count += 1
            
            logger.info(
                'merge concluido com sucesso'
            )

        finally:
            if writer:
                writer.close()

        # Remove arquivos antigos somente após sucesso

        for f in files:
            f.unlink()


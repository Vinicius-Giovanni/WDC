import inspect
from pathlib import Path
from typing import Callable, Any

"""
Classe base de extração de relatórios do sistema IBM, outras classes herdarão essa classe base
"""

class BaseDataExtraction:
    def __init__(
            self,
            cookies: list[dict],
            download_dir: Path,
            list_filial: list,
            parquet_folder: Path | None = None,
            entry_date: str | Callable | None = None,
            exit_date: str | Callable | None = None,
            **kwargs: Any
    ):
        self.cookies = cookies
        self.download_dir = download_dir
        self.list_filial = list_filial
        self.parquet_folder = parquet_folder
        self.entry_date = entry_date
        self.exit_date = exit_date
        self.extra_params = kwargs

        self.driver = None
    
    def _resolve_date(self, date_value):
        if callable(date_value):
            sig = inspect.signature(date_value)
            if 'parquet_folder' in sig.parameters:
                return date_value(self.parquet_folder)
            return date_value()
        return date_value
    
    def run(self):
        self.entry_date = self._resolve_date(self.entry_date)
        self.exit_date = self._resolve_date(self.exit_date)

        self.driver = create_authenticated_driver(
            self.cookies,
            download_dir=self.download_dir
        )

        try:
            for filial in self.list_filial:
                self._execute_for_filial(filial)

        except Exception as e:
            logger.info(
                f'falha extração {self.__class__.__name__}',
                extra={'status': 'critico', 'error': str(e)}
            )

        finally:
            self.driver.quit()
from playwright.sync_api import sync_playwright
from pathlib import Path
import tempfile
import uuid
import os
import json
import logging

logger = logging.getLogger(__name__)

def init_browser(download_dir: str | Path):
    """
    Inicializa o browser playwright com configurações

    download_dir: str | Path
    Path da pasta de download que será configurada no browser
    """

    headless = os.getenv('CHROME_HEADLESS', 'false').lower() == 'true'
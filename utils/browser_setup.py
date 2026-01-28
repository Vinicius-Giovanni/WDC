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

    user_data_dir = tempfile.mkdtemp(prefix=f'chrome_profile_{uuid.uuid4()}')

    playwright = sync_playwright().start()

    context = playwright.chromium.launch_persistent_context(
        user_data_dir=user_data_dir,
        headless=headless,
        accept_download=True,
        downloads_path=str(download_dir),
        args=[
            '--start-maximized',
            '--disable-popup-blocking',
            '--disable-extensions',
            '--no-sandbox',
            '--disable-gpu',
            '--disable-dev-shm-usage'
        ]
    )
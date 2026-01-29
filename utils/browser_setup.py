"""
Arquivo com todas as regras de configuração do navegador

functions:
init_browser(): Inicia a instância com as configurações aplicadas

create_authenticated_page(): Cria uma página já autenticada via cookies
"""

from playwright.sync_api import sync_playwright
from config.pipeline_config import LINKS
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
            '--disable-dev-shm-usage',
            '--disable-blink-features=AutomationControlled'
        ]
    )

    return playwright, context

def create_authenticated_page(cookies: list[dict], download_dir: Path):
    """
    Cria uma página playwright já autenticada via cookies

    cookies: list[dict]
    Recebe os cookies em formato de dicionário

    download_dir: Path
    Recebe o path do diretório de download que será configurado na instância
    """

    playwright, context = init_browser(download_dir=download_dir)

    page = context.new_page()
    page.goto(LINKS['LOGIN_CSI'])

    formatted_cookies = []

    for cookie in cookies:
        try:
            cookie_copy = cookie.copy()

            # Selenium -> Playwright
            if 'expiry' in cookie_copy:
                cookie_copy['expires'] = cookie_copy.pop('expiry')

            # Remove sameSite se existir (segurança)
            cookie_copy.pop('sameSite', None)

            # Playwright exige domain ou url
            if 'domain' not in cookie_copy:
                cookie_copy['url'] = LINKS['LOGIN_CSI']

            formatted_cookies.append(cookie_copy)

        except Exception as e:
            logger.warning(
                f'cookie inválido descartado {cookie} motivo {e}',
                extra={
                    'job': 'create_authenticated_driver',
                    'status': 'failure'
                })
            
    context.add_cookies(formatted_cookies)

    # Reload da página já autenticada
    page.goto(LINKS['LOGIN_CSI'])

    return page, context, playwright

def load_cookies(path='cookies.json'):
    """
    Carrega os cookies
    """
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)
    

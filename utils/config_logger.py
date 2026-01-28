import os
import logging
import time
from pythonjsonlogger import jsonlogger
from concurrent_log_handler import ConcurrentRotatingFileHandler
import functools
import uuid
import inspect
from config.paths import LOG_PATH, LOG_DIR
import getpass

class ContextFilter(logging.Filter):
    """
    Filtro que injeta o nome do usuário no regitro de log
    """

    def __init__(self, user=None, locate=None):
        super().__init__()
        self.user = user or get_user()
        self.locate = "windows"

    def filter(self, record):
        record.user = self.user
        record.locate = self.locate
        return True
    
class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """
    Formatação de asctime para registro de log
    """

    def formatTime(self, record, datefmt=None):
        return time.strftime("%d/%m/%Y %H:%M:%S", time.localtime(record.created))
    
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        if record.exc_info:
            log_record['traceback'] = self.formatException(record.exc_info)

def setup_logger(name= __name__, locate_value=None):
    """
    Criação e configuração de log com um único handler
    """

    os.makedinr(LOG_DIR, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        logHandler = ConcurrentRotatingFileHandler(LOG_PATH, maxBytes=10 * 1024 * 1024, backupCount=5)

        formatter = CustomJsonFormatter(
            fmt=' '.join([
                '%(asctime)s', # data e hora exatos
                '%(levelname)s', # log level (INFO, WARNING, ERROR, etc.)
                '%(name)s', # nome da log
                '%(message)s', # atual mensagem na log
                '%(job)s', # nome da execução
                '%(statu)s', # status da execução (sucess, failure, etc)
                '%(user)s', # usuario ou sistema responsavel pela execução
                '%(locate)s', # sistema operação
                '%(event_id)s', # ID de execução única
                '%(duration)s', # execution duration
                '%(source_file)s', # arquivo/fonte que acionou a execução
            ]))
        logHandler.setFormatter(formatter)

        context_filter = ContextFilter(user=get_user(), locate=locate_value)
        logger.addFilter(context_filter)
        logger.addHandler(logHandler)

    if not logger or not isinstance(logger, logging.Logger):
        raise RuntimeError(f'logger invalido criado para {name}')
    
    return logger

def log_with_context(job=None, logger=None):
    """
    Aplicação de decorador com contexto corporativo (job, duration, event, ID, etc)
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            event_id = str(uuid.uuid4())
            source_file = inspect.getfile(func)

            _logger = logger or kwargs.get('logger')
            if _logger is None:
                _logger = setup_logger(name='default_logger') # fallback

            try:
                result = func(*args, **kwargs)
                duration = round(time.time() - start_time, 2)

                _logger.info(
                    f"{func.__name__} executed successfully",
                    extra = {
                        'job': job or func.__name__,
                        'status': 'sucess',
                        'event_id': event_id,
                        'duration': duration,
                        'source_file': source_file
                    }
                )
                return result
            except Exception as e:
                duration = round(time.time() - start_time, 2)
                _logger.exception(
                    f"error execution {func.__name__}: {e}",
                    extra={
                        'job': job or func.__name__,
                        'status': 'failure',
                        'event_id': event_id,
                        'duration': duration,
                        'source_file': source_file
                    }
                )
                raise
        return wrapper
    return decorator

def get_user():
    """
    Retorna o namo do sistema operacional
    """

    user = os.getenv('USERNAME') or os.getenv("USER")
    if not user:
        # fallback para casos especificos
        user = getpass.getuser()
    return user
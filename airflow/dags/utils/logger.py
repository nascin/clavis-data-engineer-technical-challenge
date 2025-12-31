"""
CLAVIS DATA ENGINEER CHALLENGE
Utilitários de Logging Estruturado

Fornece logging formatado em JSON para melhor integração com ELK Stack.
"""

import logging
import json
import sys
from datetime import datetime
from typing import Any, Dict, Optional


class JSONFormatter(logging.Formatter):
    """
    Formatador personalizado que gera logs em JSON.

    Perfeito para logging estruturado no ELK Stack.
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Formata registro de log como JSON.

        Args:
            record: Registro de log

        Returns:
            String de log formatada em JSON
        """
        log_data = {
            '@timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }

        # Adiciona informações de exceção se presentes
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)

        # Adiciona campos extras se presentes
        if hasattr(record, 'extra_data'):
            log_data.update(record.extra_data)

        # Adiciona contexto DAG se disponível (para Airflow)
        if hasattr(record, 'dag_id'):
            log_data['dag_id'] = record.dag_id
        if hasattr(record, 'task_id'):
            log_data['task_id'] = record.task_id
        if hasattr(record, 'execution_date'):
            log_data['execution_date'] = str(record.execution_date)

        return json.dumps(log_data, ensure_ascii=False)


def setup_logger(
    name: str,
    level: str = 'INFO',
    json_format: bool = True
) -> logging.Logger:
    """
    Configura logger com formatação JSON opcional.

    Args:
        name: Nome do logger
        level: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_format: Usa formatador JSON se True, padrão se False

    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))

    # Remove handlers existentes
    logger.handlers.clear()

    # Cria handler de console
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, level.upper()))

    # Define formatador
    if json_format:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Previne propagação para evitar logs duplicados
    logger.propagate = False

    return logger


class LogContext:
    """
    Gerenciador de contexto para adicionar dados extras aos logs.

    Uso:
        logger = setup_logger(__name__)
        with LogContext(logger, dag_id='weather_dag', task_id='extract'):
            logger.info("Processing data")
    """

    def __init__(self, logger: logging.Logger, **context):
        """
        Inicializa o contexto de log.

        Args:
            logger: Instância do logger
            **context: Dados de contexto para adicionar aos logs
        """
        self.logger = logger
        self.context = context
        self.original_factory = None

    def __enter__(self):
        """Entrada do contexto."""
        self.original_factory = logging.getLogRecordFactory()

        def record_factory(*args, **kwargs):
            record = self.original_factory(*args, **kwargs)
            for key, value in self.context.items():
                setattr(record, key, value)
            return record

        logging.setLogRecordFactory(record_factory)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Saída do contexto."""
        logging.setLogRecordFactory(self.original_factory)


def log_function_call(logger: logging.Logger):
    """
    Decorator para registrar chamadas de função com parâmetros e resultados.

    Args:
        logger: Instância do logger

    Uso:
        @log_function_call(logger)
        def my_function(param1, param2):
            return result
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Registra chamada de função
            logger.info(
                f"Calling {func.__name__}",
                extra={
                    'extra_data': {
                        'function': func.__name__,
                        'args': str(args),
                        'kwargs': str(kwargs)
                    }
                }
            )

            try:
                result = func(*args, **kwargs)

                # Registra sucesso
                logger.info(
                    f"Function {func.__name__} completed successfully",
                    extra={
                        'extra_data': {
                            'function': func.__name__,
                            'status': 'success'
                        }
                    }
                )

                return result

            except Exception as e:
                # Registra erro
                logger.error(
                    f"Function {func.__name__} failed: {str(e)}",
                    exc_info=True,
                    extra={
                        'extra_data': {
                            'function': func.__name__,
                            'status': 'error',
                            'error': str(e)
                        }
                    }
                )
                raise

        return wrapper
    return decorator

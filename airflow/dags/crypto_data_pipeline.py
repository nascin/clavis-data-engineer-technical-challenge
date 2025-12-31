"""
CLAVIS DATA ENGINEER CHALLENGE
DAG de Coleta de Dados de Criptomoedas

DAG do Airflow para coletar dados de criptomoedas da API CoinMarketCap.

Recursos:
- Coleta de dados agendada a cada 15 minutos
- Monitoramento das 20 principais criptomoedas
- Conversão multi-moeda (USD, BRL, EUR)
- Métricas globais de mercado
- Tratamento de erros com callbacks
- Logging de métricas do pipeline

Autor: Candidato do Clavis Challenge
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Importa utilitários personalizados
from utils.crypto_api_client import CoinMarketCapClient
from utils.data_manager import DataManager
from utils.crypto_config import CryptoConfig
from utils.logger import setup_logger, LogContext


# ==========================================
# CONFIGURAÇÃO
# ==========================================

# Configura logger
logger = setup_logger(__name__, level=CryptoConfig.LOG_LEVEL)

# Argumentos padrão do DAG
default_args = {
    'owner': 'clavis-challenge',
    'depends_on_past': False,
    'email_on_failure': bool(CryptoConfig.ALERT_EMAIL),
    'email_on_retry': False,
    'email': [CryptoConfig.ALERT_EMAIL] if CryptoConfig.ALERT_EMAIL else [],
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=30),
}


# ==========================================
# FUNÇÕES DE CALLBACK
# ==========================================

def on_success_callback(context):
    """Callback executado em caso de sucesso do DAG."""
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    # Airflow 3.x uses 'logical_date' instead of 'execution_date'
    logical_date = context.get('logical_date') or context.get('execution_date')

    logger.info(
        f"✅ DAG {dag_id} completed successfully",
        extra={
            'extra_data': {
                'dag_id': dag_id,
                'task_id': task_id,
                'logical_date': str(logical_date),
                'status': 'success'
            }
        }
    )


def on_failure_callback(context):
    """Callback executado em caso de falha do DAG."""
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    # Airflow 3.x usa 'logical_date' ao invés de 'execution_date'
    logical_date = context.get('logical_date') or context.get('execution_date')
    exception = context.get('exception', 'Unknown error')

    logger.error(
        f"❌ DAG {dag_id} failed at task {task_id}",
        extra={
            'extra_data': {
                'dag_id': dag_id,
                'task_id': task_id,
                'logical_date': str(logical_date),
                'status': 'failed',
                'error': str(exception)
            }
        }
    )

    # Simula envio de email de alerta (em produção, usar SMTP/SendGrid/SES)
    logger.warning("=" * 50)
    logger.warning("📧 SIMULANDO ENVIO DE EMAIL DE ALERTA")
    logger.warning(f"Para: {CryptoConfig.ALERT_EMAIL or 'engenharia-dados@empresa.com'}")
    logger.warning(f"Assunto: DAG {dag_id} falhou na task {task_id}")
    logger.warning(f"Erro: {exception}")
    logger.warning("=" * 50)

    # Salva métricas de falha
    try:
        data_manager = DataManager()
        failure_data = {
            '@timestamp': datetime.utcnow().isoformat() + 'Z',
            'dag_id': dag_id,
            'task_id': task_id,
            'logical_date': str(logical_date),
            'status': 'failed',
            'error_message': str(exception),
            'pipeline_type': 'crypto_data_collection'
        }
        data_manager.save_pipeline_execution(failure_data)
    except Exception as e:
        logger.error(f"Failed to save failure metrics: {e}")


# ==========================================
# FUNÇÕES DE TAREFAS
# ==========================================

def validate_configuration(**context):
    """
    Valida a configuração antes de iniciar o pipeline.
    """
    with LogContext(logger, dag_id=context['dag'].dag_id, task_id=context['task'].task_id):
        logger.info("🔍 Validating configuration...")

        try:
            CryptoConfig.validate()
            logger.info("✅ Configuration valid")
            logger.info(CryptoConfig.print_config())
        except Exception as e:
            logger.error(f"❌ Configuration validation failed: {e}")
            raise


def extract_crypto_prices(**context):
    """
    Extrai preços e métricas de criptomoedas.
    """
    with LogContext(logger, dag_id=context['dag'].dag_id, task_id=context['task'].task_id):
        logger.info("💰 Starting cryptocurrency data extraction...")

        start_time = datetime.utcnow()
        symbols = CryptoConfig.get_crypto_symbol_list()
        currencies = CryptoConfig.get_fiat_currencies()

        logger.info(
            f"Extracting data for {len(symbols)} cryptocurrencies "
            f"in {len(currencies)} currencies: {', '.join(currencies)}"
        )

        try:
            # Initialize API client
            with CoinMarketCapClient() as client:
                # Extract crypto data
                crypto_data = client.collect_crypto_data(
                    symbols=symbols,
                    convert_currencies=currencies
                )

                logger.info(f"✅ Extracted {len(crypto_data)} cryptocurrency records")

                # Save to file in NDJSON format (one JSON per line for easy ELK processing)
                if crypto_data:
                    data_manager = DataManager()
                    file_path = data_manager.save_ndjson(
                        crypto_data,
                        f"crypto_prices_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
                    )
                    logger.info(f"💾 Saved crypto prices to {file_path} in NDJSON format")

                    # Log some stats
                    if crypto_data:
                        btc_data = next((c for c in crypto_data if c['symbol'] == 'BTC'), None)
                        if btc_data and 'quotes' in btc_data and 'USD' in btc_data['quotes']:
                            btc_price = btc_data['quotes']['USD']['price']
                            btc_change_24h = btc_data['quotes']['USD']['percent_change_24h']
                            logger.info(
                                f"📊 BTC: ${btc_price:,.2f} "
                                f"({btc_change_24h:+.2f}% 24h)"
                            )

                # Calculate execution metrics
                execution_time = (datetime.utcnow() - start_time).total_seconds()

                # Push metrics to XCom
                context['task_instance'].xcom_push(
                    key='crypto_metrics',
                    value={
                        'records_extracted': len(crypto_data),
                        'records_valid': len(crypto_data),
                        'execution_time_seconds': execution_time,
                        'api_calls': client.request_count
                    }
                )

                return crypto_data

        except Exception as e:
            logger.error(f"❌ Crypto data extraction failed: {e}", exc_info=True)
            raise


def extract_global_metrics(**context):
    """
    Extrai métricas globais do mercado de criptomoedas.
    """
    with LogContext(logger, dag_id=context['dag'].dag_id, task_id=context['task'].task_id):
        logger.info("🌍 Starting global market metrics extraction...")

        start_time = datetime.utcnow()
        currencies = CryptoConfig.get_fiat_currencies()

        try:
            # Initialize API client
            with CoinMarketCapClient() as client:
                # Extract global market data
                global_data = client.get_global_market_data(
                    convert_currencies=currencies
                )

                logger.info("✅ Extracted global market metrics")

                # Save to file in NDJSON format (single object as one line)
                if global_data:
                    data_manager = DataManager()
                    file_path = data_manager.save_ndjson(
                        [global_data],  # Wrap in list for NDJSON format
                        f"crypto_global_metrics_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
                    )
                    logger.info(f"💾 Saved global metrics to {file_path} in NDJSON format")

                    # Log some stats
                    if 'quotes' in global_data and 'USD' in global_data['quotes']:
                        total_market_cap = global_data['quotes']['USD']['total_market_cap']
                        total_volume = global_data['quotes']['USD']['total_volume_24h']
                        btc_dominance = global_data.get('btc_dominance', 0)

                        logger.info(
                            f"📊 Market Cap: ${total_market_cap:,.0f} | "
                            f"24h Volume: ${total_volume:,.0f} | "
                            f"BTC Dominance: {btc_dominance:.2f}%"
                        )

                # Calculate execution metrics
                execution_time = (datetime.utcnow() - start_time).total_seconds()

                # Push metrics to XCom
                context['task_instance'].xcom_push(
                    key='global_metrics',
                    value={
                        'records_extracted': 1,
                        'records_valid': 1,
                        'execution_time_seconds': execution_time,
                        'api_calls': client.request_count
                    }
                )

                return global_data

        except Exception as e:
            logger.error(f"❌ Global metrics extraction failed: {e}", exc_info=True)
            raise


def log_pipeline_metrics(**context):
    """
    Consolida e registra métricas de execução do pipeline.
    """
    with LogContext(logger, dag_id=context['dag'].dag_id, task_id=context['task'].task_id):
        logger.info("📊 Logging pipeline metrics...")

        try:
            # Get metrics from XCom
            ti = context['task_instance']
            crypto_metrics = ti.xcom_pull(task_ids='extract_crypto_prices', key='crypto_metrics') or {}
            global_metrics = ti.xcom_pull(task_ids='extract_global_metrics', key='global_metrics') or {}

            # Calculate totals
            total_records = (
                crypto_metrics.get('records_valid', 0) +
                global_metrics.get('records_valid', 0)
            )
            total_time = (
                crypto_metrics.get('execution_time_seconds', 0) +
                global_metrics.get('execution_time_seconds', 0)
            )
            total_api_calls = (
                crypto_metrics.get('api_calls', 0) +
                global_metrics.get('api_calls', 0)
            )

            # Create metrics document
            # Airflow 3.x uses 'logical_date' instead of 'execution_date'
            logical_date = context.get('logical_date') or context.get('execution_date')

            metrics = {
                '@timestamp': datetime.utcnow().isoformat() + 'Z',
                'dag_id': context['dag'].dag_id,
                'task_id': context['task'].task_id,
                'logical_date': str(logical_date),
                'status': 'success',
                'pipeline_type': 'crypto_data_collection',
                'records_extracted': total_records,
                'records_processed': total_records,
                'records_failed': 0,
                'execution_duration_seconds': round(total_time, 2),
                'api_calls_used': total_api_calls,
                'crypto_records': crypto_metrics.get('records_valid', 0),
                'global_metrics_records': global_metrics.get('records_valid', 0),
                'cryptocurrencies_monitored': len(CryptoConfig.get_crypto_symbol_list()),
                'fiat_currencies': len(CryptoConfig.get_fiat_currencies())
            }

            # Save metrics
            data_manager = DataManager()
            file_path = data_manager.save_pipeline_execution(metrics)

            logger.info(f"✅ Pipeline metrics saved to {file_path}")
            logger.info(
                f"�� Summary: {total_records} records in {round(total_time, 2)}s "
                f"using {total_api_calls} API calls"
            )

        except Exception as e:
            logger.error(f"❌ Failed to log pipeline metrics: {e}", exc_info=True)
            # Don't fail the DAG if metrics logging fails


# ==========================================
# DEFINIÇÃO DO DAG
# ==========================================

with DAG(
    dag_id='crypto_data_pipeline',
    default_args=default_args,
    description='Coletar dados de criptomoedas da API CoinMarketCap',
    schedule=timedelta(minutes=CryptoConfig.COLLECTION_INTERVAL_MINUTES),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['clavis-challenge', 'cryptocurrency', 'coinmarketcap', 'production'],
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
) as dag:

    # Tarefa 1: Validar configuração
    validate_config = PythonOperator(
        task_id='validate_configuration',
        python_callable=validate_configuration,
    )

    # Tarefa 2: Extrair preços de criptomoedas
    extract_prices = PythonOperator(
        task_id='extract_crypto_prices',
        python_callable=extract_crypto_prices,
    )

    # Tarefa 3: Extrair métricas globais de mercado
    extract_global = PythonOperator(
        task_id='extract_global_metrics',
        python_callable=extract_global_metrics,
    )

    # Tarefa 4: Registrar métricas do pipeline
    log_metrics = PythonOperator(
        task_id='log_pipeline_metrics',
        python_callable=log_pipeline_metrics,
    )

    # Define dependências de tarefas
    validate_config >> [extract_prices, extract_global] >> log_metrics
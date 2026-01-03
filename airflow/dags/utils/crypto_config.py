"""
CLAVIS DATA ENGINEER CHALLENGE
Gerenciamento de Configuração de Criptomoedas

Configuração centralizada para o pipeline de dados de criptomoedas.
"""

import os
from typing import Dict, List
from dataclasses import dataclass


@dataclass
class CryptoSymbol:
    """Configuração de símbolo de criptomoeda."""
    symbol: str
    name: str
    category: str = "crypto"

    def __str__(self) -> str:
        return self.symbol


class CryptoConfig:
    """
    Gerenciador de configuração para o pipeline de dados de criptomoedas.

    Lê de variáveis de ambiente com padrões sensatos.
    """

    # ==========================================
    # CONFIGURAÇÃO DE API
    # ==========================================
    COINMARKETCAP_API_KEY = os.getenv('COINMARKETCAP_API_KEY', '')

    # ==========================================
    # CONFIGURAÇÕES DE CRIPTOMOEDAS
    # ==========================================
    # Principais criptomoedas para monitorar
    DEFAULT_CRYPTO_SYMBOLS = [
        CryptoSymbol("BTC", "Bitcoin", "Layer 1"),
        CryptoSymbol("ETH", "Ethereum", "Layer 1"),
        CryptoSymbol("BNB", "BNB", "Exchange"),
        CryptoSymbol("SOL", "Solana", "Layer 1"),
        CryptoSymbol("XRP", "Ripple", "Payment"),
        CryptoSymbol("ADA", "Cardano", "Layer 1"),
        CryptoSymbol("AVAX", "Avalanche", "Layer 1"),
        CryptoSymbol("DOGE", "Dogecoin", "Meme"),
        CryptoSymbol("DOT", "Polkadot", "Layer 0"),
        CryptoSymbol("MATIC", "Polygon", "Layer 2"),
        CryptoSymbol("LINK", "Chainlink", "Oracle"),
        CryptoSymbol("UNI", "Uniswap", "DeFi"),
        CryptoSymbol("ATOM", "Cosmos", "Layer 0"),
        CryptoSymbol("LTC", "Litecoin", "Payment"),
        CryptoSymbol("ETC", "Ethereum Classic", "Layer 1"),
        CryptoSymbol("XLM", "Stellar", "Payment"),
        CryptoSymbol("ALGO", "Algorand", "Layer 1"),
        CryptoSymbol("VET", "VeChain", "Supply Chain"),
        CryptoSymbol("FIL", "Filecoin", "Storage"),
        CryptoSymbol("HBAR", "Hedera", "Enterprise"),
    ]

    # Moedas fiduciárias para conversão
    # Nota: Plano gratuito permite apenas 1 moeda por chamada de API
    DEFAULT_FIAT_CURRENCIES = ["BRL"]

    # Intervalo de coleta em minutos (plano gratuito: 333 chamadas/dia = ~1 chamada a cada 4,3 min)
    COLLECTION_INTERVAL_MINUTES = int(
        os.getenv('CRYPTO_COLLECTION_INTERVAL', '15')
    )

    # ==========================================
    # CAMINHOS
    # ==========================================
    DATA_BASE_PATH = os.getenv('DATA_BASE_PATH', '/opt/airflow/data')
    DATA_RAW_PATH = os.path.join(DATA_BASE_PATH, 'raw')
    DATA_PROCESSED_PATH = os.path.join(DATA_BASE_PATH, 'processed')

    # ==========================================
    # LIMITES DE ALERTA
    # ==========================================
    # Limite de alerta de mudança de preço (porcentagem)
    PRICE_CHANGE_ALERT_THRESHOLD = float(
        os.getenv('PRICE_CHANGE_ALERT_THRESHOLD', '10')
    )

    # Limite de volume (USD)
    VOLUME_ALERT_THRESHOLD_MIN = float(
        os.getenv('VOLUME_ALERT_THRESHOLD_MIN', '1000000')
    )

    # Limite de mudança de market cap (USD)
    MARKET_CAP_CHANGE_THRESHOLD = float(
        os.getenv('MARKET_CAP_CHANGE_THRESHOLD', '1000000000')
    )

    # ==========================================
    # CONFIGURAÇÕES DE NOTIFICAÇÃO (Opcional)
    # ==========================================
    ALERT_EMAIL = os.getenv('ALERT_EMAIL', '')
    SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL', '')

    # ==========================================
    # CONFIGURAÇÕES DE QUALIDADE DE DADOS
    # ==========================================
    MAX_RETRIES = 2
    RETRY_DELAY_SECONDS = 5
    REQUEST_TIMEOUT_SECONDS = 30

    # Retenção de dados (dias)
    RAW_DATA_RETENTION_DAYS = 30
    PROCESSED_DATA_RETENTION_DAYS = 90

    # ==========================================
    # LIMITAÇÃO DE TAXA DE API
    # ==========================================
    # Limites do plano gratuito CoinMarketCap
    DAILY_API_CALL_LIMIT = 333
    MONTHLY_API_CALL_LIMIT = 10000

    # ==========================================
    # LOGGING
    # ==========================================
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

    @classmethod
    def get_crypto_symbols(cls) -> List[CryptoSymbol]:
        """
        Obtém lista de criptomoedas para monitorar.

        Pode ser sobrescrito pela variável de ambiente CRYPTO_SYMBOLS (separada por vírgulas).

        Returns:
            Lista de objetos CryptoSymbol
        """
        symbols_env = os.getenv('CRYPTO_SYMBOLS', '')

        if symbols_env:
            # Analisa da variável de ambiente
            symbol_names = [s.strip() for s in symbols_env.split(',')]
            # Filtra símbolos padrão por nome
            return [s for s in cls.DEFAULT_CRYPTO_SYMBOLS if s.symbol in symbol_names]

        return cls.DEFAULT_CRYPTO_SYMBOLS

    @classmethod
    def get_crypto_symbol_list(cls) -> List[str]:
        """
        Obtém lista de símbolos de criptomoedas como strings.

        Returns:
            Lista de strings de símbolos
        """
        return [crypto.symbol for crypto in cls.get_crypto_symbols()]

    @classmethod
    def get_fiat_currencies(cls) -> List[str]:
        """
        Obtém lista de moedas fiduciárias.

        Pode ser sobrescrito pela variável de ambiente FIAT_CURRENCIES.

        Returns:
            Lista de códigos de moedas fiduciárias
        """
        currencies_env = os.getenv('FIAT_CURRENCIES', '')

        if currencies_env:
            return [c.strip() for c in currencies_env.split(',')]

        return cls.DEFAULT_FIAT_CURRENCIES

    @classmethod
    def validate(cls) -> bool:
        """
        Valida a configuração.

        Returns:
            True se a configuração é válida

        Raises:
            ValueError: Se configuração crítica estiver faltando
        """
        if not cls.COINMARKETCAP_API_KEY:
            raise ValueError(
                "COINMARKETCAP_API_KEY environment variable is required. "
                "Get your free API key from https://coinmarketcap.com/api/"
            )

        if not cls.get_crypto_symbols():
            raise ValueError("No cryptocurrencies configured for monitoring")

        if not cls.get_fiat_currencies():
            raise ValueError("No fiat currencies configured")

        return True

    @classmethod
    def get_summary(cls) -> Dict[str, any]:
        """
        Obtém resumo da configuração.

        Returns:
            Dicionário com detalhes da configuração
        """
        symbols = cls.get_crypto_symbol_list()
        currencies = cls.get_fiat_currencies()

        return {
            'api_configured': bool(cls.COINMARKETCAP_API_KEY),
            'cryptocurrencies_count': len(symbols),
            'cryptocurrencies': symbols,
            'fiat_currencies': currencies,
            'collection_interval_minutes': cls.COLLECTION_INTERVAL_MINUTES,
            'price_change_threshold': cls.PRICE_CHANGE_ALERT_THRESHOLD,
            'volume_threshold': cls.VOLUME_ALERT_THRESHOLD_MIN,
            'market_cap_threshold': cls.MARKET_CAP_CHANGE_THRESHOLD,
            'daily_api_limit': cls.DAILY_API_CALL_LIMIT,
            'monthly_api_limit': cls.MONTHLY_API_CALL_LIMIT,
            'raw_data_retention_days': cls.RAW_DATA_RETENTION_DAYS,
            'processed_data_retention_days': cls.PROCESSED_DATA_RETENTION_DAYS,
        }

    @classmethod
    def print_config(cls) -> str:
        """
        Obtém resumo da configuração como string formatada.

        Returns:
            Resumo da configuração formatado
        """
        symbols = cls.get_crypto_symbol_list()
        currencies = cls.get_fiat_currencies()

        config_str = f"""
╔════════════════════════════════════════════════════════════╗
║     DESAFIO CLAVIS DATA ENGINEER - CONFIGURAÇÃO CRIPTO     ║
╚════════════════════════════════════════════════════════════╝

API Configuration:
   • CoinMarketCap API Key: {'✓ Configured' if cls.COINMARKETCAP_API_KEY else '✗ Missing'}

Cryptocurrencies to Monitor ({len(symbols)}):
   {', '.join(symbols)}

Fiat Currencies ({len(currencies)}):
   {', '.join(currencies)}

Collection Settings:
   • Interval: {cls.COLLECTION_INTERVAL_MINUTES} minutes
   • Max Retries: {cls.MAX_RETRIES}
   • Timeout: {cls.REQUEST_TIMEOUT_SECONDS}s

Alert Thresholds:
   • Price Change: ≥ {cls.PRICE_CHANGE_ALERT_THRESHOLD}%
   • Min Volume: ≥ ${cls.VOLUME_ALERT_THRESHOLD_MIN:,.0f}
   • Market Cap Change: ≥ ${cls.MARKET_CAP_CHANGE_THRESHOLD:,.0f}

API Rate Limits (Free Tier):
   • Daily: {cls.DAILY_API_CALL_LIMIT} calls/day
   • Monthly: {cls.MONTHLY_API_CALL_LIMIT} calls/month
   • Recommended interval: ≥ 5 minutes

Data Retention:
   • Raw Data: {cls.RAW_DATA_RETENTION_DAYS} days
   • Processed Data: {cls.PROCESSED_DATA_RETENTION_DAYS} days

Notifications:
   • Email: {'✓ ' + cls.ALERT_EMAIL if cls.ALERT_EMAIL else '✗ Not configured'}
   • Slack: {'✓ Configured' if cls.SLACK_WEBHOOK_URL else '✗ Not configured'}

╚════════════════════════════════════════════════════════════╝
        """

        return config_str
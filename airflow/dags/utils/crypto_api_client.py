"""
CLAVIS DATA ENGINEER CHALLENGE
Cliente de API CoinMarketCap

Cliente de API robusto com lógica de retry, tratamento de erros e logging.
"""

import os
import time
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


class CoinMarketCapClient:
    """
    Cliente de API CoinMarketCap com recursos avançados:
    - Retentativas automáticas com backoff exponencial
    - Proteção contra limite de taxa (333 chamadas/dia no plano gratuito)
    - Tratamento de erros abrangente
    - Logging estruturado
    - Suporte a múltiplos endpoints
    """

    BASE_URL = "https://pro-api.coinmarketcap.com/v1"
    FREE_TIER_DAILY_LIMIT = 333
    FREE_TIER_MONTHLY_LIMIT = 10000

    def __init__(self, api_key: Optional[str] = None, timeout: int = 30):
        """
        Inicializa o cliente de API.

        Args:
            api_key: Chave de API CoinMarketCap (padrão: variável de ambiente)
            timeout: Tempo limite da requisição em segundos
        """
        self.api_key = api_key or os.getenv('COINMARKETCAP_API_KEY')
        if not self.api_key:
            raise ValueError(
                "Chave de API do CoinMarketCap não fornecida. "
                "Configure a variável de ambiente COINMARKETCAP_API_KEY ou passe o parâmetro api_key. "
                "Obtenha sua chave de API gratuita em: https://coinmarketcap.com/api/"
            )

        self.timeout = timeout
        self.session = self._create_session()
        self.logger = logging.getLogger(__name__)
        self.request_count = 0

    def _create_session(self) -> requests.Session:
        """
        Cria uma sessão de requests com estratégia de retry.

        Returns:
            Sessão de requests configurada
        """
        session = requests.Session()

        # Configura estratégia de retry com backoff exponencial
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,  # Aguarda progressivamente: 1s, 2s, 4s, 8s, 16s entre tentativas
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Configura headers padrão com chave de API
        session.headers.update({
            'X-CMC_PRO_API_KEY': self.api_key,
            'Accept': 'application/json'
        })

        return session

    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Faz requisição HTTP para a API com tratamento de erros.

        Args:
            endpoint: Endpoint da API (ex: 'cryptocurrency/listings/latest')
            params: Parâmetros de consulta

        Returns:
            Resposta da API como dicionário

        Raises:
            requests.exceptions.RequestException: Em caso de erros na API
        """
        url = f"{self.BASE_URL}/{endpoint}"
        params = params or {}

        self.logger.info(f"Iniciando requisição para endpoint: {endpoint}")
        self.request_count += 1

        try:
            response = self.session.get(
                url,
                params=params,
                timeout=self.timeout
            )

            # Lança exceção para códigos de status HTTP inválidos
            response.raise_for_status()

            data = response.json()

            # Valida o status retornado pela API
            status = data.get('status', {})
            if status.get('error_code') != 0:
                error_message = status.get('error_message', 'Erro desconhecido')
                self.logger.error(f"API retornou erro: {error_message}")
                raise Exception(f"Erro da API CoinMarketCap: {error_message}")

            # Registra uso de créditos da API
            credit_count = status.get('credit_count', 0)
            self.logger.info(
                f"Requisição bem-sucedida. Créditos utilizados: {credit_count}. "
                f"Total de requisições nesta sessão: {self.request_count}"
            )

            return data

        except requests.exceptions.HTTPError as e:
            self.logger.error(f"Erro HTTP ocorreu: {e}")
            if e.response.status_code == 401:
                self.logger.error("Chave de API inválida. Verifique sua COINMARKETCAP_API_KEY")
            elif e.response.status_code == 429:
                self.logger.error("Limite de taxa excedido. Você atingiu o limite diário/mensal.")
            self.logger.error(f"Conteúdo da resposta: {e.response.text}")
            raise

        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Erro de conexão ocorreu: {e}")
            raise

        except requests.exceptions.Timeout as e:
            self.logger.error(f"Erro de timeout ocorreu: {e}")
            raise

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Erro na requisição ocorreu: {e}")
            raise

        except json.JSONDecodeError as e:
            self.logger.error(f"Erro ao decodificar JSON: {e}")
            raise

    def get_latest_listings(
        self,
        limit: int = 20,
        convert: str = "USD,BRL,EUR"
    ) -> List[Dict[str, Any]]:
        """
        Obtém as últimas listagens de criptomoedas.

        Args:
            limit: Número de resultados (1-5000)
            convert: Lista separada por vírgulas de moedas fiduciárias

        Returns:
            Lista de dados de criptomoedas
        """
        params = {
            'limit': min(limit, 5000),
            'convert': convert
        }

        response = self._make_request('cryptocurrency/listings/latest', params)
        return response.get('data', [])

    def get_quotes_by_symbol(
        self,
        symbols: List[str],
        convert: str = "USD,BRL,EUR"
    ) -> Dict[str, Any]:
        """
        Obtém as últimas cotações para criptomoedas específicas por símbolo.

        Args:
            symbols: Lista de símbolos de criptomoedas (ex: ['BTC', 'ETH'])
            convert: Lista separada por vírgulas de moedas fiduciárias

        Returns:
            Dicionário mapeando símbolos para dados de cotação
        """
        symbols_str = ','.join(symbols)

        params = {
            'symbol': symbols_str,
            'convert': convert
        }

        response = self._make_request('cryptocurrency/quotes/latest', params)
        return response.get('data', {})

    def get_global_metrics(
        self,
        convert: str = "USD,BRL,EUR"
    ) -> Dict[str, Any]:
        """
        Obtém métricas globais do mercado de criptomoedas.

        Args:
            convert: Lista separada por vírgulas de moedas fiduciárias

        Returns:
            Dados de métricas globais
        """
        params = {'convert': convert}

        response = self._make_request('global-metrics/quotes/latest', params)
        return response.get('data', {})

    def get_trending(self) -> List[Dict[str, Any]]:
        """
        Obtém criptomoedas em alta (mais visitadas).

        Nota: Este endpoint pode não estar disponível no plano gratuito.

        Returns:
            Lista de criptomoedas em alta
        """
        try:
            response = self._make_request('cryptocurrency/trending/latest')
            return response.get('data', [])
        except Exception as e:
            self.logger.warning(f"Endpoint de trending não disponível: {e}")
            return []

    def collect_crypto_data(
        self,
        symbols: List[str],
        convert_currencies: List[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Coleta dados abrangentes de criptomoedas.

        Nota: O plano gratuito permite apenas 1 moeda de conversão por chamada.
        Este método faz chamadas separadas para cada moeda.

        Args:
            symbols: Lista de símbolos de cripto (ex: ['BTC', 'ETH', 'ADA'])
            convert_currencies: Lista de moedas fiduciárias (ex: ['USD', 'BRL', 'EUR'])

        Returns:
            Lista de dados de criptomoedas enriquecidos
        """
        convert_currencies = convert_currencies or ['USD']

        self.logger.info(
            f"Coletando dados de {len(symbols)} criptomoedas "
            f"em {len(convert_currencies)} moedas"
        )

        # Armazena dados combinados de todas as requisições
        crypto_data_map = {}  # símbolo -> dados_enriquecidos
        failed_symbols = set()

        # Processa em lotes (API permite múltiplos símbolos por chamada)
        batch_size = 100  # Limite máximo da API

        # Busca dados para cada moeda separadamente (limitação do plano gratuito)
        for currency in convert_currencies:
            self.logger.info(f"Buscando dados em {currency}...")

            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]

                try:
                    self.logger.info(
                        f"Processando lote {i//batch_size + 1} para {currency}: {', '.join(batch)}"
                    )

                    # Restrição do plano gratuito: apenas 1 moeda por chamada
                    quotes = self.get_quotes_by_symbol(batch, convert=currency)

                    for symbol in batch:
                        if symbol in quotes:
                            data = quotes[symbol]

                            # Inicializa dados enriquecidos na primeira moeda processada
                            if symbol not in crypto_data_map:
                                crypto_data_map[symbol] = {
                                    '@timestamp': datetime.utcnow().isoformat() + 'Z',
                                    'symbol': symbol,
                                    'name': data.get('name'),
                                    'slug': data.get('slug'),
                                    'rank': data.get('cmc_rank'),
                                    'max_supply': data.get('max_supply'),
                                    'circulating_supply': data.get('circulating_supply'),
                                    'total_supply': data.get('total_supply'),
                                    'last_updated': data.get('last_updated'),
                                    'date_added': data.get('date_added'),
                                    'tags': data.get('tags', []),
                                    'platform': data.get('platform'),
                                    'quotes': {}
                                }

                            # Adiciona cotações da moeda atual ao registro da criptomoeda
                            quote_data = data.get('quote', {})
                            if currency in quote_data:
                                curr_data = quote_data[currency]
                                crypto_data_map[symbol]['quotes'][currency] = {
                                    'price': curr_data.get('price'),
                                    'volume_24h': curr_data.get('volume_24h'),
                                    'volume_change_24h': curr_data.get('volume_change_24h'),
                                    'percent_change_1h': curr_data.get('percent_change_1h'),
                                    'percent_change_24h': curr_data.get('percent_change_24h'),
                                    'percent_change_7d': curr_data.get('percent_change_7d'),
                                    'percent_change_30d': curr_data.get('percent_change_30d'),
                                    'percent_change_60d': curr_data.get('percent_change_60d'),
                                    'percent_change_90d': curr_data.get('percent_change_90d'),
                                    'market_cap': curr_data.get('market_cap'),
                                    'market_cap_dominance': curr_data.get('market_cap_dominance'),
                                    'fully_diluted_market_cap': curr_data.get('fully_diluted_market_cap'),
                                    'last_updated': curr_data.get('last_updated')
                                }
                        else:
                            failed_symbols.add(symbol)
                            self.logger.warning(f"Nenhum dado retornado para o símbolo: {symbol}")

                    # Aguarda entre lotes para evitar exceder limite de taxa (333 chamadas/dia no plano gratuito)
                    time.sleep(0.5)

                except Exception as e:
                    self.logger.error(f"Falha ao buscar lote para {currency}: {e}")
                    failed_symbols.update(batch)

        # Converte mapa de símbolos em lista de resultados
        results = list(crypto_data_map.values())

        if failed_symbols:
            self.logger.warning(
                f"Falha ao buscar dados para {len(failed_symbols)} símbolos: "
                f"{', '.join(failed_symbols)}"
            )

        self.logger.info(f"Coletados com sucesso {len(results)} registros de criptomoedas")

        return results

    def get_global_market_data(
        self,
        convert_currencies: List[str] = None
    ) -> Dict[str, Any]:
        """
        Obtém dados globais do mercado de criptomoedas.

        Nota: O plano gratuito permite apenas 1 moeda de conversão por chamada.
        Este método faz chamadas separadas para cada moeda.

        Args:
            convert_currencies: Lista de moedas fiduciárias

        Returns:
            Dados globais de mercado enriquecidos
        """
        convert_currencies = convert_currencies or ['USD']

        try:
            enriched_data = {
                '@timestamp': datetime.utcnow().isoformat() + 'Z',
                'quotes': {}
            }

            # Busca métricas para cada moeda separadamente (limitação do plano gratuito)
            for currency in convert_currencies:
                self.logger.info(f"Buscando métricas globais em {currency}...")

                data = self.get_global_metrics(convert=currency)

                # Preenche metadados gerais apenas na primeira iteração
                if not enriched_data.get('active_cryptocurrencies'):
                    enriched_data.update({
                        'active_cryptocurrencies': data.get('active_cryptocurrencies'),
                        'active_exchanges': data.get('active_exchanges'),
                        'active_market_pairs': data.get('active_market_pairs'),
                        'btc_dominance': data.get('btc_dominance'),
                        'eth_dominance': data.get('eth_dominance'),
                        'defi_volume_24h': data.get('defi_volume_24h'),
                        'defi_market_cap': data.get('defi_market_cap'),
                        'stablecoin_volume_24h': data.get('stablecoin_volume_24h'),
                        'stablecoin_market_cap': data.get('stablecoin_market_cap'),
                        'last_updated': data.get('last_updated')
                    })

                # Extrai cotações globais para a moeda atual
                quote_data = data.get('quote', {})
                if currency in quote_data:
                    curr_data = quote_data[currency]
                    enriched_data['quotes'][currency] = {
                        'total_market_cap': curr_data.get('total_market_cap'),
                        'total_volume_24h': curr_data.get('total_volume_24h'),
                        'altcoin_volume_24h': curr_data.get('altcoin_volume_24h'),
                        'altcoin_market_cap': curr_data.get('altcoin_market_cap'),
                        'last_updated': curr_data.get('last_updated')
                    }

                # Aguarda entre chamadas para respeitar limite de taxa
                time.sleep(0.5)

            return enriched_data

        except Exception as e:
            self.logger.error(f"Falha ao buscar dados globais do mercado: {e}")
            raise

    def close(self):
        """Fecha a sessão."""
        self.session.close()

    def __enter__(self):
        """Entrada do gerenciador de contexto."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Saída do gerenciador de contexto."""
        self.close()
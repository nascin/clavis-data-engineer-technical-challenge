#!/usr/bin/env python3
"""
DESAFIO DE ENGENHARIA DE DADOS CLAVIS
Script de Configuração do Kibana - Configuração Automatizada

Este script configura automaticamente o Kibana com:
- Padrões de índice
- Dashboards
- Visualizações
- Alertas/Regras

Autor: Clavis Challenge
"""

import os
import sys
import time
import json
import requests
from pathlib import Path
from typing import Dict, List, Optional


class KibanaSetup:
    """Configuração automatizada do Kibana"""

    def __init__(self):
        self.kibana_host = os.getenv('KIBANA_HOST', 'http://kibana:5601')
        self.elasticsearch_host = os.getenv('ELASTICSEARCH_HOST', 'http://elasticsearch:9200')
        self.max_retries = 30
        self.retry_delay = 10

    def wait_for_service(self, url: str, service_name: str) -> bool:
        """Aguarda o serviço estar pronto"""
        print(f"Aguardando {service_name} ficar pronto...")

        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    print(f"{service_name} está pronto!")
                    return True
            except requests.exceptions.RequestException as e:
                print(f"   Tentativa {attempt + 1}/{self.max_retries}: {service_name} ainda não está pronto...")
                time.sleep(self.retry_delay)

        print(f"{service_name} falhou ao iniciar após {self.max_retries} tentativas")
        return False

    def create_index_pattern(self, pattern: str, time_field: str = '@timestamp', pattern_id: str = None) -> bool:
        """Cria padrão de índice no Kibana com ID específico"""
        print(f"\nCriando padrão de índice: {pattern}")

        # Usa o padrão como ID se não especificado
        if not pattern_id:
            pattern_id = pattern

        url = f"{self.kibana_host}/api/saved_objects/index-pattern/{pattern_id}"
        headers = {
            'kbn-xsrf': 'true',
            'Content-Type': 'application/json'
        }

        # Verifica se o padrão já existe
        try:
            check_response = requests.get(url, headers=headers)
            if check_response.status_code == 200:
                print(f"Padrão de índice '{pattern}' já existe, pulando...")
                return True
        except Exception as e:
            pass  # Padrão não existe, continua com a criação

        payload = {
            'attributes': {
                'title': pattern,
                'timeFieldName': time_field
            }
        }

        try:
            response = requests.post(url, headers=headers, json=payload, params={'overwrite': 'true'}, timeout=10)
            if response.status_code in [200, 201]:
                print(f"   Padrão de índice '{pattern}' criado com sucesso com ID: {pattern_id}")
                return True
            else:
                print(f"   Falha ao criar padrão de índice: {response.status_code}")
                print(f"      Resposta: {response.text}")
                return False
        except Exception as e:
            print(f"   Erro ao criar padrão de índice: {e}")
            return False

    def import_saved_objects(self, file_path: Path) -> bool:
        """Importa objetos salvos de arquivo NDJSON"""
        if not file_path.exists():
            print(f"Arquivo não encontrado: {file_path}")
            return False

        print(f"\nImportando objetos salvos de: {file_path.name}")

        url = f"{self.kibana_host}/api/saved_objects/_import?overwrite=true"
        headers = {'kbn-xsrf': 'true'}

        try:
            with open(file_path, 'rb') as f:
                files = {'file': (file_path.name, f, 'application/ndjson')}
                response = requests.post(url, headers=headers, files=files, timeout=30)

                if response.status_code == 200:
                    result = response.json()
                    if result.get('success'):
                        print(f"   {result.get('successCount', 0)} objetos importados com sucesso")
                        return True
                    else:
                        print(f"   Importação concluída com erros:")
                        for error in result.get('errors', []):
                            print(f"      - {error.get('error', {}).get('message', 'Erro desconhecido')}")
                        return False
                else:
                    print(f"   Importação falhou: {response.status_code}")
                    print(f"      Resposta: {response.text}")
                    return False
        except Exception as e:
            print(f"   Erro ao importar arquivo: {e}")
            return False

    def create_default_index_patterns(self) -> bool:
        """Cria padrões de índice padrão para o projeto"""
        patterns = [
            {
                'pattern': 'crypto-prices-*',
                'time_field': '@timestamp',
                'pattern_id': 'crypto-prices-*',
                'description': 'Preços de criptomoedas da API CoinMarketCap'
            },
            {
                'pattern': 'crypto-global-metrics-*',
                'time_field': '@timestamp',
                'pattern_id': 'crypto-global-metrics-*',
                'description': 'Métricas globais do mercado de criptomoedas'
            },
            {
                'pattern': 'pipeline-metrics-*',
                'time_field': '@timestamp',
                'pattern_id': 'pipeline-metrics-*',
                'description': 'Métricas de execução do pipeline Airflow'
            }
        ]

        success = True
        for pattern_config in patterns:
            result = self.create_index_pattern(
                pattern_config['pattern'],
                pattern_config['time_field'],
                pattern_config['pattern_id']
            )
            if not result:
                success = False

        return success

    def create_alert_rule(self, rule_config: Dict) -> bool:
        """Cria uma regra de alerta no Kibana"""
        print(f"\nCriando alerta: {rule_config['name']}")

        url = f"{self.kibana_host}/api/alerting/rule"
        headers = {
            'kbn-xsrf': 'true',
            'Content-Type': 'application/json'
        }

        try:
            response = requests.post(url, headers=headers, json=rule_config, timeout=30)
            if response.status_code in [200, 201]:
                print(f"   Alerta criado com sucesso")
                return True
            else:
                print(f"   Falha ao criar alerta: {response.status_code}")
                print(f"      Resposta: {response.text}")
                return False
        except Exception as e:
            print(f"   Erro ao criar alerta: {e}")
            return False

    def create_default_alerts(self) -> bool:
        """Cria alertas padrão para monitoramento de criptomoedas"""
        alerts = [
            {
                "name": "Volume Alto - BTC",
                "tags": ["crypto", "volume", "btc"],
                "consumer": "alerts",
                "rule_type_id": ".es-query",
                "schedule": {"interval": "5m"},
                "params": {
                    "index": ["crypto-prices-*"],
                    "timeField": "@timestamp",
                    "esQuery": json.dumps({
                        "query": {
                            "bool": {
                                "must": [
                                    {"term": {"symbol.keyword": "BTC"}},
                                    {"range": {"quotes.USD.volume_24h": {"gte": 50000000000}}}
                                ]
                            }
                        }
                    }),
                    "size": 100,
                    "timeWindowSize": 5,
                    "timeWindowUnit": "m",
                    "thresholdComparator": ">",
                    "threshold": [0]
                },
                "actions": []
            },
            {
                "name": "Variação Brusca de Preço",
                "tags": ["crypto", "price", "volatility"],
                "consumer": "alerts",
                "rule_type_id": ".es-query",
                "schedule": {"interval": "1m"},
                "params": {
                    "index": ["crypto-prices-*"],
                    "timeField": "@timestamp",
                    "esQuery": json.dumps({
                        "query": {
                            "bool": {
                                "should": [
                                    {"range": {"quotes.USD.percent_change_1h": {"gte": 5}}},
                                    {"range": {"quotes.USD.percent_change_1h": {"lte": -5}}}
                                ],
                                "minimum_should_match": 1
                            }
                        }
                    }),
                    "size": 100,
                    "timeWindowSize": 1,
                    "timeWindowUnit": "m",
                    "thresholdComparator": ">",
                    "threshold": [0]
                },
                "actions": []
            },
            {
                "name": "Falha no Pipeline",
                "tags": ["airflow", "pipeline", "failure"],
                "consumer": "alerts",
                "rule_type_id": ".es-query",
                "schedule": {"interval": "1m"},
                "params": {
                    "index": ["pipeline-metrics-*"],
                    "timeField": "@timestamp",
                    "esQuery": json.dumps({
                        "query": {
                            "bool": {
                                "must": [
                                    {"term": {"status.keyword": "failed"}},
                                    {"term": {"dag_id.keyword": "crypto_data_pipeline"}}
                                ]
                            }
                        }
                    }),
                    "size": 100,
                    "timeWindowSize": 1,
                    "timeWindowUnit": "m",
                    "thresholdComparator": ">",
                    "threshold": [0]
                },
                "actions": []
            }
        ]

        success_count = 0
        for alert in alerts:
            if self.create_alert_rule(alert):
                success_count += 1

        print(f"\n   {success_count}/{len(alerts)} alertas criados")
        return success_count > 0

    def setup(self) -> bool:
        """Executa a configuração completa do Kibana"""
        print("=" * 60)
        print("DESAFIO DE ENGENHARIA DE DADOS CLAVIS")
        print("   Configuração Automatizada do Kibana")
        print("=" * 60)

        # Aguarda pelos serviços
        if not self.wait_for_service(f"{self.elasticsearch_host}/_cluster/health", "Elasticsearch"):
            return False

        if not self.wait_for_service(f"{self.kibana_host}/api/status", "Kibana"):
            return False

        # Dá tempo extra para o Kibana inicializar completamente
        print("\nAguardando o Kibana inicializar completamente...")
        time.sleep(15)

        # Cria padrões de índice
        print("\n" + "=" * 60)
        print("ETAPA 1: Criando Padrões de Índice")
        print("=" * 60)
        if not self.create_default_index_patterns():
            print("\n  Alguns padrões de índice falharam ao criar, mas continuando...")

        # Importar objetos salvos
        print("\n" + "=" * 60)
        print("ETAPA 2: Importando Objetos Salvos")
        print("=" * 60)

        saved_objects_dir = Path('/setup/saved-objects')
        if saved_objects_dir.exists():
            ndjson_files = list(saved_objects_dir.glob('*.ndjson'))
            if ndjson_files:
                for ndjson_file in ndjson_files:
                    self.import_saved_objects(ndjson_file)
            else:
                print("  Nenhum objeto salvo para importar (nenhum arquivo .ndjson encontrado)")
        else:
            print("  Diretório de objetos salvos não encontrado, pulando importação")

        # Criar alertas
        print("\n" + "=" * 60)
        print("ETAPA 3: Criando Alertas (Regras)")
        print("=" * 60)
        self.create_default_alerts()

        # Resumo
        print("\n" + "=" * 60)
        print("CONFIGURAÇÃO CONCLUÍDA!")
        print("=" * 60)
        print("\nPontos de Acesso:")
        print(f"   • Interface do Kibana: http://localhost:5601")
        print(f"   • Elasticsearch: http://localhost:9200")
        print(f"   • Interface do Airflow: http://localhost:8080")
        print("\nPróximos Passos:")
        print("   1. Acesse a interface do Airflow e execute o DAG crypto_data_pipeline")
        print("   2. Aguarde a coleta de dados de criptomoedas (~2-3 minutos)")
        print("   3. Acesse o Kibana para visualizar dashboards e visualizações de criptomoedas")
        print("   4. Verifique os alertas no Kibana: Management > Stack Management > Rules")
        print("\nPadrões de Índice Criados:")
        print("   • crypto-prices-*")
        print("   • crypto-global-metrics-*")
        print("   • pipeline-metrics-*")
        print("\nAlertas Configurados:")
        print("   • Volume Alto - BTC (verifica a cada 5 minutos)")
        print("   • Variação Brusca de Preço (verifica a cada 1 minuto)")
        print("   • Falha no Pipeline (verifica a cada 1 minuto)")
        print("\n" + "=" * 60)

        return True


def main():
    """Execução principal"""
    setup = KibanaSetup()

    try:
        success = setup.setup()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nConfiguração interrompida pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nErro inesperado: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()

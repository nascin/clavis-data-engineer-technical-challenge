# 🔔 Sistema de Alertas - Documentação

## 1. Alertas do Kibana

### 1.1 Configuração Inicial

Para habilitar o sistema de alertas no Kibana, foi necessário adicionar chaves de encriptação no arquivo `stack-elk/config/kibana/kibana.yml`:

```yaml
# Configurações de segurança para Alerting
xpack.encryptedSavedObjects.encryptionKey: "clavis_data_engineer_challenge_encryption_key_32chars"
xpack.reporting.encryptionKey: "clavis_reporting_encryption_key_minimum_32_characters"
xpack.security.encryptionKey: "clavis_security_encryption_key_minimum_32_chars"
```

**Por que isso é necessário?**
- O Kibana usa essas chaves para encriptar dados sensíveis dos alertas
- Obrigatório para armazenar credenciais de conectores (email, Slack, webhooks)
- Garante segurança das configurações de alertas no Elasticsearch

### 1.2 Alertas Criados Automaticamente

O script `kibana-setup/scripts/setup-kibana.py` cria automaticamente 3 alertas ao executar o setup:

#### Alerta 1: Volume Alto - BTC
- **Objetivo**: Detectar volume de negociação anormalmente alto do Bitcoin
- **Condição**: `volume_24h > 50 bilhões USD`
- **Frequência**: Verifica a cada 5 minutos
- **Janela de Tempo**: 5 minutos
- **Tags**: `crypto`, `volume`, `btc`

#### Alerta 2: Variação Brusca de Preço
- **Objetivo**: Detectar alta volatilidade no mercado de criptomoedas
- **Condição**: `percent_change_1h >= 5% ou <= -5%`
- **Frequência**: Verifica a cada 1 minuto
- **Janela de Tempo**: 1 minuto
- **Tags**: `crypto`, `price`, `volatility`

#### Alerta 3: Falha no Pipeline
- **Objetivo**: Notificar quando o pipeline do Airflow falha
- **Condição**: `status = "failed"` E `dag_id = "crypto_data_pipeline"`
- **Frequência**: Verifica a cada 1 minuto
- **Janela de Tempo**: 1 minuto
- **Tags**: `airflow`, `pipeline`, `failure`

### 1.3 Estrutura de Configuração dos Alertas

Cada alerta segue esta estrutura:

```python
{
    "name": "Nome do Alerta",
    "tags": ["tag1", "tag2"],
    "consumer": "alerts",                    # Aplicação que consome o alerta
    "rule_type_id": ".es-query",            # Tipo: consulta Elasticsearch
    "schedule": {"interval": "5m"},         # Intervalo de verificação
    "params": {
        "index": ["crypto-prices-*"],       # Índice a monitorar
        "timeField": "@timestamp",          # Campo de tempo
        "esQuery": "...",                   # Query Elasticsearch (JSON)
        "size": 100,                        # Máximo de documentos
        "timeWindowSize": 5,                # Tamanho da janela
        "timeWindowUnit": "m",              # Unidade (m = minutos)
        "thresholdComparator": ">",         # Comparador
        "threshold": [0]                    # Valor limite
    },
    "actions": []                           # Ações ao disparar (vazio = apenas log)
}
```

## 2. Simulação de Email no Airflow

### 2.1 Implementação

Foi adicionada uma simulação de envio de email no callback de falha da DAG `crypto_data_pipeline`.

**Arquivo**: `airflow/dags/crypto_data_pipeline.py` (linhas 97-103)

```python
# Simula envio de email de alerta (em produção, usar SMTP/SendGrid/SES)
logger.warning("=" * 50)
logger.warning("📧 SIMULANDO ENVIO DE EMAIL DE ALERTA")
logger.warning(f"Para: {CryptoConfig.ALERT_EMAIL or 'engenharia-dados@empresa.com'}")
logger.warning(f"Assunto: DAG {dag_id} falhou na task {task_id}")
logger.warning(f"Erro: {exception}")
logger.warning("=" * 50)
```

### 2.2 Quando é Disparado

- Automaticamente quando qualquer task da DAG falha
- Executado pelo callback `on_failure_callback`
- Registra nos logs do Airflow

### 2.3 Como Testar

Para forçar uma falha e ver a simulação:

1. **Desabilitar API Key temporariamente**:
   ```bash
   # No arquivo .env
   COINMARKETCAP_API_KEY=invalid_key
   ```

2. **Executar a DAG no Airflow**:
   - Acesse: `http://localhost:8080`
   - Trigger manual da DAG `crypto_data_pipeline`

3. **Ver os logs**:
   - Clique na task que falhou
   - Veja os logs com a simulação de email

**Exemplo de saída nos logs:**

```
==================================================
📧 SIMULANDO ENVIO DE EMAIL DE ALERTA
Para: engenharia-dados@empresa.com
Assunto: DAG crypto_data_pipeline falhou na task extract_crypto_prices
Erro: API key invalid
==================================================
```

## 3. Integração em Produção

### 3.1 Email Real no Airflow

Para enviar emails reais, configure no `airflow.cfg` ou `docker-compose.yml`:

```yaml
environment:
  AIRFLOW__SMTP__SMTP_HOST: smtp.gmail.com
  AIRFLOW__SMTP__SMTP_PORT: 587
  AIRFLOW__SMTP__SMTP_USER: seu-email@gmail.com
  AIRFLOW__SMTP__SMTP_PASSWORD: sua-senha-app
  AIRFLOW__SMTP__SMTP_MAIL_FROM: seu-email@gmail.com
```

E substitua a simulação por:

```python
from airflow.utils.email import send_email

def on_failure_callback(context):
    send_email(
        to=['engenharia-dados@empresa.com'],
        subject=f"DAG {dag_id} falhou",
        html_content=f"<p>Erro: {exception}</p>"
    )
```

### 3.2 Conectores no Kibana

Para notificações reais do Kibana:

1. **Email** (SMTP/SendGrid/AWS SES):
   - Stack Management → Connectors → Create Connector
   - Escolha "Email"
   - Configure servidor SMTP

2. **Slack**:
   - Crie um Incoming Webhook no Slack
   - Adicione conector "Slack" no Kibana
   - Configure a URL do webhook

3. **Webhook Genérico**:
   - Para integrar com qualquer API
   - Teams, PagerDuty, custom endpoints

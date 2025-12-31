# Clavis Data Engineer Challenge - Cryptocurrency Data Pipeline

Pipeline de dados robusto e escalável para coleta, processamento e visualização de dados de criptomoedas em tempo real.

## Pré-requisitos

- **Docker Desktop** instalado ([Download aqui](https://www.docker.com/products/docker-desktop))
- **CoinMarketCap API Key** gratuita ([Registre-se aqui](https://coinmarketcap.com/api/))

## Como Executar Localmente

### 1. Clone o repositório

```bash
git clone https://github.com/nascin/clavis-data-engineer-technical-challenge.git
cd clavis-data-engineer-technical-challenge
```

### 2. Configure a API Key

O arquivo `.env` já está incluído no repositório para facilitar a execução local.

**IMPORTANTE**: Esta NÃO é uma prática recomendada para produção. Inseri aqui para facilitar a execução do projeto. Para produção use (AWS Secrets Manager, HashiCorp Vault, etc).

Edite o arquivo `.env` e adicione sua CoinMarketCap API Key, caso não esteja configurado:

```bash
COINMARKETCAP_API_KEY=sua-api-key-aqui
```

### 3. Inicie o ambiente

```bash
docker-compose up -d
```

Aguarde aproximadamente 2-3 minutos para todos os serviços iniciarem.

### 4. Configure o Kibana

Para garantir que todos os índices, objetos e alertas sejam criados corretamente:

```bash
docker-compose run --rm kibana-setup
```

Este comando:
- Cria os índices no Elasticsearch
- Importa dashboards e visualizações
- Configura alertas automáticos

## Acessar as Interfaces

Após a inicialização:

| Serviço | URL | Credenciais |
|---------|-----|-------------|
| **Airflow** | http://localhost:8080 | admin / admin |
| **Kibana** | http://localhost:5601 | - |
| **Elasticsearch** | http://localhost:9200 | - |

## Executar o Pipeline

1. Acesse o Airflow em http://localhost:8080
2. Faça login com `admin` / `admin`

O pipeline executará automaticamente a cada 15 minutos. Não precisa nenhuma configuração adicional.

## Visualizar os Dados no Kibana

1. Acesse http://localhost:5601
2. Aguarde 2-3 minutos após a primeira execução do DAG
3. Navegue até **Analytics** → **Discover**
4. Selecione um dos índices:
   - `crypto-prices-*` - Preços e métricas das criptomoedas
   - `crypto-global-metrics-*` - Métricas globais do mercado
   - `pipeline-metrics-*` - Métricas de execução do pipeline

### Dashboards Disponíveis

Os dashboards são criados automaticamente pelo script `kibana-setup`. Se houver erro na criação automática, você pode importar manualmente:

1. Acesse **Stack Management** → **Saved Objects**
2. Clique em **Import**
3. Selecione o arquivo: `kibana-setup/saved-objects/crypto-dashboards.ndjson`
4. Clique em **Import**

## Alertas Configurados

O sistema cria automaticamente 3 alertas para monitoramento:

1. **Volume Alto - BTC**: Detecta volumes de trading acima de 50 bilhões USD
2. **Variação Brusca de Preço**: Alerta para variações maiores que ±5% em 1 hora
3. **Falha no Pipeline**: Notifica falhas na execução do DAG

Para visualizar os alertas:
- Acesse **Stack Management** → **Rules and Connectors**

## Recuperação Manual do Setup

Se o setup automático falhar, execute:

```bash
# Certifique-se que os serviços estão rodando
docker-compose up -d

# Aguarde 2 minutos
# Execute o setup do Kibana
docker-compose run --rm kibana-setup
```

### Dados não aparecem no Kibana

Checklist:
1. DAG foi executado? Verifique no Airflow
2. Arquivos JSON foram criados em `data/raw/`?
3. Elasticsearch tem índices? Execute: `curl http://localhost:9200/_cat/indices`

```bash
# Verificar índices
curl http://localhost:9200/_cat/indices?v

# Deve mostrar:
# crypto-prices-YYYY.MM.DD
# crypto-global-metrics-YYYY.MM.DD
# pipeline-metrics-YYYY.MM.DD
```

### Limpar e reiniciar

```bash
# Parar containers
docker-compose down

# Remover volumes (CUIDADO: apaga dados!)
docker-compose down -v

# Limpar dados locais
rm -rf data/raw/* data/processed/*

# Reiniciar
docker-compose up -d
```

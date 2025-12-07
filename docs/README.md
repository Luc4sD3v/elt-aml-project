# ELT AML Project — Airflow + DuckDB

## Visão Geral

Este projeto implementa um pipeline ELT utilizando Apache Airflow e DuckDB para o processamento de dados de transações financeiras com foco em Anti-Money Laundering (AML).

O pipeline processa o dataset **SAML-D**, um dataset sintético de transações bancárias voltado para pesquisas em monitoramento de lavagem de dinheiro, transformando dados brutos em KPIs analíticos organizados nas camadas Bronze, Silver e Gold.

---

## Dataset

O projeto utiliza o dataset **SAML-D – Synthetic AML Dataset**, publicado em:

> https://www.kaggle.com/datasets/berkanoztas/synthetic-transaction-monitoring-dataset-aml

O dataset contém:
- Mais de 9,5 milhões de transações
- 12 variáveis
- 28 tipologias (normais e suspeitas)
- Apenas ~0,1% das transações são classificadas como suspeitas

⚠️ **O dataset não está no repositório devido ao limite de tamanho do GitHub.**

### Como adicionar o dataset localmente:

1. Baixe o dataset no link acima
2. Crie uma pasta chamada 'raw' dentro da pasta data
3. certifique-se que o arquivo csv se chama 'SAML-D.csv'
---

### Como subir e rodar o airflow:
1. Digitar "docker compose up -d" no bash
2. acessar http://localhost:8080/
3. admin: airflow
4. senha: airflow
5. Executar a DAG "elt_aml_project"


## Arquitetura do Pipeline

O projeto segue uma arquitetura de dados em camadas no formato **Bronze → Silver → Gold**, orquestrada por uma DAG no **Apache Airflow** e utilizando o **DuckDB** como mecanismo de processamento analítico local.

### Visão Geral do Fluxo


- **RAW:** dados brutos conforme fornecidos pela fonte original
- **BRONZE:** cópia inicial e controle da ingestão
- **SILVER:** dados tratados, tipados e estruturados
- **GOLD:** camadas analíticas com KPIs prontos para consumo

---

## Orquestração com Airflow

A orquestração do pipeline é realizada através de uma DAG do Airflow (`elt_projeto_final.py`), que controla a execução sequencial das etapas:

1. **Extract**
2. **Bronze → Silver**
3. **Silver → Gold**

O Airflow permite agendamento, controle de dependências, visualização da execução e reprocessamento das tarefas em caso de falha.

---

## Descrição dos Scripts

### `extract.py` — Ingestão para a Camada Bronze

Responsável por copiar o dataset bruto para a camada Bronze.

Funções principais:
- Leitura do arquivo CSV da pasta `raw`
- Armazenamento na camada `bronze`
- Simula a ingestão de dados de uma fonte externa

---

### `bronze_to_silver.py` — Camada Silver (Transformação)

Responsável por transformar os dados brutos em dados estruturados e preparados para análise.

Principais transformações:
- Conversão de data e hora para timestamp
- Tipagem de campos numéricos
- Padronização de nomes
- Criação do indicador `is_cross_border`
- Filtro de valores inválidos

Tecnologia:
- DuckDB para execução SQL local
- Exportação em formato Parquet

---

### `silver_to_gold.py` — Camada Gold (KPIs Analíticos)

Responsável por gerar métricas analíticas a partir dos dados tratados.

KPIs gerados:
- Taxa de transações suspeitas
- Volume financeiro por país
- Valor médio por tipo de transação
- Ranking de tipologias de lavagem
- Percentual de transações internacionais

---

## Arquivo de Configuração (YAML)

O projeto utiliza arquivo **YAML de configuração** para parametrizar diretórios, caminhos e variáveis do pipeline, permitindo:

- Separação entre código e configuração
- Facilidade de manutenção
- Reaproveitamento do pipeline com novos datasets
- Padronização de ambientes

---

## Tecnologias Utilizadas

- **Apache Airflow** — Orquestração
- **DuckDB** — Engine analítica
- **Python** — Scripts de processamento
- **Parquet** — Armazenamento analítico
- **Git/GitHub** — Versionamento
- **YAML** — Configuração

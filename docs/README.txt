# AML Detection Pipeline com Airflow e DuckDB

Este projeto implementa um pipeline ELT completo para análise e detecção de padrões de lavagem de dinheiro (AML – Anti-Money Laundering), utilizando **Apache Airflow** para orquestração e **DuckDB** como motor analítico para transformação dos dados.

O pipeline segue a arquitetura Bronze → Silver → Gold, garantindo organização, padronização e rastreabilidade. O dataset escolhido contém transações financeiras simuladas com variáveis relevantes para cenários de AML, o que permite criar KPIs realistas usados no setor financeiro.

---

## Estrutura Geral do Pipeline (ELT)

### 1. Extract (Bronze)
O pipeline lê os dados brutos e armazena na camada Bronze.  
A etapa utiliza DuckDB para carregar arquivos CSV com eficiência.

Principais ações:
- Leitura dos dados brutos.
- Limite opcional de linhas para evitar excesso de memória.
- Conversão para Parquet.

### 2. Transform (Silver)
Nesta etapa ocorre o tratamento dos dados:
- Limpeza e padronização.
- Conversão de tipos.
- Criação de colunas derivadas (ex.: flags de lavagem, transação internacional).

A saída também é armazenada como Parquet.

### 3. Analytics / Load (Gold)
O script `silver_to_gold.py` gera indicadores de negócio importantes para AML:

- Taxa de lavagem.  
- Volume financeiro por país.  
- Valor médio por tipo (suspeito vs não suspeito).  
- Ranking de tipologias de lavagem.  
- Percentual de transações internacionais.  

Cada KPI é exportado como um arquivo Parquet individual.

---

## Papel do Airflow

O Airflow orquestra todo o pipeline. Ele define:
- ordem de execução,
- dependências,
- agendamento,
- logs,
- monitoramento visual.

O arquivo `elt_aml_project.py` cria um DAG com três tarefas:

1. `extract_to_bronze`  
2. `bronze_to_silver`  
3. `silver_to_gold`

Cada tarefa executa uma função Python correspondente aos scripts do pipeline.  

Fluxo completo:  
**Extract → Transform Bronze → Transform Silver → Gold**

---

## Papel do DuckDB

DuckDB é um banco analítico local, rápido e leve. Ele executa:
- SQL de transformação,
- leitura de CSV e Parquet,
- criação de tabelas temporárias,
- geração dos arquivos Gold.

Aparece no código em:

```python
con = duckdb.connect()
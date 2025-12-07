# ELT AML Project — Airflow + DuckDB

## Visão Geral

Este projeto implementa um pipeline ELT utilizando Apache Airflow e DuckDB para o processamento de dados de transações financeiras com foco em Anti-Money Laundering (AML).

O pipeline processa o dataset **SAML-D**, um dataset sintético de transações bancárias voltado para pesquisas em monitoramento de lavagem de dinheiro, transformando dados brutos em KPIs analíticos organizados nas camadas Bronze, Silver e Gold.

---

## Dataset

O projeto utiliza o dataset **SAML-D – Synthetic AML Dataset**, publicado em:

> Oztas, B. et al.  
> *Enhancing Anti-Money Laundering: Development of a Synthetic Transaction Monitoring Dataset*  
> IEEE ICEBE 2023  
> https://ieeexplore.ieee.org/document/10356193

O dataset contém:
- Mais de 9,5 milhões de transações
- 12 variáveis
- 28 tipologias (normais e suspeitas)
- Apenas ~0,1% das transações são classificadas como suspeitas

⚠️ **O dataset não está no repositório devido ao limite de tamanho do GitHub.**

### Como adicionar o dataset localmente:

1. Baixe o dataset no link acima
2. Renomeie o arquivo para:

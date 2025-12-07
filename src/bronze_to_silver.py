import duckdb
from pathlib import Path

def run_bronze_to_silver(cfg=None):
    # Informa o início do processo
    print("🚀 Iniciando Bronze → Silver")

    # Caminho do arquivo CSV na camada Bronze
    bronze_path = Path("data/bronze/SAML-D.csv")

    # Diretório de saída da camada Silver
    silver_path = Path("data/silver")

    # Garante que o diretório exista (cria se não existir)
    silver_path.mkdir(parents=True, exist_ok=True)

    # Conexão com o banco DuckDB armazenado na camada Silver
    con = duckdb.connect(database="data/silver/silver.duckdb")

    # Carrega o CSV para uma tabela RAW no DuckDB
    con.execute(f"""
        CREATE OR REPLACE TABLE raw AS
        SELECT *
        FROM read_csv_auto('{bronze_path}')
        LIMIT 1000000  -- Limita a 1 milhão de linhas (se desejar remover, apagar esta linha)
    """)

    # Confirma que o CSV foi carregado
    print("✅ CSV carregado no DuckDB.")

    #padronizada e deixa pronto para análises
    con.execute("""
        CREATE OR REPLACE TABLE silver_transactions AS
        SELECT
            CAST(Date || ' ' || Time AS TIMESTAMP) AS transaction_datetime,
            Sender_account AS sender_account,
            Receiver_account AS receiver_account,
            CAST(Amount AS DOUBLE) AS amount,
            Payment_currency AS payment_currency,
            Received_currency AS received_currency,
            Sender_bank_location AS sender_country,
            Receiver_bank_location AS receiver_country,
            Payment_type AS payment_type,
            CAST(Is_laundering AS INTEGER) AS is_laundering,
            Laundering_type AS laundering_type,
            CASE 
                WHEN Sender_bank_location != Receiver_bank_location THEN 1 ELSE 0 
            END AS is_cross_border
        FROM raw
        WHERE Amount IS NOT NULL
    """)

    print("✅ Transformações aplicadas.")

    # Exporta a tabela final para um arquivo Parquet
    con.execute("""
        COPY silver_transactions
        TO 'data/silver/silver_transactions.parquet'
        (FORMAT PARQUET)
    """)

    print("✅ Camada SILVER gerada com sucesso.")

    # Fecha conexão com o banco DuckDB
    con.close()

# Executa a função se o arquivo for rodado diretamente
if __name__ == "__main__":
    run_bronze_to_silver()

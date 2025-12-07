import duckdb
from pathlib import Path

def run_bronze_to_silver(cfg=None):
    print("🚀 Iniciando Bronze → Silver")

    bronze_path = Path("data/bronze/SAML-D.csv")
    silver_path = Path("data/silver")
    silver_path.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database="data/silver/silver.duckdb")

    con.execute(f"""
        CREATE OR REPLACE TABLE raw AS
        SELECT *
        FROM read_csv_auto('{bronze_path}')
        LIMIT 1000000
    """)

    print("✅ CSV carregado no DuckDB.")

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

    con.execute("""
        COPY silver_transactions
        TO 'data/silver/silver_transactions.parquet'
        (FORMAT PARQUET)
    """)

    print("✅ Camada SILVER gerada com sucesso.")

    con.close()

if __name__ == "__main__":
    run_bronze_to_silver()
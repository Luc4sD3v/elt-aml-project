import duckdb
from pathlib import Path

def run_silver_to_gold(cfg=None):
    print("🚀 Iniciando Silver → Gold")

    silver_path = Path("data/silver/silver_transactions.parquet")
    gold_path = Path("data/gold")
    gold_path.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    #1 KPI: Taxa de lavagem
    con.execute(f"""
        COPY (
            SELECT 
                COUNT(*) AS total_transactions,
                SUM(is_laundering) AS suspicious_transactions,
                SUM(is_laundering)::DOUBLE / COUNT(*) AS laundering_rate
            FROM read_parquet('{silver_path}')
        )
        TO 'data/gold/kpi_laundering_rate.parquet'
        (FORMAT PARQUET)
    """)

    print("KPI 1 gerado")

    #2️ KPI: Volume por país
    con.execute(f"""
        COPY (
            SELECT 
                sender_country,
                SUM(amount) AS total_amount,
                COUNT(*) AS total_transactions
            FROM read_parquet('{silver_path}')
            GROUP BY sender_country
            ORDER BY total_amount DESC
        )
        TO 'data/gold/kpi_volume_by_country.parquet'
        (FORMAT PARQUET)
    """)

    print("KPI 2 gerado")

    #3️ KPI: Valor médio por tipo de transação
    con.execute(f"""
        COPY (
            SELECT 
                is_laundering,
                AVG(amount) AS avg_amount,
                COUNT(*) AS total
            FROM read_parquet('{silver_path}')
            GROUP BY is_laundering
        )
        TO 'data/gold/kpi_avg_amount.parquet'
        (FORMAT PARQUET)
    """)

    print("KPI 3 gerado")

    #4️ KPI: Ranking de tipologias
    con.execute(f"""
        COPY (
            SELECT 
                laundering_type,
                COUNT(*) AS total_cases
            FROM read_parquet('{silver_path}')
            WHERE is_laundering = 1
            GROUP BY laundering_type
            ORDER BY total_cases DESC
        )
        TO 'data/gold/kpi_laundering_typology.parquet'
        (FORMAT PARQUET)
    """)

    print("KPI 4 gerado")

    #5️ KPI: % transações internacionais
    con.execute(f"""
        COPY (
            SELECT
                SUM(is_cross_border) * 1.0 / COUNT(*) AS international_rate
            FROM read_parquet('{silver_path}')
        )
        TO 'data/gold/kpi_cross_border_rate.parquet'
        (FORMAT PARQUET)
    """)

    print("KPI 5 gerado")

    con.close()
    print("Gold gerada com sucesso.")
    

if __name__ == "__main__":
    run_silver_to_gold()

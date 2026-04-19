"""
pipeline.py
ETL/ELT para fintrust: raw → staging → analytics → exportación BI.

Motor: DuckDB (equivalente local a BigQuery).
Carga incremental: basada en loaded_at para payments; para el resto
  se usa UPSERT por clave primaria (INSERT OR REPLACE).
Exportación: una archivo .parquet por cada vista de analytics/,
  en exports/, listo para conectar desde Power BI.

Uso:
    python pipeline.py            # carga completa
    python pipeline.py --incremental  # solo registros nuevos en payments
"""

import argparse
import sys
from pathlib import Path
import duckdb
import validations
import source_data_tables as src

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

DB_PATH   = Path(__file__).parent.parent / "fintrust.duckdb"
SQL_DIR   = Path(__file__).parent.parent / "sql"
EXPORT_DIR = Path(__file__).parent.parent / "exports"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read_sql(relative_path: str) -> str:
    """Lee y retorna el contenido de un archivo SQL relativo a SQL_DIR."""
    return (SQL_DIR / relative_path).read_text(encoding="utf-8")


def exec_sql(con: duckdb.DuckDBPyConnection, path: str) -> None:
    """Ejecuta el SQL de un archivo contra la conexión activa."""
    con.execute(read_sql(path))


def setup_schemas(con: duckdb.DuckDBPyConnection) -> None:
    """Crea los esquemas raw_fintrust, staging y analytics si no existen."""
    for schema in ("raw_fintrust", "staging", "analytics"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


# ---------------------------------------------------------------------------
# Carga de datos
# ---------------------------------------------------------------------------

def load_raw_table(con: duckdb.DuckDBPyConnection, table: str, meta: dict) -> None:
    """Inserta en raw_fintrust.table solo las filas cuya PK no existe aún (upsert por omisión)."""
    cols         = meta["columns"]
    pk           = meta["pk"]
    placeholders = ",".join(["?" for _ in cols.split(",")])

    existing_pks = {r[0] for r in con.execute(
        f"SELECT {pk} FROM raw_fintrust.{table}"
    ).fetchall()}

    new_rows = [r for r in meta["rows"] if r[0] not in existing_pks]
    if new_rows:
        con.executemany(
            f"INSERT INTO raw_fintrust.{table} ({cols}) VALUES ({placeholders})",
            new_rows
        )
    print(f"  raw_fintrust.{table}: {len(new_rows)} filas nuevas insertadas")


def load_payments_incremental(con: duckdb.DuckDBPyConnection, incremental: bool) -> None:
    """Carga payments: incremental inserta solo IDs nuevos; full recarga completa con loaded_at como watermark."""
    if incremental:
        existing = {r[0] for r in con.execute(
            "SELECT payment_id FROM raw_fintrust.payments"
        ).fetchall()}
        new_rows = [r for r in src.PAYMENTS["rows"] if r[0] not in existing]
    else:
        con.execute("DELETE FROM raw_fintrust.payments")
        new_rows = src.PAYMENTS["rows"]

    if new_rows:
        con.executemany(
            """INSERT INTO raw_fintrust.payments
               (payment_id,loan_id,installment_id,payment_date,
                payment_amount,payment_channel,payment_status,loaded_at)
               VALUES (?,?,?,?,?,?,?,CURRENT_TIMESTAMP)""",
            new_rows
        )
    print(f"  raw_fintrust.payments: {len(new_rows)} filas nuevas insertadas")


# ---------------------------------------------------------------------------
# Construcción de staging y analytics (ejecuta los .sql)
# ---------------------------------------------------------------------------

def build_layer(con: duckdb.DuckDBPyConnection, layer: str, files: list[str]) -> None:
    """Ejecuta en orden los archivos SQL de una capa (staging o analytics)."""
    print(f"\n[{layer.upper()}]")
    for f in files:
        exec_sql(con, f)
        print(f"  {f} OK")


# ---------------------------------------------------------------------------
# Exportación para BI
# ---------------------------------------------------------------------------

ANALYTICS_VIEWS = [
    "dm_cartera",
    "vw_daily_snapshot",
    "vw_desembolsos_dia_ciudad_segmento",
    "vw_saldo_segmento",
    "vw_recaudo_mora",
    "vw_cohort_deterioro",
    "vw_top10_creditos_atraso",
]


def export_bi(con: duckdb.DuckDBPyConnection) -> None:
    """Exporta cada vista de analytics/ a un .parquet individual en exports/ para Power BI."""
    EXPORT_DIR.mkdir(exist_ok=True)
    print("\n[EXPORT]")
    for view in ANALYTICS_VIEWS:
        df = con.execute(f"SELECT * FROM analytics.{view}").df()
        path = EXPORT_DIR / f"{view}.parquet"
        df.to_parquet(path, index=False)
        print(f"  {view}.parquet  ({len(df)} filas)")


# ---------------------------------------------------------------------------
# Orquestador principal
# ---------------------------------------------------------------------------

def run(incremental: bool = False) -> None:
    """Orquesta el pipeline completo: DDL → carga raw → calidad → staging → analytics → exportación."""
    print(f"\n{'='*55}")
    print(f"  FINTRUST PIPELINE  |  {'INCREMENTAL' if incremental else 'FULL LOAD'}")
    print(f"{'='*55}")

    con = duckdb.connect(str(DB_PATH))
    setup_schemas(con)

    # 1. DDL raw
    print("\n[RAW — DDL]")
    exec_sql(con, "01-raw/create_raw_tables.sql")

    # 2. Carga raw
    print("\n[RAW — LOAD]")
    for table, meta in {"customers": src.CUSTOMERS, "loans": src.LOANS, "installments": src.INSTALLMENTS}.items():
        load_raw_table(con, table, meta)
    load_payments_incremental(con, incremental)

    # 3. Validaciones de calidad
    print("\n[QUALITY CHECKS]")
    results = validations.run_all(con)
    ok = validations.print_report(results)
    if not ok:
        print("\n[ABORT] Errores críticos en calidad de datos. Pipeline detenido.")
        con.close()
        sys.exit(1)

    # 4. Staging
    build_layer(con, "staging", [
        "02-staging/stg_customers.sql",
        "02-staging/stg_loans.sql",
        "02-staging/stg_installments.sql",
        "02-staging/stg_payments.sql",
    ])

    # 5. Analytics
    build_layer(con, "analytics", [
        "03-analytics/dm_cartera.sql",
        "03-analytics/vw_daily_snapshot.sql",
        "03-analytics/vw_desembolsos_dia_ciudad_segmento.sql",
        "03-analytics/vw_saldo_segmento.sql",
        "03-analytics/vw_recaudo_mora.sql",
        "03-analytics/vw_cohort_deterioro.sql",
        "03-analytics/vw_top10_creditos_atraso.sql",
    ])

    # 6. Exportación BI
    export_bi(con)

    con.close()
    print("\n[DONE] Pipeline completado exitosamente.\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fintrust ETL Pipeline")
    parser.add_argument("--incremental", action="store_true",
                        help="Solo carga registros nuevos en payments")
    args = parser.parse_args()
    run(incremental=args.incremental)

"""
validations.py
Validaciones de calidad de datos sobre las tablas raw.
Retorna un reporte estructurado; no lanza excepciones — el pipeline decide
si detener o continuar según la severidad.
"""

from dataclasses import dataclass, field
from typing import Any
import duckdb


@dataclass
class ValidationResult:
    check: str
    table: str
    severity: str          # ERROR | WARNING | INFO
    passed: bool
    detail: str = ""
    failing_rows: int = 0


def _run(con: duckdb.DuckDBPyConnection, sql: str) -> Any:
    """Ejecuta una query escalar y retorna el primer valor del resultado."""
    return con.execute(sql).fetchone()[0]


# ---------------------------------------------------------------------------
# Checks individuales
# ---------------------------------------------------------------------------

def check_nulls(con, table: str, column: str, severity="ERROR") -> ValidationResult:
    """Verifica que column no tenga valores nulos en la tabla indicada."""
    n = _run(con, f"SELECT COUNT(*) FROM raw_fintrust.{table} WHERE {column} IS NULL")
    return ValidationResult(
        check=f"null_{column}", table=table, severity=severity,
        passed=(n == 0), detail=f"{n} nulls en {column}", failing_rows=n
    )

def check_duplicates(con, table: str, key: str, severity="ERROR") -> ValidationResult:
    """Verifica que key no tenga valores duplicados en la tabla indicada."""
    n = _run(con, f"""
        SELECT COUNT(*) FROM (
            SELECT {key}, COUNT(*) c FROM raw_fintrust.{table}
            GROUP BY {key} HAVING c > 1
        )
    """)
    return ValidationResult(
        check=f"dup_{key}", table=table, severity=severity,
        passed=(n == 0), detail=f"{n} claves duplicadas en {key}", failing_rows=n
    )

def check_positive(con, table: str, column: str, severity="ERROR") -> ValidationResult:
    """Verifica que column solo contenga valores estrictamente positivos (> 0)."""
    n = _run(con, f"SELECT COUNT(*) FROM raw_fintrust.{table} WHERE {column} <= 0")
    return ValidationResult(
        check=f"positive_{column}", table=table, severity=severity,
        passed=(n == 0), detail=f"{n} valores <= 0 en {column}", failing_rows=n
    )

def check_domain(con, table: str, column: str, allowed: list, severity="WARNING") -> ValidationResult:
    """Verifica que todos los valores no nulos de column pertenezcan al conjunto allowed."""
    vals = "','".join(allowed)
    n = _run(con, f"""
        SELECT COUNT(*) FROM raw_fintrust.{table}
        WHERE UPPER(TRIM({column})) NOT IN ('{vals}')
          AND {column} IS NOT NULL
    """)
    return ValidationResult(
        check=f"domain_{column}", table=table, severity=severity,
        passed=(n == 0), detail=f"{n} valores fuera de dominio en {column}", failing_rows=n
    )

def check_referential(con, child_table: str, child_col: str,
                       parent_table: str, parent_col: str, severity="WARNING") -> ValidationResult:
    """Verifica integridad referencial: child_col debe existir en parent_table.parent_col."""
    n = _run(con, f"""
        SELECT COUNT(*) FROM raw_fintrust.{child_table} c
        LEFT JOIN raw_fintrust.{parent_table} p ON c.{child_col} = p.{parent_col}
        WHERE p.{parent_col} IS NULL
    """)
    return ValidationResult(
        check=f"fk_{child_table}_{child_col}", table=child_table, severity=severity,
        passed=(n == 0), detail=f"{n} huérfanos: {child_col} sin match en {parent_table}", failing_rows=n
    )

def check_installment_number(con, severity="WARNING") -> ValidationResult:
    """Verifica que installment_number esté en el rango válido [1, 60]."""
    n = _run(con, """
        SELECT COUNT(*) FROM raw_fintrust.installments
        WHERE installment_number <= 0 OR installment_number > 60
    """)
    return ValidationResult(
        check="installment_number_range", table="installments", severity=severity,
        passed=(n == 0), detail=f"{n} cuotas con número fuera de rango [1,60]", failing_rows=n
    )


# ---------------------------------------------------------------------------
# Suite completa
# ---------------------------------------------------------------------------

def run_all(con: duckdb.DuckDBPyConnection) -> list[ValidationResult]:
    """Ejecuta la suite completa de checks sobre todas las tablas raw y retorna los resultados."""
    return [
        # --- customers ---
        check_nulls(con, "customers", "customer_id"),
        check_nulls(con, "customers", "full_name"),
        check_duplicates(con, "customers", "customer_id"),
        check_positive(con, "customers", "monthly_income"),

        # --- loans ---
        check_nulls(con, "loans", "loan_id"),
        check_nulls(con, "loans", "customer_id"),
        check_duplicates(con, "loans", "loan_id"),
        check_positive(con, "loans", "principal_amount"),
        check_positive(con, "loans", "annual_rate"),
        check_positive(con, "loans", "term_months"),
        check_domain(con, "loans", "loan_status", ["ACTIVE", "CLOSED", "DEFAULT"]),
        check_referential(con, "loans", "customer_id", "customers", "customer_id"),

        # --- installments ---
        check_nulls(con, "installments", "installment_id"),
        check_nulls(con, "installments", "loan_id"),
        check_duplicates(con, "installments", "installment_id"),
        check_installment_number(con),
        check_referential(con, "installments", "loan_id", "loans", "loan_id"),

        # --- payments ---
        check_nulls(con, "payments", "payment_id"),
        check_duplicates(con, "payments", "payment_id"),
        check_domain(con, "payments", "payment_status",
                     ["CONFIRMED", "REVERSED", "PENDING"], severity="INFO"),
        check_referential(con, "payments", "installment_id", "installments", "installment_id",
                          severity="WARNING"),
    ]


def print_report(results: list[ValidationResult]) -> bool:
    """Imprime el resumen de checks y retorna True si no hay errores de severidad ERROR."""
    errors = [r for r in results if not r.passed and r.severity == "ERROR"]
    warnings = [r for r in results if not r.passed and r.severity == "WARNING"]

    print(f"\n{'='*60}")
    print(f"REPORTE DE CALIDAD — {len(results)} checks")
    print(f"  Pasados : {sum(r.passed for r in results)}")
    print(f"  Errores : {len(errors)}")
    print(f"  Warnings: {len(warnings)}")
    print(f"{'='*60}")

    for r in results:
        if not r.passed:
            icon = "X" if r.severity == "ERROR" else "!"
            print(f"  [{r.severity}] {icon} {r.table}.{r.check}: {r.detail}")

    return len(errors) == 0

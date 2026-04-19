-- =============================================================================
-- DATA MART: dm_cartera
-- Granularidad: una fila por cuota (installment)
-- Propósito: fuente única para tableros de cartera, mora y recaudo en BI
-- Consumo: Power BI / Tableau / Looker vía conector DuckDB o exportación CSV
-- =============================================================================

CREATE OR REPLACE VIEW analytics.dm_cartera AS
WITH pagos_por_cuota AS (
    SELECT
        installment_id,
        SUM(payment_amount)  AS total_paid,
        MAX(payment_date)    AS last_payment_date
    FROM staging.stg_payments
    WHERE inconsistency = FALSE
    GROUP BY installment_id
),
cartera_base AS (
    SELECT
        c.customer_id,
        c.full_name,
        c.city,
        c.segment,
        c.monthly_income,

        l.loan_id,
        DATE_TRUNC('month', l.origination_date) AS cohort,   -- derivado de origination_date
        l.origination_date,
        l.principal_amount,
        l.annual_rate,
        l.term_months,
        l.loan_status,
        l.product_type,

        i.installment_id,
        i.installment_number,
        i.due_date,
        i.installment_status,

        i.principal_due,
        i.interest_due,
        (i.principal_due + i.interest_due)          AS total_due,

        COALESCE(p.total_paid, 0)                   AS total_paid,
        p.last_payment_date,

        GREATEST(
            (i.principal_due + i.interest_due) - COALESCE(p.total_paid, 0),
            0
        )                                           AS saldo_pendiente,

        CASE
            WHEN i.installment_status = 'PAID' THEN 0
            ELSE CAST(CURRENT_DATE - i.due_date AS INTEGER)
        END AS dias_atraso,

        CASE
            WHEN i.installment_status = 'PAID'                                     THEN 'AL DIA'
            WHEN CAST(CURRENT_DATE - i.due_date AS INTEGER) BETWEEN 1  AND 30      THEN '1-30'
            WHEN CAST(CURRENT_DATE - i.due_date AS INTEGER) BETWEEN 31 AND 60      THEN '31-60'
            WHEN CAST(CURRENT_DATE - i.due_date AS INTEGER) BETWEEN 61 AND 90      THEN '61-90'
            WHEN CAST(CURRENT_DATE - i.due_date AS INTEGER) > 90                   THEN '>90'
            ELSE 'VIGENTE'
        END AS state_mora

    FROM staging.stg_installments  i
    INNER JOIN staging.stg_loans     l ON i.loan_id     = l.loan_id
    INNER JOIN staging.stg_customers c ON l.customer_id = c.customer_id
    LEFT  JOIN pagos_por_cuota       p ON i.installment_id = p.installment_id
    WHERE i.inconsistency = FALSE
      AND l.inconsistency = FALSE
      AND c.inconsistency = FALSE
)
SELECT
    *,
    CASE WHEN state_mora NOT IN ('AL DIA', 'VIGENTE') THEN TRUE ELSE FALSE END AS en_mora
FROM cartera_base;

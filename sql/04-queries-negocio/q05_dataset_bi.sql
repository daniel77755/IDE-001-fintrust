-- =============================================================================
-- Q05: Dataset maestro para BI
-- Propósito: tabla plana desnormalizada lista para conectar directamente
--            desde Power BI / Tableau / Looker sin joins adicionales.
-- Fuente: dm_cartera
-- Exportación: pipeline.py genera dm_cartera.parquet en exports/
-- =============================================================================

SELECT
    CURRENT_DATE                        AS fecha_corte,

    -- Dimensión cliente
    dm.customer_id,
    dm.full_name,
    dm.city,
    dm.segment,
    dm.monthly_income,

    -- Dimensión crédito
    dm.loan_id,
    dm.cohort,
    dm.origination_date,
    dm.principal_amount,
    dm.annual_rate,
    dm.term_months,
    dm.loan_status,
    dm.product_type,

    -- Dimensión cuota
    dm.installment_id,
    dm.installment_number,
    dm.due_date,
    dm.installment_status,

    -- Métricas financieras
    dm.principal_due,
    dm.interest_due,
    dm.total_due,
    dm.total_paid,
    dm.saldo_pendiente,
    dm.last_payment_date,

    -- Indicadores de mora
    dm.dias_atraso,
    dm.state_mora,
    dm.en_mora,
    CASE
        WHEN dm.total_due > 0 THEN ROUND(dm.total_paid / dm.total_due, 4)
        ELSE NULL
    END                                 AS ratio_cobertura

FROM analytics.dm_cartera dm
ORDER BY
    dm.loan_id,
    dm.installment_number;

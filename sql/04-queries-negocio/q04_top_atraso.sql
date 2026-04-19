-- =============================================================================
-- Q04: Top créditos / clientes en atraso
-- Responde: ¿Quiénes son los clientes con mayor exposición en mora?
-- Uso BI: tabla de gestión de cobranza, ranking de riesgo
-- Fuente: vw_top10_creditos_atraso + dm_cartera para detalle de cliente
-- =============================================================================

SELECT
    t.ranking,
    t.loan_id,
    t.customer_id,
    t.full_name,
    t.city,
    t.segment,
    t.product_type,
    t.loan_status,
    t.origination_date,
    t.principal_amount,
    t.saldo_pendiente_total          AS saldo_en_mora,
    t.max_dias_atraso,
    t.num_cuotas_en_mora,
    t.num_cuotas_total,
    t.peor_bucket_mora               AS state_mora,
    t.pct_saldo_vs_principal,

    -- Ratio mora vs ingreso mensual del cliente (capacidad de pago)
    ROUND(
        t.saldo_pendiente_total / NULLIF(dm.monthly_income, 0),
        2
    )                                AS ratio_mora_ingreso,
    dm.monthly_income,

    -- Conteo de cuotas por estado
    COUNT(CASE WHEN dm.installment_status = 'LATE'    THEN 1 END) AS cuotas_late,
    COUNT(CASE WHEN dm.installment_status = 'PARTIAL' THEN 1 END) AS cuotas_parciales,
    COUNT(CASE WHEN dm.installment_status = 'DUE'     THEN 1 END) AS cuotas_vencidas

FROM analytics.vw_top10_creditos_atraso  t
INNER JOIN analytics.dm_cartera          dm ON t.loan_id = dm.loan_id
GROUP BY
    t.ranking,
    t.loan_id,
    t.customer_id,
    t.full_name,
    t.city,
    t.segment,
    t.product_type,
    t.loan_status,
    t.origination_date,
    t.principal_amount,
    t.saldo_pendiente_total,
    t.max_dias_atraso,
    t.num_cuotas_en_mora,
    t.num_cuotas_total,
    t.peor_bucket_mora,
    t.pct_saldo_vs_principal,
    dm.monthly_income
ORDER BY
    t.ranking;

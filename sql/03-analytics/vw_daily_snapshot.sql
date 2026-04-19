-- =============================================================================
-- VISTA: vw_daily_snapshot
-- Propósito: snapshot diario de KPIs agregados para tablero ejecutivo
-- Granularidad: snapshot_date × product_type × segment × city
-- Fuente: dm_cartera
-- =============================================================================

CREATE OR REPLACE VIEW analytics.vw_daily_snapshot AS
WITH recaudo_hoy AS (
    -- Recaudo del día actual por cuota, para cruzar con dm_cartera
    SELECT
        installment_id,
        SUM(payment_amount) AS recaudo_dia
    FROM staging.stg_payments
    WHERE payment_date = CURRENT_DATE
      AND inconsistency = FALSE
    GROUP BY installment_id
)
SELECT
    CURRENT_DATE                                AS snapshot_date,
    product_type,
    segment,
    city,

    -- -------------------------------------------------------------------------
    -- Desembolso
    -- -------------------------------------------------------------------------
    COUNT(DISTINCT CASE
        WHEN origination_date = CURRENT_DATE THEN loan_id
    END)                                        AS creditos_originados_hoy,

    SUM(CASE
        WHEN origination_date = CURRENT_DATE THEN principal_amount ELSE 0
    END)                                        AS desembolso_hoy,

    -- -------------------------------------------------------------------------
    -- Cartera
    -- -------------------------------------------------------------------------
    COUNT(DISTINCT loan_id)                     AS total_creditos,

    SUM(CASE
        WHEN loan_status = 'ACTIVE' THEN principal_amount ELSE 0
    END)                                        AS cartera_activa,

    SUM(saldo_pendiente)                        AS saldo_pendiente_total,

    -- -------------------------------------------------------------------------
    -- Mora
    -- -------------------------------------------------------------------------
    SUM(CASE WHEN en_mora THEN saldo_pendiente ELSE 0 END)   AS cartera_en_mora,

    COUNT(DISTINCT CASE WHEN en_mora THEN loan_id END)        AS creditos_en_mora,

    SUM(CASE WHEN state_mora = '1-30'  THEN saldo_pendiente ELSE 0 END) AS mora_1_30,
    SUM(CASE WHEN state_mora = '31-60' THEN saldo_pendiente ELSE 0 END) AS mora_31_60,
    SUM(CASE WHEN state_mora = '61-90' THEN saldo_pendiente ELSE 0 END) AS mora_61_90,
    SUM(CASE WHEN state_mora = '>90'   THEN saldo_pendiente ELSE 0 END) AS mora_mayor_90,

    -- Tasa de mora = saldo vencido / cartera activa
    CASE
        WHEN SUM(CASE WHEN loan_status = 'ACTIVE' THEN principal_amount ELSE 0 END) > 0
        THEN ROUND(
            SUM(CASE WHEN en_mora THEN saldo_pendiente ELSE 0 END) /
            SUM(CASE WHEN loan_status = 'ACTIVE' THEN principal_amount ELSE 0 END),
        4)
        ELSE NULL
    END                                         AS tasa_mora,

    -- -------------------------------------------------------------------------
    -- Recaudo del día
    -- -------------------------------------------------------------------------
    SUM(COALESCE(r.recaudo_dia, 0))             AS recaudo_hoy,

    SUM(CASE WHEN en_mora THEN COALESCE(r.recaudo_dia, 0) ELSE 0 END)     AS recaudo_hoy_a_mora,
    SUM(CASE WHEN NOT en_mora THEN COALESCE(r.recaudo_dia, 0) ELSE 0 END) AS recaudo_hoy_a_vigente,

    -- % del recaudo del día aplicado a mora
    CASE
        WHEN SUM(COALESCE(r.recaudo_dia, 0)) > 0
        THEN ROUND(
            SUM(CASE WHEN en_mora THEN COALESCE(r.recaudo_dia, 0) ELSE 0 END) /
            SUM(COALESCE(r.recaudo_dia, 0)),
        4)
        ELSE NULL
    END                                         AS pct_recaudo_a_mora

FROM analytics.dm_cartera
LEFT JOIN recaudo_hoy r USING (installment_id)
GROUP BY
    product_type,
    segment,
    city
ORDER BY
    desembolso_hoy      DESC,
    cartera_en_mora     DESC;

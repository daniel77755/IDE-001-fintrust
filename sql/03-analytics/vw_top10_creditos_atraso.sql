-- =============================================================================
-- VISTA: vw_top10_creditos_atraso
-- Pregunta: Top 10 créditos con mayor atraso y saldo pendiente
-- Granularidad: una fila por crédito (loan_id)
-- Fuente: dm_cartera
-- =============================================================================

CREATE OR REPLACE VIEW analytics.vw_top10_creditos_atraso AS
WITH creditos_mora AS (
    SELECT
        loan_id,
        customer_id,
        full_name,
        city,
        segment,
        product_type,
        origination_date,
        principal_amount,
        loan_status,

        -- Días de atraso máximos del crédito (cuota más antigua sin pagar)
        MAX(dias_atraso)                    AS max_dias_atraso,

        -- Saldo pendiente total del crédito (suma de todas las cuotas impagas)
        SUM(saldo_pendiente)                AS saldo_pendiente_total,

        -- Cuotas en mora del crédito
        COUNT(CASE WHEN en_mora THEN 1 END) AS num_cuotas_en_mora,
        COUNT(installment_id)               AS num_cuotas_total,

        -- Bucket de mora del peor atraso
        MAX(state_mora)                     AS peor_bucket_mora,

        -- % de deuda original que está pendiente
        CASE
            WHEN principal_amount > 0
            THEN ROUND(SUM(saldo_pendiente) / principal_amount, 4)
            ELSE NULL
        END                                 AS pct_saldo_vs_principal

    FROM analytics.dm_cartera
    WHERE en_mora = TRUE
    GROUP BY
        loan_id,
        customer_id,
        full_name,
        city,
        segment,
        product_type,
        origination_date,
        principal_amount,
        loan_status
)
SELECT
    ROW_NUMBER() OVER (
        ORDER BY max_dias_atraso DESC, saldo_pendiente_total DESC
    )                                       AS ranking,
    loan_id,
    customer_id,
    full_name,
    city,
    segment,
    product_type,
    origination_date,
    principal_amount,
    saldo_pendiente_total,
    max_dias_atraso,
    num_cuotas_en_mora,
    num_cuotas_total,
    peor_bucket_mora,
    pct_saldo_vs_principal
FROM creditos_mora
ORDER BY
    max_dias_atraso       DESC,
    saldo_pendiente_total DESC
LIMIT 10;

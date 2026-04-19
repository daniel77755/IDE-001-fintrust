-- =============================================================================
-- VISTA: vw_recaudo_mora
-- Pregunta: ¿Qué porcentaje del recaudo del día cubrió cuotas en mora?
-- Granularidad: payment_date × payment_channel × product_type × segment
-- Fuente: stg_payments + dm_cartera
-- =============================================================================

CREATE OR REPLACE VIEW analytics.vw_recaudo_mora AS
WITH pagos_diarios AS (
    SELECT
        p.payment_date,
        p.payment_id,
        p.loan_id,
        p.payment_amount,
        p.payment_channel,
        dc.product_type,
        dc.segment,
        dc.city,
        dc.en_mora,
        dc.state_mora
    FROM staging.stg_payments       p
    INNER JOIN analytics.dm_cartera dc ON p.installment_id = dc.installment_id
    WHERE p.inconsistency = FALSE
)
SELECT
    payment_date                                                            AS fecha_recaudo,
    payment_channel,
    product_type,
    segment,

    COUNT(DISTINCT payment_id)                                             AS num_pagos,
    COUNT(DISTINCT loan_id)                                                AS num_creditos_pagaron,
    SUM(payment_amount)                                                    AS recaudo_total,
    AVG(payment_amount)                                                    AS pago_promedio,

    SUM(CASE WHEN en_mora     THEN payment_amount ELSE 0 END)             AS recaudo_aplicado_mora,
    SUM(CASE WHEN NOT en_mora THEN payment_amount ELSE 0 END)             AS recaudo_aplicado_vigente,

    COUNT(CASE WHEN en_mora THEN 1 END)                                   AS num_pagos_mora,

    CASE
        WHEN SUM(payment_amount) > 0
        THEN ROUND(
            SUM(CASE WHEN en_mora THEN payment_amount ELSE 0 END)
            / SUM(payment_amount), 4)
        ELSE NULL
    END                                                                    AS pct_recaudo_a_mora,

    -- Desglose por bucket de mora
    SUM(CASE WHEN state_mora = '1-30'  THEN payment_amount ELSE 0 END)   AS recaudo_mora_1_30,
    SUM(CASE WHEN state_mora = '31-60' THEN payment_amount ELSE 0 END)   AS recaudo_mora_31_60,
    SUM(CASE WHEN state_mora = '61-90' THEN payment_amount ELSE 0 END)   AS recaudo_mora_61_90,
    SUM(CASE WHEN state_mora = '>90'   THEN payment_amount ELSE 0 END)   AS recaudo_mora_mayor_90

FROM pagos_diarios
GROUP BY
    payment_date,
    payment_channel,
    product_type,
    segment
ORDER BY
    payment_date   DESC,
    recaudo_total  DESC;

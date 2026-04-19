-- =============================================================================
-- VISTA: vw_saldo_segmento
-- Pregunta: ¿Cuál es el saldo vigente y vencido por segmento?
-- Granularidad: segment × bucket_mora
-- Fuente: dm_cartera
-- =============================================================================

CREATE OR REPLACE VIEW analytics.vw_saldo_segmento AS
SELECT
    segment,
    state_mora,

    COUNT(DISTINCT loan_id)                                         AS num_creditos,
    COUNT(installment_id)                                           AS num_cuotas,

    SUM(saldo_pendiente)                                            AS saldo_pendiente_total,

    -- Saldo vigente: cuotas no vencidas aún (VIGENTE + AL DIA)
    SUM(CASE WHEN state_mora IN ('AL DIA', 'VIGENTE')
             THEN saldo_pendiente ELSE 0 END)                       AS saldo_vigente,

    -- Saldo vencido: cuotas en mora (cualquier bucket de días)
    SUM(CASE WHEN en_mora
             THEN saldo_pendiente ELSE 0 END)                       AS saldo_vencido,

    -- Detalle vencido por bucket
    SUM(CASE WHEN state_mora = '1-30'  THEN saldo_pendiente ELSE 0 END) AS vencido_1_30,
    SUM(CASE WHEN state_mora = '31-60' THEN saldo_pendiente ELSE 0 END) AS vencido_31_60,
    SUM(CASE WHEN state_mora = '61-90' THEN saldo_pendiente ELSE 0 END) AS vencido_61_90,
    SUM(CASE WHEN state_mora = '>90'   THEN saldo_pendiente ELSE 0 END) AS vencido_mayor_90

FROM analytics.dm_cartera
GROUP BY
    segment,
    state_mora
ORDER BY
    segment,
    -- orden lógico de los buckets
    CASE state_mora
        WHEN 'VIGENTE'  THEN 1
        WHEN 'AL DIA'   THEN 2
        WHEN '1-30'     THEN 3
        WHEN '31-60'    THEN 4
        WHEN '61-90'    THEN 5
        WHEN '>90'      THEN 6
        ELSE 99
    END;

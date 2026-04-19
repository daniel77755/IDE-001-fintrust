-- =============================================================================
-- VISTA: vw_cohort_deterioro
-- Pregunta: ¿Qué cohortes muestran mayor deterioro temprano?
-- Granularidad: cohort (mes de originación) × mes_de_vida del crédito
-- Fuente: dm_cartera
-- Lógica: cohort = mes de originación; mes_de_vida = meses transcurridos
--         desde originación hasta due_date de cada cuota.
--         Deterioro temprano = mora en los primeros 3 meses de vida.
-- =============================================================================

CREATE OR REPLACE VIEW analytics.vw_cohort_deterioro AS
WITH cartera_cohort AS (
    SELECT
        cohort,
        loan_id,
        installment_id,
        installment_number,
        due_date,
        origination_date,
        saldo_pendiente,
        principal_amount,
        en_mora,
        state_mora,
        dias_atraso,

        -- Mes de vida: número de mes transcurrido desde la originación
        -- installment_number es un proxy directo del mes de vida en créditos mensuales
        installment_number                                  AS mes_de_vida

    FROM analytics.dm_cartera
),
cohort_stats AS (
    SELECT
        cohort,
        mes_de_vida,

        COUNT(DISTINCT loan_id)                             AS total_creditos_cohorte,
        COUNT(installment_id)                               AS total_cuotas,

        -- Cuotas en mora en ese mes de vida
        COUNT(CASE WHEN en_mora THEN 1 END)                AS cuotas_en_mora,
        COUNT(DISTINCT CASE WHEN en_mora THEN loan_id END) AS creditos_en_mora,

        SUM(principal_amount) / COUNT(DISTINCT loan_id)    AS principal_promedio,
        SUM(saldo_pendiente)                               AS saldo_vencido_total,

        -- Tasa de mora de la cohorte en ese mes de vida
        ROUND(
            COUNT(CASE WHEN en_mora THEN 1 END)::DOUBLE
            / NULLIF(COUNT(installment_id), 0),
        4)                                                  AS tasa_mora_cuotas,

        ROUND(
            COUNT(DISTINCT CASE WHEN en_mora THEN loan_id END)::DOUBLE
            / NULLIF(COUNT(DISTINCT loan_id), 0),
        4)                                                  AS tasa_mora_creditos,

        -- Flag deterioro temprano: mora antes del mes 4
        BOOL_OR(en_mora AND mes_de_vida <= 3)              AS tiene_deterioro_temprano

    FROM cartera_cohort
    GROUP BY
        cohort,
        mes_de_vida
),
cohort_resumen AS (
    -- Resumen por cohorte: tasa de mora promedio en meses 1-3
    SELECT
        cohort,
        ROUND(AVG(CASE WHEN mes_de_vida <= 3 THEN tasa_mora_creditos END), 4) AS tasa_mora_temprana,
        ROUND(AVG(tasa_mora_creditos), 4)                                      AS tasa_mora_total,
        MAX(total_creditos_cohorte)                                            AS total_creditos,
        BOOL_OR(tiene_deterioro_temprano)                                      AS cohorte_con_deterioro_temprano
    FROM cohort_stats
    GROUP BY cohort
)
SELECT
    cs.cohort,
    cs.mes_de_vida,
    cs.total_creditos_cohorte,
    cs.total_cuotas,
    cs.cuotas_en_mora,
    cs.creditos_en_mora,
    cs.saldo_vencido_total,
    cs.tasa_mora_cuotas,
    cs.tasa_mora_creditos,
    cs.tiene_deterioro_temprano,

    -- Datos del resumen de cohorte para ranking
    cr.tasa_mora_temprana,
    cr.tasa_mora_total,
    cr.cohorte_con_deterioro_temprano,

    -- Ranking de cohortes por deterioro temprano (1 = peor)
    RANK() OVER (ORDER BY cr.tasa_mora_temprana DESC NULLS LAST) AS ranking_deterioro_temprano

FROM cohort_stats       cs
INNER JOIN cohort_resumen cr ON cs.cohort = cr.cohort
ORDER BY
    cr.tasa_mora_temprana DESC,
    cs.cohort,
    cs.mes_de_vida;

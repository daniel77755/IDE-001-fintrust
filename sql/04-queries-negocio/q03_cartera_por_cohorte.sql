-- =============================================================================
-- Q03: Cartera por cohorte de originación
-- Responde: ¿Cómo evoluciona la mora de cada cohorte de créditos?
-- Uso BI: heatmap cohorte × bucket de mora, curvas de vintage
-- Fuente: vw_cohort_deterioro (ya tiene la lógica de cohorte y mes de vida)
-- =============================================================================

SELECT
    cohort,
    mes_de_vida,
    state_mora,
    total_creditos_cohorte,
    total_cuotas,
    cuotas_en_mora,
    creditos_en_mora,
    saldo_vencido_total,
    tasa_mora_cuotas,
    tasa_mora_creditos,
    tasa_mora_temprana,
    ranking_deterioro_temprano
FROM analytics.vw_cohort_deterioro
ORDER BY
    cohort,
    mes_de_vida;

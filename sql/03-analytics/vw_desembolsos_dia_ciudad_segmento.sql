-- =============================================================================
-- VISTA: vw_desembolsos_dia_ciudad_segmento
-- Pregunta: ¿Cuánto desembolso se originó por día y ciudad?
-- Granularidad: origination_date × city × segment
-- Fuente: dm_cartera
-- =============================================================================

CREATE OR REPLACE VIEW analytics.vw_desembolsos_dia_ciudad_segmento AS
WITH creditos_unicos AS (
    SELECT DISTINCT
        loan_id,
        origination_date,
        city,
        segment,
        product_type,
        principal_amount
    FROM analytics.dm_cartera
)
SELECT
    origination_date                        AS fecha_desembolso,
    city                                    AS ciudad,
    segment                                 AS segmento,
    COUNT(DISTINCT loan_id)                 AS num_creditos,
    SUM(principal_amount)                   AS total_desembolsado,
    AVG(principal_amount)                   AS ticket_promedio,
    SUM(SUM(principal_amount)) OVER (
        PARTITION BY city
        ORDER BY origination_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                       AS desembolso_acumulado_ciudad
FROM creditos_unicos
GROUP BY
    origination_date,
    city,
    segment
ORDER BY
    origination_date DESC,
    total_desembolsado DESC;

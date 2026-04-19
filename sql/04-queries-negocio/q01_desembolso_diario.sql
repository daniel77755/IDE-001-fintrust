-- =============================================================================
-- Q01: Desembolso diario
-- Responde: ¿Cuánto se desembolsó cada día y en qué ciudad?
-- Uso BI: serie de tiempo + desembolso acumulado por ciudad
-- Fuente: vw_desembolsos_dia_ciudad_segmento
-- =============================================================================

SELECT
    fecha_desembolso,
    ciudad,
    num_creditos,
    total_desembolsado,
    ticket_promedio,
    desembolso_acumulado_ciudad
FROM analytics.vw_desembolsos_dia_ciudad_segmento
ORDER BY
    fecha_desembolso       DESC,
    total_desembolsado     DESC;

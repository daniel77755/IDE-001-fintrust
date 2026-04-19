-- =============================================================================
-- Q02: Recaudo diario
-- Responde: ¿Cuánto se recaudó cada día, por canal y producto?
-- Uso BI: serie de tiempo de recaudo + mix de canales
-- Fuente: vw_recaudo_mora
-- =============================================================================

SELECT
    fecha_recaudo,
    payment_channel,
    product_type,
    segment,
    num_pagos,
    num_creditos_pagaron,
    recaudo_total,
    pago_promedio,
    recaudo_aplicado_mora,
    recaudo_aplicado_vigente,
    pct_recaudo_a_mora
FROM analytics.vw_recaudo_mora
ORDER BY
    fecha_recaudo  DESC,
    recaudo_total  DESC;

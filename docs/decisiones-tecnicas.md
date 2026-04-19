# Decisiones Técnicas — IDE-001-fintrust

## Motor de base de datos: DuckDB en lugar de BigQuery

**Supuesto:** BigQuery no está disponible en el entorno local de ejecución.

**Decisión:** Se usa DuckDB como motor SQL embebido.

**Justificación:**
- DuckDB es columnar, igual que BigQuery. Las queries analíticas son semánticamente idénticas.
- Soporta los mismos tipos conceptuales: `VARCHAR ≈ STRING`, `DECIMAL ≈ NUMERIC`, `BIGINT ≈ INT64`.
- La migración a BigQuery requiere únicamente cambiar el conector en `pipeline.py` (de `duckdb.connect()` a `google.cloud.bigquery.Client()`). El SQL de staging y analytics no cambia salvo el prefijo de esquema.
- Permite persistencia en archivo `.duckdb`, ejecución sin servidor y exportación directa a Parquet.

**Compatibilidad BigQuery preservada:**
- Nombres de esquemas: `raw_fintrust`, `staging`, `analytics` → equivalen a datasets de BQ.
- `DATE_TRUNC('month', ...)` → idéntico en BQ.
- `CURRENT_DATE` → idéntico en BQ.
- `ROW_NUMBER() OVER (PARTITION BY ...)` → idéntico en BQ.
- `QUALIFY` → soportado en BQ desde 2021.

---

## Arquitectura de capas

```
raw_fintrust  →  staging  →  analytics  →  exports/
   (tablas)      (vistas)    (vistas)      (.parquet por vista)
```

- **raw**: datos tal como llegan, sin transformar. Inmutable.
- **staging**: vistas con limpieza, estandarización, validación referencial y flag `inconsistency`.
- **analytics**: data mart base (`dm_cartera`) + 6 vistas temáticas consumibles directamente por BI.
- **exports**: un archivo `.parquet` por cada vista de analytics, listo para Power BI sin joins adicionales.

---

## Carga incremental

**Tabla con carga incremental:** `raw_fintrust.payments`

**Mecanismo:** comparación de `payment_id` existentes contra los nuevos. Solo se insertan registros cuyo ID no existe en la tabla. La columna `loaded_at` actúa como watermark de auditoría.

**Justificación:** payments es la tabla de mayor volumen y frecuencia de actualización (transaccional diaria). Las demás tablas (customers, loans, installments) tienen menor frecuencia y se manejan con upsert por clave primaria.

**Comando incremental:**
```bash
python pipeline.py --incremental
```

---

## Capa analítica para BI

### Data mart base

**Vista:** `analytics.dm_cartera`
- Granularidad: cuota × crédito × cliente
- Incluye: `state_mora` (bucket regulatorio Colombia), `dias_atraso`, `saldo_pendiente`, `en_mora`, `total_due`, `cohort`
- Solo consume registros con `inconsistency = FALSE` de las tres tablas de staging
- Es la única fuente de verdad para todas las demás vistas de analytics

**Decisión — columnas derivadas en dm_cartera, no en staging:**
- `cohort` = `DATE_TRUNC('month', origination_date)`: staging no lo expone porque es un cálculo analítico, no de limpieza.
- `total_due` = `principal_due + interest_due`: idem — es una métrica financiera, no un campo de estandarización.
- Esto preserva el principio de que staging solo limpia y analytics calcula.

**Decisión — renombrar `bucket_mora` a `state_mora`:**
- El término `state_mora` es más descriptivo para consumidores de BI que no conocen la jerga interna.
- Se actualizaron consistentemente todas las vistas y queries dependientes.

### Vistas temáticas

| Vista | Granularidad | Pregunta de negocio |
|---|---|---|
| `vw_daily_snapshot` | fecha × producto × segmento × ciudad | KPIs ejecutivos diarios |
| `vw_desembolsos_dia_ciudad_segmento` | origination_date × ciudad × segmento | ¿Cuánto desembolso por día y ciudad? |
| `vw_saldo_segmento` | segmento × state_mora | ¿Saldo vigente y vencido por segmento? |
| `vw_recaudo_mora` | payment_date × canal × producto × segmento | ¿Qué % del recaudo cubrió mora? |
| `vw_cohort_deterioro` | cohort × mes_de_vida | ¿Qué cohortes muestran deterioro temprano? |
| `vw_top10_creditos_atraso` | loan_id | Top 10 créditos con mayor atraso y saldo |

**Decisión — ampliar `vw_recaudo_mora` con `payment_channel` y `product_type`:**
- La vista original solo tenía granularidad por `payment_date`. Al agregar canal y producto, `q02_recaudo_diario` puede consumirla sin tocar staging, manteniendo la separación de capas.
- Tradeoff: la vista ya no responde solo "% mora del día" sino también "mix de canales". Se acepta porque ambas preguntas pertenecen al mismo dominio de recaudo.

**Decisión — separar `vw_top10_creditos_atraso` de `dm_cartera`:**
- El top 10 podría hacerse con un `SELECT ... LIMIT 10` sobre `dm_cartera`, pero encapsularlo en una vista permite que Power BI lo consuma como tabla fija y que el ranking sea consistente entre reportes.

---

## Queries de negocio (`04-queries-negocio/`)

**Decisión — todos los queries consumen `03-analytics/`, no `02-staging/` directamente:**

| Query | Fuente |
|---|---|
| `q01_desembolso_diario` | `vw_desembolsos_dia_ciudad_segmento` |
| `q02_recaudo_diario` | `vw_recaudo_mora` |
| `q03_cartera_por_cohorte` | `vw_cohort_deterioro` |
| `q04_top_atraso` | `vw_top10_creditos_atraso` + `dm_cartera` |
| `q05_dataset_bi` | `dm_cartera` |

**Justificación:** evita duplicar lógica de negocio (cálculo de mora, cohort, saldo pendiente) en múltiples archivos. Un cambio en una regla de negocio se hace en una sola vista y se propaga automáticamente a todos los queries.

**Excepción — `q04` hace join con `dm_cartera`:** `vw_top10_creditos_atraso` agrega por `loan_id` y pierde el detalle de cuotas individuales (`installment_status`). El join con `dm_cartera` es necesario solo para las columnas `cuotas_late`, `cuotas_parciales`, `cuotas_vencidas` y `monthly_income`.

---

## Exportación para Power BI

**Decisión — un `.parquet` por vista en lugar de un único `bi_dataset`:**
- El archivo original `bi_dataset.parquet` exportaba solo `dm_cartera` sin agregar, obligando a Power BI a hacer todas las agregaciones en memoria.
- Con un parquet por vista, cada visual de Power BI conecta directamente a la tabla ya agregada que necesita, reduciendo el volumen de datos cargado y simplificando el modelo de datos en el lado BI.
- Se eliminaron `bi_dataset.parquet` y `bi_dataset.csv` por quedar redundantes con `dm_cartera.parquet`.

**Archivos generados en `exports/`:**
```
dm_cartera.parquet
vw_daily_snapshot.parquet
vw_desembolsos_dia_ciudad_segmento.parquet
vw_saldo_segmento.parquet
vw_recaudo_mora.parquet
vw_cohort_deterioro.parquet
vw_top10_creditos_atraso.parquet
```

**Conexión recomendada en Power BI:** `Obtener datos → Parquet` apuntando a la carpeta `exports/`. Cada archivo aparece como una tabla independiente sin necesidad de joins en Power BI.

---

## Supuestos de negocio

| Supuesto | Decisión |
|---|---|
| `installment_number = 99` (I135) es un error de carga | Filtrado en staging (rango válido: 1–60) |
| Pagos con `status = REVERSED` no representan recaudo | `inconsistency = TRUE` en stg_payments |
| Pagos con `status = PENDING` no son recaudo confirmado | `inconsistency = TRUE` en stg_payments |
| Pago con `amount = 0` (P106) es error operativo | `inconsistency = TRUE` en stg_payments |
| `installment_id = I999` (P101) no existe | `inconsistency = TRUE`, conservado para auditoría |
| P102 referencia I040 de otro loan (cross-loan) | `inconsistency = TRUE`, conservado para auditoría |
| P104 duplica I051 ya pagado en P041 | Deduplicado por `QUALIFY ROW_NUMBER()` |
| Loans en DEFAULT siguen en el análisis de cartera | Incluidos con `state_mora` correspondiente |
| `annual_rate` expresada como decimal (0.24 = 24%) | Se muestra tal cual; BI aplica formato porcentual |
| Plazo máximo razonable: 60 meses | Límite en validación de `installment_number` |
| `installment_number` es proxy del mes de vida del crédito | Usado en `vw_cohort_deterioro` para calcular `mes_de_vida` (asume cuotas mensuales) |
| Deterioro temprano = mora en los primeros 3 meses | Umbral definido en `vw_cohort_deterioro`; ajustable según política de riesgo |

---

## Agente conversacional LLM (`bonus/LLMs/`)

**Propósito:** permitir a usuarios de negocio consultar en lenguaje natural qué métricas y tablas están disponibles en la capa analytics, sin necesidad de conocer SQL.

**Stack:**
- Interfaz: Streamlit
- LLM: Google Gemini free tier (`gemini-2.5-flash`) vía `google-generativeai`
- Conocimiento: archivo externo `knowledge.txt` (system prompt editable sin tocar código)

**Decisión — conocimiento en archivo separado (`knowledge.txt`):**
- Desacopla el contenido del agente del código de la aplicación.
- Permite actualizar descripciones de tablas o agregar nuevas vistas editando solo `knowledge.txt`, sin modificar `agente.py`.

**Decisión — el agente NO expone SQL ni estructura técnica:**
- El system prompt prohíbe explícitamente revelar nombres de columnas, joins, CTEs o lógica interna.
- Responde únicamente sobre descripciones y métricas de las 7 vistas de analytics.
- Cualquier pregunta fuera de ese alcance devuelve: *"No tengo información al respecto"*.

**Decisión — detección automática de modelo disponible:**
- Al arrancar, `get_model_name()` consulta los modelos activos para la API key y verifica si `gemini-2.5-flash` está disponible.
- Si no lo está, hace fallback a `gemini-2.0-flash` → `gemini-1.5-flash` → primer modelo disponible.
- El modelo resuelto se muestra en el sidebar para transparencia con el usuario.

**Decisión — reintento automático ante cuota agotada (free tier):**
- Hasta 3 intentos con espera progresiva (45s, 90s, 135s) antes de informar al usuario.
- Justificación: el free tier de Gemini tiene límites por minuto; el reintento evita errores visibles en uso normal.

**Ejecución:**
```bash
pip install -r bonus/LLMs/requirements.txt
streamlit run bonus/LLMs/agente.py
```

---

## Riesgos conocidos

1. **Escalabilidad de DuckDB**: para volúmenes > 10M filas conviene migrar a BigQuery o Spark.
2. **Concurrencia**: DuckDB no soporta escrituras concurrentes. Si el pipeline se ejecuta en paralelo, puede haber bloqueos de archivo (error `IOException`).
3. **`dias_atraso` depende de `CURRENT_DATE`**: los resultados de `state_mora` y `en_mora` cambian con el tiempo. Para análisis histórico se debe agregar una tabla de snapshots persistidos con fecha de corte.
4. **Deduplicación de pagos**: el criterio actual es `ROW_NUMBER() = 1` por orden de inserción. Si el negocio prefiere "pago de mayor monto" o "pago más reciente", ajustar el `ORDER BY` en `stg_payments`.
5. **`vw_top10_creditos_atraso` tiene `LIMIT 10` fijo**: si se necesita un top N configurable, debe parametrizarse en el query de negocio (`q04`) en lugar de la vista.
6. **Parquet como formato de exportación**: Power BI Desktop soporta Parquet nativamente desde 2022. Versiones anteriores requieren el conector adicional o exportar a CSV como fallback.

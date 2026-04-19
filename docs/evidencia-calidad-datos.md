# Evidencia de Calidad de Datos — IDE-001-fintrust

## Arquitectura de calidad

La calidad se aplica en dos capas secuenciales:

```
raw_fintrust  →  [validations.py]  →  [02-staging/]  →  analytics
                  Checks sobre raw     Filtrado y         Solo registros
                  Pipeline aborta      estandarización    con inconsistency
                  si hay ERROR         flag por fila      = FALSE
```

---

## Capa 1 — Checks en `validations.py` (sobre tablas raw)

Se ejecutan antes del staging. Si algún check devuelve severidad **ERROR**, el pipeline se detiene.

### customers

| Check | Función | Severidad | Condición que falla |
|---|---|---|---|
| Nulls en `customer_id` | `check_nulls` | ERROR | `customer_id IS NULL` |
| Nulls en `full_name` | `check_nulls` | ERROR | `full_name IS NULL` |
| Duplicados en `customer_id` | `check_duplicates` | ERROR | `COUNT(*) > 1` por `customer_id` |
| Valores positivos en `monthly_income` | `check_positive` | ERROR | `monthly_income <= 0` |

### loans

| Check | Función | Severidad | Condición que falla |
|---|---|---|---|
| Nulls en `loan_id` | `check_nulls` | ERROR | `loan_id IS NULL` |
| Nulls en `customer_id` | `check_nulls` | ERROR | `customer_id IS NULL` |
| Duplicados en `loan_id` | `check_duplicates` | ERROR | `COUNT(*) > 1` por `loan_id` |
| Valores positivos en `principal_amount` | `check_positive` | ERROR | `principal_amount <= 0` |
| Valores positivos en `annual_rate` | `check_positive` | ERROR | `annual_rate <= 0` |
| Valores positivos en `term_months` | `check_positive` | ERROR | `term_months <= 0` |
| Dominio de `loan_status` | `check_domain` | WARNING | Valor fuera de `{ACTIVE, CLOSED, DEFAULT}` |
| Integridad referencial `customer_id` | `check_referential` | WARNING | `customer_id` no existe en `customers` |

### installments

| Check | Función | Severidad | Condición que falla |
|---|---|---|---|
| Nulls en `installment_id` | `check_nulls` | ERROR | `installment_id IS NULL` |
| Nulls en `loan_id` | `check_nulls` | ERROR | `loan_id IS NULL` |
| Duplicados en `installment_id` | `check_duplicates` | ERROR | `COUNT(*) > 1` por `installment_id` |
| Rango de `installment_number` | `check_installment_number` | WARNING | `installment_number <= 0 OR > 60` |
| Integridad referencial `loan_id` | `check_referential` | WARNING | `loan_id` no existe en `loans` |

### payments

| Check | Función | Severidad | Condición que falla |
|---|---|---|---|
| Nulls en `payment_id` | `check_nulls` | ERROR | `payment_id IS NULL` |
| Duplicados en `payment_id` | `check_duplicates` | ERROR | `COUNT(*) > 1` por `payment_id` |
| Dominio de `payment_status` | `check_domain` | INFO | Valor fuera de `{CONFIRMED, REVERSED, PENDING}` |
| Integridad referencial `installment_id` | `check_referential` | WARNING | `installment_id` no existe en `installments` |

---

## Capa 2 — Reglas de staging (sobre `02-staging/`)

Cada staging genera dos conjuntos de registros: `clean` (`inconsistency = FALSE`) y `audit` (`inconsistency = TRUE`). La capa analytics **solo consume registros con `inconsistency = FALSE`**.

### stg_customers

| Regla | Tipo | Transformación |
|---|---|---|
| `customer_id` no nulo ni vacío | Filtro clean | `NULLIF(customer_id, '') IS NOT NULL` |
| `created_at` no nulo | Filtro clean | `created_at IS NOT NULL` |
| Estandarización de `city` | Normalización | `UPPER(TRIM(NULLIF(city, '')))` |
| Estandarización de `segment` | Normalización | `UPPER(TRIM(NULLIF(segment, '')))` |
| Estandarización de `full_name` | Normalización | `TRIM(NULLIF(full_name, ''))` |
| Deduplicación por `customer_id` | Dedup | `QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id) = 1` |

### stg_loans

| Regla | Tipo | Transformación |
|---|---|---|
| `loan_id` no nulo | Filtro clean | `loan_id IS NOT NULL` |
| `annual_rate > 0` | Filtro clean | Excluye tasas cero o negativas |
| `principal_amount > 0` | Filtro clean | Excluye montos cero o negativos |
| `term_months > 0` | Filtro clean | Excluye plazos inválidos |
| `loan_status` no vacío | Filtro clean | `NULLIF(loan_status, '') IS NOT NULL` |
| `product_type` no vacío | Filtro clean | `NULLIF(product_type, '') IS NOT NULL` |
| `customer_id` existe en customers | Integridad referencial | Solo loans con cliente válido en clean |
| Estandarización de `loan_status` | Normalización | `UPPER(TRIM(NULLIF(loan_status, '')))` |
| Estandarización de `product_type` | Normalización | `UPPER(TRIM(NULLIF(product_type, '')))` |
| Deduplicación por `loan_id` | Dedup | `QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_id) = 1` |

### stg_installments

| Regla | Tipo | Transformación |
|---|---|---|
| `installment_id` no nulo ni vacío | Filtro clean | `NULLIF(installment_id, '') IS NOT NULL` |
| `principal_due > 0` | Filtro clean | Excluye cuotas con capital cero |
| `interest_due > 0` | Filtro clean | Excluye cuotas con interés cero |
| `installment_number > 0` | Filtro clean | Excluye números de cuota inválidos |
| `loan_id` existe en loans | Integridad referencial | Solo cuotas con crédito válido en clean |
| Deduplicación por `installment_id` | Dedup | `QUALIFY ROW_NUMBER() OVER (PARTITION BY installment_id) = 1` |

### stg_payments

| Regla | Tipo | Transformación |
|---|---|---|
| `payment_id` no nulo ni vacío | Filtro clean | `NULLIF(payment_id, '') IS NOT NULL` |
| `payment_amount > 0` | Filtro clean | Excluye pagos de monto cero |
| `payment_channel` no vacío | Filtro clean | `NULLIF(payment_channel, '') IS NOT NULL` |
| `loan_id` existe en loans | Integridad referencial | Solo pagos con crédito válido en clean |
| `installment_id` existe en installments | Integridad referencial | Solo pagos con cuota válida en clean |
| `payment_channel` nulo → 'UNKNOWN' | Imputación | `COALESCE(UPPER(TRIM(NULLIF(payment_channel, ''))), 'UNKNOWN')` |
| `payment_status` nulo → 'UNKNOWN' | Imputación | `COALESCE(UPPER(TRIM(NULLIF(payment_status, ''))), 'UNKNOWN')` |
| Deduplicación por `payment_id` | Dedup | `QUALIFY ROW_NUMBER() OVER (PARTITION BY payment_id) = 1` |

---

## Anomalías conocidas en los datos fuente

| ID | Tabla | Registro | Anomalía | Severidad | Acción en staging |
|---|---|---|---|---|---|
| 1 | installments | I135 | `installment_number = 99` (fuera de rango) | WARNING | `inconsistency = TRUE` — excluido de analytics |
| 2 | payments | P101 | `installment_id = I999` no existe | WARNING | `inconsistency = TRUE` — excluido de analytics |
| 3 | payments | P102 | `installment_id = I040` pertenece a L012, no a L013 | WARNING | `inconsistency = TRUE` — excluido de analytics |
| 4 | payments | P102 | `payment_channel = NULL` | INFO | Reemplazado por `'UNKNOWN'` vía `COALESCE` |
| 5 | payments | P103 | `payment_status = REVERSED` | INFO | Pasa staging pero `inconsistency = TRUE` si huérfano |
| 6 | payments | P104 | Mismo loan+installment ya pagado en P041 | WARNING | Deduplicado — `QUALIFY` retiene primera ocurrencia |
| 7 | payments | P105 | `payment_status = PENDING` | INFO | Pasa staging; excluido solo si huérfano |
| 8 | payments | P106 | `payment_amount = 0` | WARNING | `inconsistency = TRUE` — excluido de analytics |
| 9 | payments | P107 | Referencia a I135 (cuota fantasma) | WARNING | `inconsistency = TRUE` — excluido de analytics |
| 10 | loans | L017, L043 | `loan_status = DEFAULT` | INFO | Incluidos — `DEFAULT` está en el dominio permitido |

---

## Impacto cuantitativo

| Tabla | Total raw | Excluidos (inconsistency=TRUE) | Válidos en analytics |
|---|---|---|---|
| customers | — | Nulls en `customer_id` o `created_at` | Solo `inconsistency = FALSE` |
| loans | — | `annual_rate/principal/term = 0`, `customer_id` huérfano | Solo `inconsistency = FALSE` |
| installments | 135 | 1 (I135 — `installment_number = 99`) | 134 |
| payments | 107 | P106 (monto 0), P101/P102/P107 (huérfanos), P104 (dedup) | ~100 |

---

## Comportamiento del pipeline ante fallos

| Severidad | Ejemplo | Acción del pipeline |
|---|---|---|
| ERROR | `customer_id IS NULL` | Detiene ejecución — `sys.exit(1)` |
| WARNING | `installment_number > 60` | Continúa — registra en reporte, fila queda con `inconsistency = TRUE` |
| INFO | `payment_status = PENDING` | Continúa — solo informativo en reporte |

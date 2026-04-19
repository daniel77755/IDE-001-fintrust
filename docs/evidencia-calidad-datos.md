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
| `loan_id` existe en loans | Integridad referencial | Solo pagos con crédito válido en clean |
| `installment_id` existe en installments | Integridad referencial | Solo pagos con cuota válida en clean |
| `loan_id` coincide con el `loan_id` de la cuota | Integridad cruzada (cross-loan) | Subconsulta contra `raw_fintrust.installments`; detecta pagos asignados al crédito incorrecto |
| `payment_channel` nulo → 'UNKNOWN' | Imputación | `COALESCE(UPPER(TRIM(NULLIF(payment_channel, ''))), 'UNKNOWN')` |
| `payment_status` nulo → 'UNKNOWN' | Imputación | `COALESCE(UPPER(TRIM(NULLIF(payment_status, ''))), 'UNKNOWN')` |
| Deduplicación por `payment_id` | Dedup | `QUALIFY ROW_NUMBER() OVER (PARTITION BY payment_id) = 1` |

---

## Anomalías conocidas en los datos fuente

| ID | Tabla | Registro | Anomalía | Severidad | Acción en staging | Llega a analytics |
|---|---|---|---|---|---|---|
| 1 | installments | I135 | `installment_number = 99` (fuera de rango) | WARNING | `inconsistency = TRUE` | No |
| 2 | payments | P101 | `installment_id = I999` no existe en installments | WARNING | `inconsistency = TRUE` | No |
| 3 | payments | P102 | `loan_id = L013` pero `I040` pertenece a `L012` (cross-loan) | WARNING | `inconsistency = TRUE` — detectado por validación cruzada loan/cuota | No |
| 4 | payments | P102 | `payment_channel = NULL` | INFO | Imputado a `'UNKNOWN'` vía `COALESCE` — no impide clean si pasa las demás reglas | No (bloqueado por anomalía 3) |
| 5 | payments | P103 | `payment_status = REVERSED` | INFO | Pasa staging con `inconsistency = FALSE`; no hay regla de dominio en staging | Sí |
| 6 | payments | P104 | Mismo `loan_id + installment_id` ya registrado en P041 | WARNING | Deduplicado — `QUALIFY ROW_NUMBER()` retiene primera ocurrencia (P041) | No |
| 7 | payments | P105 | `payment_status = PENDING` | INFO | Pasa staging con `inconsistency = FALSE` | Sí |
| 8 | payments | P106 | `payment_amount = 0` | WARNING | `inconsistency = TRUE` | No |
| 9 | payments | P107 | Referencia a I135 (cuota fantasma, `installment_number = 99`) | WARNING | `inconsistency = TRUE` — I135 no existe en staging clean | No |
| 10 | loans | L017, L043 | `loan_status = DEFAULT` | INFO | Incluidos — `DEFAULT` está en el dominio permitido | Sí |

---

## Impacto cuantitativo

| Tabla | Total raw | Excluidos (`inconsistency = TRUE`) | Válidos en analytics |
|---|---|---|---|
| customers | 35 | 0 | 35 |
| loans | 45 | 0 | 45 |
| installments | 135 | 1 (I135 — `installment_number = 99`) | 134 |
| payments | 107 | P101 (cuota inexistente), P102 (cross-loan), P104 (dedup), P106 (monto 0), P107 (cuota fantasma) | 102 |

> P103 (`REVERSED`) y P105 (`PENDING`) pasan el filtro de staging y llegan a analytics con `inconsistency = FALSE`. Si se requiere excluirlos, debe agregarse una regla de dominio sobre `payment_status` en `stg_payments`.

---

## Qué llega a la capa analytics (`03-analytics/`)

`dm_cartera` —y todas las vistas que la consumen— filtran explícitamente `inconsistency = FALSE` en las tres tablas de staging que unen:

```sql
WHERE i.inconsistency = FALSE   -- stg_installments
  AND l.inconsistency = FALSE   -- stg_loans
  AND c.inconsistency = FALSE   -- stg_customers
-- stg_payments se filtra en el CTE pagos_por_cuota: WHERE inconsistency = FALSE
```

| Vista analytics | Fuente principal | Registros con inconsistency = TRUE excluidos |
|---|---|---|
| `dm_cartera` | `stg_installments` + `stg_loans` + `stg_customers` + `stg_payments` | Sí — filtro explícito en todas las capas |
| `vw_daily_snapshot` | `dm_cartera` | Sí — hereda filtro de `dm_cartera` |
| `vw_desembolsos_dia_ciudad_segmento` | `dm_cartera` | Sí |
| `vw_saldo_segmento` | `dm_cartera` | Sí |
| `vw_recaudo_mora` | `dm_cartera` | Sí |
| `vw_cohort_deterioro` | `dm_cartera` | Sí |
| `vw_top10_creditos_atraso` | `dm_cartera` | Sí |

---

## Comportamiento del pipeline ante fallos

| Severidad | Ejemplo | Acción del pipeline |
|---|---|---|
| ERROR | `customer_id IS NULL` | Detiene ejecución — `sys.exit(1)` |
| WARNING | `installment_number > 60` | Continúa — registra en reporte, fila queda con `inconsistency = TRUE` |
| INFO | `payment_status = PENDING` | Continúa — solo informativo en reporte |

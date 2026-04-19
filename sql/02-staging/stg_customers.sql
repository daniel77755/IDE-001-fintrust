-- =============================================================================
-- STAGING: stg_customers
-- Fuente: raw_fintrust.customers
-- Limpieza: elimina registros sin customer_id o created_at, estandariza city y
--           segment a UPPER TRIM, deduplica por customer_id (QUALIFY ROW_NUMBER).
-- Flag: inconsistency = TRUE conserva registros con datos faltantes para auditoría.
-- =============================================================================

CREATE OR REPLACE VIEW staging.stg_customers AS
WITH base_customers_clean AS (
    SELECT
    NULLIF(customer_id, '')              AS customer_id,
    TRIM(NULLIF(full_name, ''))          AS full_name,
    UPPER(TRIM(NULLIF(city, '')))        AS city,
    UPPER(TRIM(NULLIF(segment, '')))     AS segment,
    monthly_income,
    created_at,
    FALSE AS inconsistency 
    FROM fintrust.raw_fintrust.customers
    WHERE created_at IS NOT NULL 
      AND customer_id IS NOT NULL
),
base_customers_audit AS (
      SELECT
    NULLIF(customer_id, '')              AS customer_id,
    TRIM(NULLIF(full_name, ''))          AS full_name,
    UPPER(TRIM(NULLIF(city, '')))        AS city,
    UPPER(TRIM(NULLIF(segment, '')))     AS segment,
    monthly_income,
    created_at,
    TRUE AS inconsistency 
    FROM fintrust.raw_fintrust.customers
    WHERE created_at IS NULL 
       OR customer_id IS NULL
)
SELECT * FROM
(SELECT * FROM base_customers_clean
UNION ALL
SELECT * FROM base_customers_audit)
QUALIFY (ROW_NUMBER() OVER (PARTITION BY NULLIF(customer_id, ''))) = 1

-- =============================================================================
-- STAGING: stg_loans
-- Fuente: raw_fintrust.loans
-- Limpieza: excluye créditos con loan_id nulo, annual_rate/principal_amount/
--           term_months = 0, loan_status o product_type vacíos, o customer_id
--           sin match en customers. Estandariza loan_status y product_type a
--           UPPER TRIM. Deduplica por loan_id (QUALIFY ROW_NUMBER).
-- Flag: inconsistency = TRUE conserva registros con incumplimientos para auditoría.
-- =============================================================================

CREATE OR REPLACE VIEW staging.stg_loans AS
WITH base_loans_clean AS (
    SELECT
        NULLIF(loan_id, '') AS loan_id,
        NULLIF(customer_id, '') AS customer_id,
        origination_date,
        principal_amount,
        annual_rate,
        term_months,
        UPPER(TRIM(NULLIF(loan_status, ''))) AS loan_status,
        UPPER(TRIM(NULLIF(product_type, ''))) AS product_type,
        FALSE AS inconsistency 
    FROM fintrust.raw_fintrust.loans
    WHERE loan_id IS NOT NULL
        AND annual_rate    > 0
        AND principal_amount >  0
        AND term_months > 0
        AND NULLIF(loan_status, '') IS NOT NULL
        AND NULLIF(product_type, '') IS NOT NULL
        AND NULLIF(customer_id, '') IN (SELECT DISTINCT NULLIF(customer_id, '') FROM raw_fintrust.customers)
),
base_loans_audit AS (
      SELECT
        NULLIF(loan_id, '') AS loan_id,
        NULLIF(customer_id, '') AS customer_id,
        origination_date,
        principal_amount,
        annual_rate,
        term_months,
        UPPER(TRIM(NULLIF(loan_status, ''))) AS loan_status,
        UPPER(TRIM(NULLIF(product_type, ''))) AS product_type,
        TRUE AS inconsistency 
    FROM fintrust.raw_fintrust.loans
    WHERE loan_id IS NULL
        OR annual_rate      = 0
        OR principal_amount =  0
        OR term_months = 0
        OR NULLIF(loan_status, '') IS NULL
        OR NULLIF(product_type, '') IS NULL
        OR NULLIF(customer_id, '') NOT IN (SELECT DISTINCT NULLIF(customer_id, '') FROM raw_fintrust.customers)
)
SELECT * FROM
(SELECT * FROM base_loans_clean
UNION ALL
SELECT * FROM base_loans_audit)
QUALIFY (ROW_NUMBER() OVER (PARTITION BY NULLIF(loan_id, ''))) = 1
  
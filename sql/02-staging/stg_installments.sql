-- =============================================================================
-- STAGING: stg_installments
-- Fuente: raw_fintrust.installments
-- Limpieza: excluye cuotas con installment_id nulo, principal_due/interest_due/
--           installment_number = 0, o loan_id sin match en loans.
--           Deduplica por installment_id (QUALIFY ROW_NUMBER).
-- Flag: inconsistency = TRUE conserva registros con incumplimientos para auditoría.
-- =============================================================================

CREATE OR REPLACE VIEW staging.stg_installments AS
WITH base_installments_clean AS (
    SELECT
        NULLIF(installment_id, '')       AS installment_id,
        NULLIF(loan_id, '')              AS loan_id,
        installment_number,
        due_date,
        principal_due,
        interest_due,
        installment_status,
        FALSE AS inconsistency 
    FROM fintrust.raw_fintrust.installments
    WHERE principal_due > 0
    AND interest_due > 0
    AND installment_number BETWEEN 1 AND 12
    AND NULLIF(installment_id, '') IS NOT NULL
    AND NULLIF(loan_id, '') IN (SELECT DISTINCT NULLIF(loan_id, '') FROM raw_fintrust.loans)
),
base_installments_audit AS (
    SELECT
        NULLIF(installment_id, '')       AS installment_id,
        NULLIF(loan_id, '')              AS loan_id,
        installment_number,
        due_date,
        principal_due,
        interest_due,
        installment_status,
        TRUE AS inconsistency 
    FROM fintrust.raw_fintrust.installments
    WHERE principal_due = 0
    OR interest_due = 0
    OR installment_number NOT BETWEEN 1 AND 12
    OR NULLIF(installment_id, '') IS NULL
    OR NULLIF(loan_id, '') NOT IN (SELECT DISTINCT NULLIF(loan_id, '') FROM raw_fintrust.loans)
)
SELECT * FROM
(SELECT * FROM base_installments_clean
UNION ALL
SELECT * FROM base_installments_audit)
QUALIFY (ROW_NUMBER() OVER (PARTITION BY NULLIF(installment_id, ''))) = 1;

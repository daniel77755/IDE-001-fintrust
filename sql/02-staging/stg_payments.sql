-- =============================================================================
-- STAGING: stg_payments
-- Fuente: raw_fintrust.payments
-- Limpieza: excluye pagos con payment_id nulo, payment_amount = 0, channel vacío,
--           loan_id/installment_id sin match en sus tablas padre,
--           o loan_id inconsistente con el loan_id registrado en la cuota (cross-loan).
--           Imputa payment_channel y payment_status nulos como 'UNKNOWN'.
--           Deduplica por payment_id (QUALIFY ROW_NUMBER).
-- Flag: inconsistency = TRUE conserva registros con incumplimientos para auditoría.
-- Carga incremental: tabla con mayor frecuencia de actualización (diaria).
-- =============================================================================

CREATE OR REPLACE VIEW staging.stg_payments AS
WITH base_payments_clean AS (
    SELECT
        NULLIF(payment_id, '')       AS payment_id,
        NULLIF(loan_id, '')          AS loan_id,
        NULLIF(installment_id, '')   AS installment_id,
        payment_date,
        payment_amount,
        COALESCE(UPPER(TRIM(NULLIF(payment_channel, ''))), 'UNKNOWN') AS payment_channel,
        COALESCE(UPPER(TRIM(NULLIF(payment_status, ''))), 'UNKNOWN')  AS payment_status,
        loaded_at,
        FALSE AS inconsistency 
    FROM fintrust.raw_fintrust.payments
    WHERE payment_amount > 0
    AND NULLIF(payment_id, '') IS NOT NULL
    AND NULLIF(payment_id, '') IN (SELECT DISTINCT NULLIF(payment_id, '') FROM raw_fintrust.payments)
    AND NULLIF(loan_id, '') IN (SELECT DISTINCT NULLIF(loan_id, '') FROM raw_fintrust.loans)
    AND NULLIF(installment_id, '') IN (SELECT DISTINCT NULLIF(installment_id, '') FROM raw_fintrust.installments)
    AND NULLIF(loan_id, '') = (
        SELECT loan_id FROM raw_fintrust.installments
        WHERE installment_id = NULLIF(payments.installment_id, '')
    )
),
base_payments_audit AS (
    SELECT
        NULLIF(payment_id, '')       AS payment_id,
        NULLIF(loan_id, '')          AS loan_id,
        NULLIF(installment_id, '')   AS installment_id,
        payment_date,
        payment_amount,
        COALESCE(UPPER(TRIM(NULLIF(payment_channel, ''))), 'UNKNOWN') AS payment_channel,
        COALESCE(UPPER(TRIM(NULLIF(payment_status, ''))), 'UNKNOWN')  AS payment_status,
        loaded_at,
        TRUE AS inconsistency
    FROM fintrust.raw_fintrust.payments
    WHERE payment_amount = 0
    OR NULLIF(payment_id, '') IS NULL
    OR NULLIF(payment_id, '') NOT IN (SELECT DISTINCT NULLIF(payment_id, '') FROM raw_fintrust.payments)
    OR NULLIF(loan_id, '') NOT IN (SELECT DISTINCT NULLIF(loan_id, '') FROM raw_fintrust.loans)
    OR NULLIF(installment_id, '') NOT IN (SELECT DISTINCT NULLIF(installment_id, '') FROM raw_fintrust.installments)
    OR NULLIF(loan_id, '') != (
        SELECT loan_id FROM raw_fintrust.installments
        WHERE installment_id = NULLIF(payments.installment_id, '')
    )
)
SELECT * FROM
(SELECT * FROM base_payments_clean
UNION ALL
SELECT * FROM base_payments_audit)
QUALIFY (ROW_NUMBER() OVER (PARTITION BY NULLIF(payment_id, ''))) = 1;

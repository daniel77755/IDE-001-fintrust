-- =============================================================================
-- CAPA RAW: raw_fintrust
-- Motor: DuckDB (compatibilidad conceptual con BigQuery)
-- Decisión: BigQuery no disponible localmente; DuckDB replica el modelo
--           columnar, soporta tipos equivalentes (VARCHAR≈STRING,
--           DECIMAL≈NUMERIC, BIGINT≈INT64) y permite migración directa
--           cambiando solo el conector en pipeline.py.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS raw_fintrust;

-- ----------------------------------------------------------------------------
-- customers: clientes del sistema
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_fintrust.customers (
    customer_id     VARCHAR,
    full_name       VARCHAR,
    city            VARCHAR,
    segment         VARCHAR,
    monthly_income  DECIMAL(18,2),
    created_at      DATE
);

-- ----------------------------------------------------------------------------
-- loans: créditos desembolsados
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_fintrust.loans (
    loan_id           VARCHAR,
    customer_id       VARCHAR,
    origination_date  DATE,
    principal_amount  DECIMAL(18,2),
    annual_rate       DECIMAL(6,4),
    term_months       BIGINT,
    loan_status       VARCHAR,
    product_type      VARCHAR
);

-- ----------------------------------------------------------------------------
-- installments: plan de cuotas programadas
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_fintrust.installments (
    installment_id      VARCHAR,
    loan_id             VARCHAR,
    installment_number  BIGINT,
    due_date            DATE,
    principal_due       DECIMAL(18,2),
    interest_due        DECIMAL(18,2),
    installment_status  VARCHAR
);

-- ----------------------------------------------------------------------------
-- payments: pagos recibidos
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_fintrust.payments (
    payment_id       VARCHAR,
    loan_id          VARCHAR,
    installment_id   VARCHAR,
    payment_date     DATE,
    payment_amount   DECIMAL(18,2),
    payment_channel  VARCHAR,
    payment_status   VARCHAR,
    loaded_at        TIMESTAMP
);

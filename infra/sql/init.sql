-- DLQ Medic schema. Idempotent: safe to run on every `docker compose up`.
-- Variables supplied by sqlcmd -v: ORDER_SVC_PASSWORD, DLQ_MEDIC_PASSWORD
SET NOCOUNT ON;
GO

IF DB_ID('orders_db') IS NULL CREATE DATABASE orders_db;
GO
USE orders_db;
GO

-- ── Business tables (owned by order-consumer) ─────────────────────────────
IF OBJECT_ID('dbo.orders') IS NULL
CREATE TABLE dbo.orders (
    order_id         VARCHAR(40)    NOT NULL PRIMARY KEY,
    customer_id      VARCHAR(40)    NOT NULL,
    amount           DECIMAL(12, 2) NOT NULL,
    currency         CHAR(3)        NOT NULL,
    created_at       DATETIMEOFFSET NOT NULL,
    source_topic     VARCHAR(100)   NOT NULL,
    source_partition INT            NOT NULL,
    source_offset    BIGINT         NOT NULL,
    processed_at     DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);

-- Deliberately NO unique constraint on order_id: every processed event appends a charge.
-- This is the non-idempotent side effect that makes a blind DLT replay a double charge.
IF OBJECT_ID('dbo.payment_ledger') IS NULL
CREATE TABLE dbo.payment_ledger (
    ledger_id  BIGINT IDENTITY PRIMARY KEY,
    order_id   VARCHAR(40)    NOT NULL,
    amount     DECIMAL(12, 2) NOT NULL,
    charged_at DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_payment_ledger_order')
CREATE INDEX ix_payment_ledger_order ON dbo.payment_ledger (order_id);

-- ── Agent tables (owned by dlq-medic MCP server) ──────────────────────────
IF OBJECT_ID('dbo.replay_batch') IS NULL
CREATE TABLE dbo.replay_batch (
    batch_id   VARCHAR(40)   NOT NULL PRIMARY KEY,
    status     VARCHAR(20)   NOT NULL,  -- STAGED | CANARY_SENT | COMPLETED
    reason     NVARCHAR(400) NOT NULL,
    item_count INT           NOT NULL,
    created_at DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('dbo.replay_item') IS NULL
CREATE TABLE dbo.replay_item (
    batch_id      VARCHAR(40)   NOT NULL REFERENCES dbo.replay_batch (batch_id),
    message_id    VARCHAR(40)   NOT NULL,  -- "<dlt partition>:<dlt offset>"
    order_id      VARCHAR(40)   NOT NULL,
    fixed_payload NVARCHAR(MAX) NOT NULL,
    status        VARCHAR(30)   NOT NULL,  -- STAGED | SENT | SKIPPED_ALREADY_PROCESSED
    sent_at       DATETIME2     NULL,
    error_pattern NVARCHAR(300) NULL,      -- canonical error type, so the canary covers every type
    fix           VARCHAR(60)   NULL,      -- vetted fix applied
    PRIMARY KEY (batch_id, message_id)
);
IF COL_LENGTH('dbo.replay_item', 'error_pattern') IS NULL ALTER TABLE dbo.replay_item ADD error_pattern NVARCHAR(300) NULL;
IF COL_LENGTH('dbo.replay_item', 'fix') IS NULL ALTER TABLE dbo.replay_item ADD fix VARCHAR(60) NULL;

IF OBJECT_ID('dbo.agent_audit_log') IS NULL
CREATE TABLE dbo.agent_audit_log (
    audit_id BIGINT IDENTITY PRIMARY KEY,
    at       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    tool     VARCHAR(60)   NOT NULL,
    args     NVARCHAR(MAX) NULL,
    outcome  VARCHAR(20)   NOT NULL,  -- OK | REJECTED | ERROR
    detail   NVARCHAR(MAX) NULL
);

-- Long-term incident memory. Survives demo resets; append-only for the agent.
IF OBJECT_ID('dbo.incident_memory') IS NULL
CREATE TABLE dbo.incident_memory (
    incident_id       BIGINT IDENTITY PRIMARY KEY,
    recorded_at       DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    batch_id          VARCHAR(40)    NULL,
    producer_versions NVARCHAR(200)  NOT NULL,
    patterns          NVARCHAR(MAX)  NOT NULL,  -- JSON [{errorPattern, count, action}], validated against the DLT
    root_cause        NVARCHAR(1000) NOT NULL,
    outcome           NVARCHAR(400)  NOT NULL,  -- JSON computed by the server from the batch, not by the model
    human_decisions   NVARCHAR(2000) NULL,
    lessons           NVARCHAR(2000) NULL
);

-- Emails the agent sent (after human approval). One per replay batch.
IF OBJECT_ID('dbo.notification_log') IS NULL
CREATE TABLE dbo.notification_log (
    notification_id BIGINT IDENTITY PRIMARY KEY,
    sent_at         DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    batch_id        VARCHAR(40)   NULL,
    team            VARCHAR(60)   NOT NULL,
    recipient       VARCHAR(200)  NOT NULL,
    subject         NVARCHAR(200) NOT NULL,
    parked_rows     INT           NOT NULL
);
GO

-- ── Least-privilege identities ────────────────────────────────────────────
USE master;
IF SUSER_ID('order_svc') IS NULL
    CREATE LOGIN order_svc WITH PASSWORD = '$(ORDER_SVC_PASSWORD)', CHECK_POLICY = ON;
ELSE
    ALTER LOGIN order_svc WITH PASSWORD = '$(ORDER_SVC_PASSWORD)';
IF SUSER_ID('dlq_medic') IS NULL
    CREATE LOGIN dlq_medic WITH PASSWORD = '$(DLQ_MEDIC_PASSWORD)', CHECK_POLICY = ON;
ELSE
    ALTER LOGIN dlq_medic WITH PASSWORD = '$(DLQ_MEDIC_PASSWORD)';
GO
USE orders_db;
IF USER_ID('order_svc') IS NULL CREATE USER order_svc FOR LOGIN order_svc;
IF USER_ID('dlq_medic') IS NULL CREATE USER dlq_medic FOR LOGIN dlq_medic;
GO

-- order-consumer: may upsert orders and append charges. Nothing else.
GRANT SELECT, INSERT, UPDATE ON dbo.orders         TO order_svc;
GRANT SELECT, INSERT         ON dbo.payment_ledger TO order_svc;

-- dlq-medic (the agent's MCP server): READ-ONLY on business data,
-- read/write on its own replay tables, APPEND-ONLY audit log.
GRANT SELECT                 ON dbo.orders          TO dlq_medic;
GRANT SELECT                 ON dbo.payment_ledger  TO dlq_medic;
GRANT SELECT, INSERT, UPDATE ON dbo.replay_batch    TO dlq_medic;
GRANT SELECT, INSERT, UPDATE ON dbo.replay_item     TO dlq_medic;
GRANT SELECT, INSERT         ON dbo.agent_audit_log TO dlq_medic;
DENY  UPDATE, DELETE         ON dbo.agent_audit_log TO dlq_medic;
GRANT SELECT, INSERT         ON dbo.incident_memory TO dlq_medic;
DENY  UPDATE, DELETE         ON dbo.incident_memory TO dlq_medic;
GRANT SELECT, INSERT         ON dbo.notification_log TO dlq_medic;
DENY  UPDATE, DELETE         ON dbo.notification_log TO dlq_medic;
DENY  INSERT, UPDATE, DELETE ON dbo.orders          TO dlq_medic;
DENY  INSERT, UPDATE, DELETE ON dbo.payment_ledger  TO dlq_medic;
GO

PRINT 'orders_db ready';
GO

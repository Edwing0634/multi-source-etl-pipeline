-- ============================================================
-- Staging schema and tables for multi-source ETL pipeline
-- Run once on the target SQL Server database
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;
GO

-- SFTP: policy records from flat file drops
CREATE TABLE staging.sftp_policies (
    record_id      INT,
    client_code    VARCHAR(20)     NOT NULL,
    policy_number  VARCHAR(30)     NOT NULL,
    premium_amount DECIMAL(18, 2),
    status         VARCHAR(20),
    effective_date DATE,
    source         VARCHAR(50),
    _etl_source    VARCHAR(50),
    _etl_loaded_at DATETIME2       DEFAULT GETDATE(),
    CONSTRAINT pk_sftp_policies PRIMARY KEY (policy_number)
);
GO

-- API: survey responses (Qualtrics-style)
CREATE TABLE staging.survey_responses (
    response_id        VARCHAR(20)  NOT NULL,
    recorded_date      DATE,
    q1_satisfaction    TINYINT,
    q2_recommend       TINYINT,
    q3_comment         NVARCHAR(500),
    source             VARCHAR(50),
    _etl_source        VARCHAR(50),
    _etl_loaded_at     DATETIME2    DEFAULT GETDATE(),
    CONSTRAINT pk_survey_responses PRIMARY KEY (response_id)
);
GO

-- AS400: policy master from legacy system
CREATE TABLE staging.as400_policies (
    record_id    INT,
    policy_num   VARCHAR(30)     NOT NULL,
    client_code  VARCHAR(20),
    premium      DECIMAL(18, 2),
    issue_date   DATE,
    status_code  VARCHAR(20),
    table_source VARCHAR(50),
    _etl_source  VARCHAR(50),
    _etl_loaded_at DATETIME2    DEFAULT GETDATE(),
    CONSTRAINT pk_as400_policies PRIMARY KEY (policy_num)
);
GO

-- API: SMS delivery tracking
CREATE TABLE staging.sms_delivery_log (
    id             INT IDENTITY(1,1) PRIMARY KEY,
    message_id     VARCHAR(30)  NOT NULL,
    recipient      VARCHAR(20),
    sent_at        DATETIME2,
    status         VARCHAR(30),
    error_code     VARCHAR(20),
    source         VARCHAR(50),
    _etl_source    VARCHAR(50),
    _etl_loaded_at DATETIME2    DEFAULT GETDATE()
);
GO

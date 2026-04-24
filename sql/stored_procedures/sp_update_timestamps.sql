-- ============================================================
-- sp_update_timestamps
-- Updates _etl_loaded_at on rows that were reprocessed,
-- and logs the run in the etl_run_log table.
-- ============================================================

CREATE OR ALTER PROCEDURE dbo.sp_update_timestamps
    @table_name  NVARCHAR(128),
    @source_name NVARCHAR(50),
    @rows_loaded INT
AS
BEGIN
    SET NOCOUNT ON;

    -- Log the ETL run
    IF NOT EXISTS (
        SELECT 1 FROM sys.tables
        WHERE name = 'etl_run_log' AND schema_id = SCHEMA_ID('dbo')
    )
    BEGIN
        CREATE TABLE dbo.etl_run_log (
            log_id       INT IDENTITY(1,1) PRIMARY KEY,
            run_date     DATETIME2        DEFAULT GETDATE(),
            table_name   NVARCHAR(128),
            source_name  NVARCHAR(50),
            rows_loaded  INT,
            status       VARCHAR(10)      DEFAULT 'OK'
        );
    END;

    INSERT INTO dbo.etl_run_log (table_name, source_name, rows_loaded)
    VALUES (@table_name, @source_name, @rows_loaded);

    -- Refresh _etl_loaded_at for rows loaded in this batch
    DECLARE @sql NVARCHAR(MAX) =
        N'UPDATE ' + QUOTENAME(@table_name) +
        N' SET _etl_loaded_at = GETDATE()' +
        N' WHERE _etl_source = @src AND _etl_loaded_at >= DATEADD(MINUTE, -5, GETDATE())';

    EXEC sp_executesql @sql, N'@src NVARCHAR(50)', @src = @source_name;

    SELECT @@ROWCOUNT UpdatedRows;
END;
GO

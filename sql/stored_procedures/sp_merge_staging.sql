-- ============================================================
-- sp_merge_staging
-- Merges records from staging into the production table,
-- deduplicating by policy_number and keeping the latest record.
-- ============================================================

CREATE OR ALTER PROCEDURE dbo.sp_merge_staging
    @source_table  NVARCHAR(128),
    @target_table  NVARCHAR(128),
    @merge_key     NVARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql NVARCHAR(MAX);
    DECLARE @rows_affected INT;

    SET @sql = N'
        MERGE ' + QUOTENAME(@target_table) + N' AS target
        USING (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY ' + QUOTENAME(@merge_key) + N'
                       ORDER BY _etl_loaded_at DESC
                   ) row_num
            FROM ' + QUOTENAME(@source_table) + N'
        ) AS source
           ON source.' + QUOTENAME(@merge_key) + N' = target.' + QUOTENAME(@merge_key) + N'
          AND source.row_num = 1
        WHEN MATCHED THEN
            UPDATE SET
                target._etl_loaded_at = source._etl_loaded_at,
                target._etl_source    = source._etl_source
        WHEN NOT MATCHED BY TARGET AND source.row_num = 1 THEN
            INSERT SELECT * FROM source WHERE row_num = 1;
    ';

    EXEC sp_executesql @sql;
    SET @rows_affected = @@ROWCOUNT;

    SELECT @rows_affected RowsAffected;
END;
GO

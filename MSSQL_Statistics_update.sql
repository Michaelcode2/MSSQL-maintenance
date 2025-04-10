-- SQL Script for Database Statistics Maintenance
-- This script will update statistics for all user tables in the database
-- with options for full scan or sampling based on table size

-- Set the target database
USE KUPDIK;

-- Set nocount on to reduce output messages
SET NOCOUNT ON;

-- Create a temporary table to store table information
IF OBJECT_ID('tempdb..#TableStats') IS NOT NULL
    DROP TABLE #TableStats;

CREATE TABLE #TableStats (
    SchemaName NVARCHAR(128),
    TableName NVARCHAR(128),
    TableRowCount BIGINT,
    HasLOBColumns BIT,
    LastStatsUpdate DATETIME,
    StatisticsUpdateType NVARCHAR(20)
);

-- Create a temporary table to store statistics information
IF OBJECT_ID('tempdb..#StatsInfo') IS NOT NULL
    DROP TABLE #StatsInfo;

CREATE TABLE #StatsInfo (
    TableID INT,
    StatsID INT,
    LastUpdated DATETIME
);

-- Variables for processing
DECLARE @SchemaName NVARCHAR(128);
DECLARE @TableName NVARCHAR(128);
DECLARE @TableRowCount BIGINT;
DECLARE @StatisticsUpdateType NVARCHAR(20);
DECLARE @SQL NVARCHAR(MAX);
DECLARE @DatabaseName NVARCHAR(128) = DB_NAME();
DECLARE @RowCountThreshold BIGINT = 1000000; -- Tables larger than this will use sampling
DECLARE @SamplePercent INT = 30; -- Sampling percentage for large tables

-- Print script start info
PRINT '-- Starting statistics maintenance on database: ' + @DatabaseName;
PRINT '-- ' + CONVERT(NVARCHAR(30), GETDATE(), 120);
PRINT '';

-- Get statistics last update dates for all statistics
INSERT INTO #StatsInfo (TableID, StatsID, LastUpdated)
SELECT 
    object_id AS TableID,
    stats_id AS StatsID,
    STATS_DATE(object_id, stats_id) AS LastUpdated
FROM 
    sys.stats
WHERE 
    OBJECT_SCHEMA_NAME(object_id) != 'sys';

-- Get table information including row counts and LOB column presence
INSERT INTO #TableStats (SchemaName, TableName, TableRowCount, HasLOBColumns, LastStatsUpdate)
SELECT 
    s.name AS SchemaName,
    t.name AS TableName,
    ISNULL(SUM(p.rows), 0) AS TableRowCount,
    CASE WHEN EXISTS (
        SELECT 1 
        FROM sys.columns c
        JOIN sys.types ty ON c.system_type_id = ty.system_type_id
        WHERE c.object_id = t.object_id
        AND (ty.name IN ('text', 'ntext', 'image', 'varchar(max)', 'nvarchar(max)', 'varbinary(max)', 'xml') 
            OR ty.name LIKE '%blob')
    ) THEN 1 ELSE 0 END AS HasLOBColumns,
    (
        SELECT MAX(LastUpdated)
        FROM #StatsInfo
        WHERE TableID = t.object_id
    ) AS LastStatsUpdate
FROM 
    sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    JOIN sys.indexes i ON t.object_id = i.object_id
    JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
WHERE 
    t.is_ms_shipped = 0 -- Skip system tables
    AND t.type = 'U' -- User tables only
GROUP BY 
    s.name, t.name, t.object_id;

-- Determine update method based on table size
UPDATE #TableStats
SET StatisticsUpdateType = 
    CASE 
        WHEN TableRowCount > @RowCountThreshold THEN 'SAMPLE'
        ELSE 'FULLSCAN'
    END;

-- Show summary of tables and their update methods
PRINT '-- Statistics Update Summary:';
PRINT '';
SELECT 
    SchemaName,
    TableName,
    TableRowCount,
    CONVERT(VARCHAR(20), LastStatsUpdate, 120) AS LastStatsUpdate,
    StatisticsUpdateType
FROM 
    #TableStats
ORDER BY 
    TableRowCount DESC;
PRINT '';

-- Create cursor to process each table
DECLARE TableCursor CURSOR FOR
SELECT 
    SchemaName,
    TableName,
    TableRowCount,
    StatisticsUpdateType
FROM 
    #TableStats
ORDER BY 
    TableRowCount DESC; -- Process larger tables first

-- Open cursor and start processing tables
OPEN TableCursor;
FETCH NEXT FROM TableCursor INTO @SchemaName, @TableName, @TableRowCount, @StatisticsUpdateType;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Build the UPDATE STATISTICS command with appropriate options
    IF @StatisticsUpdateType = 'FULLSCAN'
        SET @SQL = 'UPDATE STATISTICS [' + @SchemaName + '].[' + @TableName + '] WITH FULLSCAN';
    ELSE
        SET @SQL = 'UPDATE STATISTICS [' + @SchemaName + '].[' + @TableName + '] WITH SAMPLE ' + CAST(@SamplePercent AS NVARCHAR(3)) + ' PERCENT';
    
    -- Print and execute the command
    PRINT '-- Processing statistics for: [' + @SchemaName + '].[' + @TableName + ']';
    PRINT '-- Method: ' + @StatisticsUpdateType + ', Row Count: ' + CAST(@TableRowCount AS VARCHAR(20));
    PRINT @SQL;
    
    BEGIN TRY
        EXEC sp_executesql @SQL;
        PRINT '-- Completed successfully';
    END TRY
    BEGIN CATCH
        PRINT '-- Error: ' + ERROR_MESSAGE();
    END CATCH
    
    PRINT '';
    
    FETCH NEXT FROM TableCursor INTO @SchemaName, @TableName, @TableRowCount, @StatisticsUpdateType;
END

-- Clean up
CLOSE TableCursor;
DEALLOCATE TableCursor;
DROP TABLE #TableStats;
DROP TABLE #StatsInfo;

-- Print script completion info
PRINT '-- Statistics maintenance completed';
PRINT '-- ' + CONVERT(NVARCHAR(30), GETDATE(), 120);
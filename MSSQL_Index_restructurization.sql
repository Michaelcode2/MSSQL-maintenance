-- SQL Script to Reindex All Indexed Tables in the Database
-- This script will identify tables with indexes and rebuild/reorganize them based on fragmentation level

-- Set the target database
USE Servio;

-- Set nocount on to reduce output messages
SET NOCOUNT ON;

-- Create a temporary table to store index information
IF OBJECT_ID('tempdb..#IndexStats') IS NOT NULL
    DROP TABLE #IndexStats;

CREATE TABLE #IndexStats (
    DatabaseName NVARCHAR(128),
    SchemaName NVARCHAR(128),
    TableName NVARCHAR(128),
    IndexName NVARCHAR(128),
    AvgFragmentation FLOAT,
    PageCount INT
);

-- Declare variables for cursor processing
DECLARE @SchemaName NVARCHAR(128);
DECLARE @TableName NVARCHAR(128);
DECLARE @IndexName NVARCHAR(128);
DECLARE @Fragmentation FLOAT;
DECLARE @PageCount INT;
DECLARE @SQL NVARCHAR(MAX);
DECLARE @DatabaseName NVARCHAR(128) = DB_NAME();
DECLARE @IndexAction NVARCHAR(20);
DECLARE @ReindexOptions NVARCHAR(100);

-- Set online mode option based on SQL Server edition (Enterprise allows ONLINE = ON)
IF SERVERPROPERTY('EngineEdition') = 3 -- Enterprise
    SET @ReindexOptions = 'WITH (ONLINE = ON)';
ELSE
    SET @ReindexOptions = 'WITH (ONLINE = OFF)';

-- Print script start info
PRINT '-- Starting index maintenance on database: ' + @DatabaseName;
PRINT '-- ' + CONVERT(NVARCHAR(30), GETDATE(), 120);
PRINT '';

-- Populate the temp table with index fragmentation information
INSERT INTO #IndexStats (DatabaseName, SchemaName, TableName, IndexName, AvgFragmentation, PageCount)
SELECT 
    DB_NAME() AS DatabaseName,
    SCHEMA_NAME(o.schema_id) AS SchemaName,
    OBJECT_NAME(i.object_id) AS TableName,
    i.name AS IndexName,
    ROUND(ips.avg_fragmentation_in_percent, 2) AS AvgFragmentation,
    ips.page_count AS PageCount
FROM 
    sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
    INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    INNER JOIN sys.objects o ON i.object_id = o.object_id
WHERE 
    ips.index_id > 0 -- Skip heaps
    AND ips.page_count > 1000 -- Only consider indexes with at least 1000 pages
    AND i.name IS NOT NULL -- Skip unnamed indexes
    AND o.type = 'U' -- User tables only
ORDER BY 
    AvgFragmentation DESC;

-- Show index fragmentation summary
PRINT '-- Index Fragmentation Summary:';
PRINT '';
SELECT 
    SchemaName,
    TableName,
    IndexName,
    AvgFragmentation,
    PageCount
FROM 
    #IndexStats
ORDER BY 
    AvgFragmentation DESC;
PRINT '';

-- Create cursor to process each index
DECLARE IndexCursor CURSOR FOR
SELECT 
    SchemaName,
    TableName,
    IndexName,
    AvgFragmentation,
    PageCount
FROM 
    #IndexStats
ORDER BY 
    SchemaName, TableName, IndexName;

-- Open cursor and start processing indexes
OPEN IndexCursor;
FETCH NEXT FROM IndexCursor INTO @SchemaName, @TableName, @IndexName, @Fragmentation, @PageCount;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Determine if REBUILD or REORGANIZE based on fragmentation level
    IF @Fragmentation > 30.0
        SET @IndexAction = 'REBUILD';
    ELSE IF @Fragmentation >= 5.0
        SET @IndexAction = 'REORGANIZE';
    ELSE
        SET @IndexAction = 'SKIP'; -- Skip if fragmentation is less than 5%

    -- Only process indexes that need work
    IF @IndexAction <> 'SKIP'
    BEGIN
        SET @SQL = 'ALTER INDEX [' + @IndexName + '] ON [' + @SchemaName + '].[' + @TableName + '] ';
        
        -- Add appropriate action and options
        IF @IndexAction = 'REBUILD'
            SET @SQL = @SQL + 'REBUILD ' + @ReindexOptions;
        ELSE
            SET @SQL = @SQL + 'REORGANIZE';
        
        -- Print and execute the command
        PRINT '-- Processing index: [' + @IndexName + '] on [' + @SchemaName + '].[' + @TableName + ']';
        PRINT '-- Action: ' + @IndexAction + ', Fragmentation: ' + CAST(@Fragmentation AS VARCHAR(10)) + '%, Pages: ' + CAST(@PageCount AS VARCHAR(10));
        PRINT @SQL;
        
        BEGIN TRY
            EXEC sp_executesql @SQL;
            PRINT '-- Completed successfully';
        END TRY
        BEGIN CATCH
            PRINT '-- Error: ' + ERROR_MESSAGE();
        END CATCH
        
        PRINT '';
    END
    
    FETCH NEXT FROM IndexCursor INTO @SchemaName, @TableName, @IndexName, @Fragmentation, @PageCount;
END

-- Clean up
CLOSE IndexCursor;
DEALLOCATE IndexCursor;
DROP TABLE #IndexStats;

-- Print script completion info
PRINT '-- Index maintenance completed';
PRINT '-- ' + CONVERT(NVARCHAR(30), GETDATE(), 120);
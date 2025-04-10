## MSSQL_Index_restructurization

This script performs comprehensive index maintenance on your Microsoft SQL Server database. Here's what it does:

-   Creates a temporary table to store index statistics
-   Collects fragmentation data for all indexes on user tables
-   Displays a summary of fragmentation levels
-   Processes each index using a cursor with these rules:
    -   If fragmentation > 30%: REBUILD the index
    -   If fragmentation between 5-30%: REORGANIZE the index
    -   If fragmentation < 5%: Skip the index (no action needed)
-   Automatically detects if you're using Enterprise edition to enable ONLINE index operations
-   Includes error handling for each index operation
-   Provides detailed logging throughout the process

To use this script:

-   Set Your database name un the top of the script
-   Run it in SQL Server Management Studio against the target database
-   Review the fragmentation summary before actual changes are made
-   Monitor the output for any errors during processing

## MSSQL_Statistics_update

This statistics maintenance script is designed to complement index maintenance script. It intelligently handles statistics updates based on table size and provides detailed logging. Here's what it does:

- Identifies all user tables in the database
- Determines the most efficient update method for each table:
  - FULLSCAN for tables with fewer than 1 million rows
  - SAMPLE (30%) for larger tables to reduce resource consumption
- Processes tables in order of size (largest first)
- Provides detailed logging of operations
- Includes error handling for each table

#### Key features:

- Shows when statistics were last updated for each table
- Detects tables with LOB columns (which can affect statistics updates)
- Intelligently balances between accuracy (FULLSCAN) and performance (SAMPLE)
- Can be run independently or after the index maintenance script

We can adjust these parameters in the script to match your needs:

- @RowCountThreshold: Tables larger than this use sampling (default: 1 million rows)
- @SamplePercent: Percentage to sample for large tables (default: 30%)

For most databases, running both scripts weekly provides an excellent maintenance strategy. 

## Summary:

Index Maintenance Script: Identifies and rebuilds/reorganizes fragmented indexes based on their fragmentation level (rebuild if >30%, reorganize if 5-30%).
Statistics Maintenance Script: Updates statistics for all user tables, intelligently choosing between FULLSCAN for smaller tables and SAMPLE for larger ones.

Both scripts include detailed logging, error handling, and are designed to work efficiently even on large databases.
For best results, recommended:

- Running these scripts weekly during a maintenance window
- Scheduling them through SQL Server Agent jobs
- Consider running the index maintenance first, followed by the statistics maintenance
- Monitor their execution time to ensure they fit within your maintenance window

These maintenance routines will help improve query performance, optimize execution plans, and maintain consistent database performance over time.

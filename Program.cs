using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

class Program
{
    static async Task Main(string[] args)
    {
        // ==========================================
        // 1. LOAD CONFIGURATION
        // ==========================================
        var config = new ConfigurationBuilder()
            .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        string sourceConnStr = config.GetConnectionString("SourceDatabase");
        string destConnStr = config.GetConnectionString("DestinationDatabase");

        var settings = config.GetSection("DataTransferSettings");
        string sourceTableName = settings["SourceTableName"];
        string destTableName = settings["DestinationTableName"];
        string stagingTableName = settings["StagingTableName"];

        var lookupMapping = config.GetSection("DataTransferSettings:LookupColumns").GetChildren().ToDictionary(x => x.Key, x => x.Value);
        var updateMapping = config.GetSection("DataTransferSettings:UpdateColumns").GetChildren().ToDictionary(x => x.Key, x => x.Value);

        // ==========================================
        // 2. DYNAMIC SQL GENERATION
        // ==========================================
        var allMappings = lookupMapping.Concat(updateMapping).ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        var selectClauses = allMappings.Select(m => $"[{m.Key}] AS [{m.Value}]");
        string sourceQuery = $"SELECT {string.Join(", ", selectClauses)} FROM {sourceTableName}";

        List<string> destLookupColumns = lookupMapping.Values.ToList();
        List<string> destUpdateColumns = updateMapping.Values.ToList();
        List<string> allDestColumns = allMappings.Values.ToList();

        // ==========================================
        // 3. EXECUTE PIPELINE
        // ==========================================
        bool triggersDisabled = false;

        try
        {
            Console.Clear();
            Console.WriteLine("=== Starting High-Volume Data Update Pipeline ===\n");

            long totalRowsToProcess = 0;
            using (var spinner = new ConsoleSpinner("Calculating total rows to process... "))
            {
                using (SqlConnection sourceConn = new SqlConnection(sourceConnStr))
                using (SqlCommand countCmd = new SqlCommand($"SELECT COUNT(*) FROM {sourceTableName}", sourceConn))
                {
                    await sourceConn.OpenAsync();
                    totalRowsToProcess = Convert.ToInt64(await countCmd.ExecuteScalarAsync());
                }
            }
            Console.WriteLine($"Total rows to process: {totalRowsToProcess:N0}\n");

            if (totalRowsToProcess == 0) return;

            using (SqlConnection destConn = new SqlConnection(destConnStr))
            {
                await destConn.OpenAsync();

                // Phase 0: Disable Triggers
                using (var spinner = new ConsoleSpinner($"[Phase 0] Disabling triggers on {destTableName}... "))
                {
                    await DisableTriggersAsync(destConn, destTableName);
                    triggersDisabled = true; 
                }
                Console.WriteLine("Done.");

                // Phase 1: Create Staging
                using (var spinner = new ConsoleSpinner("[Phase 1] Creating dynamic Staging Table... "))
                {
                    await CreateDynamicStagingTableAsync(destConn, destTableName, stagingTableName, allDestColumns);
                }
                Console.WriteLine("Done.");

                // Phase 2: Transfer Data
                Console.WriteLine("\n[Phase 2] Starting Resilient Data Transfer (SqlBulkCopy)...");
                await TransferDataWithRetryAsync(sourceConnStr, destConnStr, sourceQuery, stagingTableName, allDestColumns, totalRowsToProcess);

                // Phase 2.5: Index Staging
                using (var spinner = new ConsoleSpinner("\n\n[Phase 2.5] Building Clustered Index on Staging Table... "))
                {
                    await CreateIndexOnStagingAsync(destConn, stagingTableName, destLookupColumns);
                }
                Console.WriteLine("Done.");

                // Phase 3: Batched Update Loop
                Console.WriteLine("\n\n[Phase 3] Running Safe Batched Updates...");
                await PerformBatchedUpdateAsync(destConn, destTableName, stagingTableName, destLookupColumns, destUpdateColumns, totalRowsToProcess);

                // Phase 4: Cleanup
                using (var spinner = new ConsoleSpinner("\n\n[Phase 4] Cleaning up Staging Table... "))
                {
                    await DropStagingTableAsync(destConn, stagingTableName);
                }
                Console.WriteLine("Done.");

                Console.WriteLine("\n\n=== Pipeline Completed Successfully! ===");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n\n[FATAL ERROR] {ex.Message}");
            Console.WriteLine("Note: Completed batches are saved. Resolve the error and run again.");
        }
        finally
        {
            // SAFETY GUARANTEE: Always re-enable triggers
            if (triggersDisabled)
            {
                Console.WriteLine($"\n[Safety Check] Re-enabling triggers on {destTableName}...");
                try
                {
                    await EnableTriggersAsync(destConnStr, destTableName);
                    Console.WriteLine("Triggers successfully re-enabled.");
                }
                catch (Exception finalEx)
                {
                    Console.WriteLine($"\n[CRITICAL ALARM] Failed to re-enable triggers! Error: {finalEx.Message}");
                }
            }
        }
    }

    // ==========================================
    // PIPELINE LOGIC
    // ==========================================
    
    static async Task DisableTriggersAsync(SqlConnection destConn, string tableName)
    {
        using (SqlCommand cmd = new SqlCommand($"ALTER TABLE {tableName} DISABLE TRIGGER ALL;", destConn))
        {
            await cmd.ExecuteNonQueryAsync();
        }
    }

    static async Task EnableTriggersAsync(string destConnStr, string tableName)
    {
        using (SqlConnection safeConn = new SqlConnection(destConnStr))
        {
            await safeConn.OpenAsync();
            using (SqlCommand cmd = new SqlCommand($"ALTER TABLE {tableName} ENABLE TRIGGER ALL;", safeConn))
            {
                await cmd.ExecuteNonQueryAsync();
            }
        }
    }

    static async Task CreateDynamicStagingTableAsync(SqlConnection destConn, string destTable, string stagingTable, List<string> columns)
    {
        string cols = string.Join(", ", columns.Select(c => $"[{c}]"));
        string sql = $"IF OBJECT_ID('{stagingTable}', 'U') IS NOT NULL DROP TABLE {stagingTable}; SELECT TOP 0 {cols} INTO {stagingTable} FROM {destTable};";
        using (SqlCommand cmd = new SqlCommand(sql, destConn)) { await cmd.ExecuteNonQueryAsync(); }
    }

    static async Task TransferDataWithRetryAsync(string sourceConnStr, string destConnStr, string sourceQuery, string stagingTable, List<string> columns, long totalRows)
    {
        int maxRetries = 4;
        int delayMs = 2000;
        var stopwatch = Stopwatch.StartNew();

        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                using (SqlConnection sourceConn = new SqlConnection(sourceConnStr))
                using (SqlCommand sourceCmd = new SqlCommand(sourceQuery, sourceConn))
                using (SqlConnection destConn = new SqlConnection(destConnStr)) 
                {
                    sourceCmd.CommandTimeout = 0;
                    await sourceConn.OpenAsync();
                    await destConn.OpenAsync(); 

                    using (SqlDataReader reader = await sourceCmd.ExecuteReaderAsync())
                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(destConn))
                    {
                        bulkCopy.DestinationTableName = stagingTable;
                        bulkCopy.BatchSize = 100000;
                        bulkCopy.BulkCopyTimeout = 0; 
                        bulkCopy.NotifyAfter = 100000; 

                        foreach (var col in columns) bulkCopy.ColumnMappings.Add(col, col);

                        bulkCopy.SqlRowsCopied += (sender, e) => LogProgress("Transfer", e.RowsCopied, totalRows, stopwatch.Elapsed);
                        await bulkCopy.WriteToServerAsync(reader);
                    }
                }
                
                // Force UI to 100% and break to next line cleanly
                LogProgress("Transfer", totalRows, totalRows, stopwatch.Elapsed);
                Console.WriteLine();
                
                stopwatch.Stop();
                break; 
            }
            catch (SqlException ex)
            {
                if (attempt == maxRetries || !IsTransientError(ex)) throw; 
                Console.WriteLine($"\n[Warning] Network blip... Retrying in {delayMs / 1000}s...");
                await Task.Delay(delayMs);
                delayMs *= 2; 
            }
        }
    }

    static async Task CreateIndexOnStagingAsync(SqlConnection destConn, string stagingTable, List<string> lookupCols)
    {
        string indexCols = string.Join(", ", lookupCols.Select(c => $"[{c}]"));
        string sql = $"CREATE CLUSTERED INDEX IX_Staging_Lookup ON {stagingTable} ({indexCols});";
        using (SqlCommand cmd = new SqlCommand(sql, destConn)) 
        { 
            cmd.CommandTimeout = 0; 
            await cmd.ExecuteNonQueryAsync(); 
        }
    }

    static async Task PerformBatchedUpdateAsync(SqlConnection destConn, string destTable, string stagingTable, List<string> lookupCols, List<string> updateCols, long totalRows)
    {
        string joinConditions = string.Join(" AND ", lookupCols.Select(c => $"d.[{c}] = s.[{c}]"));
        string batchJoinConditions = string.Join(" AND ", lookupCols.Select(c => $"s.[{c}] = b.[{c}]"));
        string setClauses = string.Join(", ", updateCols.Select(c => $"d.[{c}] = s.[{c}]"));
        string lookupSelectCols = string.Join(", ", lookupCols.Select(c => $"[{c}]"));

        string singleBatchSql = $@"
            DECLARE @BatchSize INT = 50000; 
            DECLARE @RowsAffected INT = 0;
            
            SELECT TOP (@BatchSize) {lookupSelectCols} INTO #CurrentBatch FROM {stagingTable};
            SET @RowsAffected = @@ROWCOUNT;
            
            IF @RowsAffected > 0 
            BEGIN
                BEGIN TRY 
                    BEGIN TRAN;
                        UPDATE d SET {setClauses} FROM {destTable} d INNER JOIN {stagingTable} s ON {joinConditions} INNER JOIN #CurrentBatch b ON {batchJoinConditions};
                        DELETE s FROM {stagingTable} s INNER JOIN #CurrentBatch b ON {batchJoinConditions};
                    COMMIT TRAN; 
                END TRY
                BEGIN CATCH 
                    IF @@TRANCOUNT > 0 ROLLBACK TRAN; 
                    THROW; 
                END CATCH
            END
            
            DROP TABLE IF EXISTS #CurrentBatch; 
            SELECT @RowsAffected;";

        long totalUpdated = 0;
        var stopwatch = Stopwatch.StartNew();

        using (SqlCommand cmd = new SqlCommand(singleBatchSql, destConn))
        {
            cmd.CommandTimeout = 0; 
            while (true)
            {
                int rowsProcessed = (int)await cmd.ExecuteScalarAsync();
                if (rowsProcessed == 0) break;
                totalUpdated += rowsProcessed;
                LogProgress("Updating", totalUpdated, totalRows, stopwatch.Elapsed);
            }
        }
        
        // Force UI to 100% and break to next line cleanly
        LogProgress("Updating", totalRows, totalRows, stopwatch.Elapsed);
        Console.WriteLine();
        stopwatch.Stop();
    }

    static async Task DropStagingTableAsync(SqlConnection destConn, string stagingTable)
    {
        using (SqlCommand cmd = new SqlCommand($"DROP TABLE IF EXISTS {stagingTable};", destConn)) { await cmd.ExecuteNonQueryAsync(); }
    }

    // ==========================================
    // UI & ERROR LOGGING HELPER METHODS
    // ==========================================
    static void LogProgress(string phase, long currentRows, long totalRows, TimeSpan elapsed)
    {
        if (currentRows > totalRows) currentRows = totalRows; // Clamp to 100% if source grew during copy

        double elapsedSeconds = elapsed.TotalSeconds == 0 ? 0.001 : elapsed.TotalSeconds; 
        double speed = currentRows / elapsedSeconds;
        long remainingRows = totalRows - currentRows;
        TimeSpan eta = TimeSpan.FromSeconds(speed > 0 ? remainingRows / speed : 0);
        
        double percentComplete = (double)currentRows / totalRows * 100;
        int barSize = 15; // Shorter bar to prevent console line-wrapping
        int filled = (int)(percentComplete / 100 * barSize);
        string bar = new string('█', filled) + new string('░', barSize - filled);

        Console.Write($"\r{phase,-8} [{bar}] {percentComplete,5:F1}% | {currentRows,9:N0}/{totalRows,9:N0} | ETA: {eta:hh\\:mm\\:ss}   ");
    }

    static bool IsTransientError(SqlException ex)
    {
        int[] transientErrors = { -2, 20, 64, 233, 10053, 10054, 10060, 40197, 40501, 40613, 49918, 49919, 49920 };
        foreach (SqlError err in ex.Errors)
        {
            if (transientErrors.Contains(err.Number)) return true;
        }
        return false; 
    }
}

// ==========================================
// BACKGROUND SPINNER CLASS
// ==========================================
public class ConsoleSpinner : IDisposable
{
    private readonly int _delay = 100;
    private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();
    private readonly Task _task;
    private readonly string _text;

    public ConsoleSpinner(string text)
    {
        _text = text;
        Console.Write(_text);
        _task = Task.Run(Spin);
    }

    private async Task Spin()
    {
        char[] sequence = new[] { '/', '-', '\\', '|' };
        int counter = 0;
        while (!_cancellation.IsCancellationRequested)
        {
            Console.Write(sequence[counter++ % sequence.Length]);
            Console.SetCursorPosition(Console.CursorLeft - 1, Console.CursorTop);
            await Task.Delay(_delay);
        }
    }

    public void Dispose()
    {
        _cancellation.Cancel();
        _task.Wait();
        Console.Write(" \b"); 
    }
}
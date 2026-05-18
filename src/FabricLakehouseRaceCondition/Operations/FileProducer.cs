using Azure;
using Azure.Storage.Files.DataLake;
using FabricLakehouseRaceCondition.Config;
using FabricLakehouseRaceCondition.Logging;

namespace FabricLakehouseRaceCondition.Operations;

/// <summary>
/// Record tracking a single file upload attempt.
/// </summary>
public sealed record UploadRecord
{
    public required string FileId { get; init; }
    public required string TargetPath { get; init; }
    public required DateTimeOffset UploadStart { get; init; }
    public DateTimeOffset UploadEnd { get; set; }
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    /// <summary>Number of attempts taken (1 = succeeded first try, >1 = retried after 404).</summary>
    public int Attempts { get; set; }
}

/// <summary>
/// Produces random-data CSV files and uploads them to ADLS Gen2 (OneLake).
/// Tracks every upload attempt for later verification.
/// </summary>
public sealed class FileProducer
{
    private readonly DataLakeFileSystemClient _fsClient;
    private readonly HarnessConfig _config;
    private readonly List<UploadRecord> _records = [];
    private readonly object _recordsLock = new();

    public FileProducer(DataLakeFileSystemClient fsClient, HarnessConfig config)
    {
        _fsClient = fsClient;
        _config = config;
    }

    /// <summary>All upload records for the current iteration (thread-safe snapshot).</summary>
    public IReadOnlyList<UploadRecord> Records
    {
        get { lock (_recordsLock) { return _records.ToList(); } }
    }

    /// <summary>Clear records between iterations.</summary>
    public void Reset() { lock (_recordsLock) { _records.Clear(); } }

    /// <summary>
    /// Upload a batch of files with multiple uploads in flight simultaneously.
    /// Uses a semaphore to cap concurrency at <see cref="HarnessConfig.MaxConcurrentUploads"/>.
    /// Invokes <paramref name="onFileUploaded"/> after each completed upload so the caller
    /// can count progress and trigger renames at the right moment.
    /// </summary>
    public async Task UploadBatchAsync(
        int count,
        Action<int>? onFileUploaded = null,
        CancellationToken ct = default)
    {
        var semaphore = new SemaphoreSlim(_config.MaxConcurrentUploads);
        var completedCount = 0;
        var tasks = new List<Task>(count);

        for (var i = 0; i < count; i++)
        {
            if (ct.IsCancellationRequested) break;

            await semaphore.WaitAsync(ct);

            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    await UploadSingleFileAsync(ct);
                    var completed = Interlocked.Increment(ref completedCount);
                    onFileUploaded?.Invoke(completed);
                }
                finally
                {
                    semaphore.Release();
                }
            }, ct));

            // Stagger launch slightly to spread upload starts across time.
            if (_config.UploadDelayMs > 0)
            {
                await Task.Delay(_config.UploadDelayMs, ct).ConfigureAwait(false);
            }
        }

        await Task.WhenAll(tasks);
    }

    // ── Internals ────────────────────────────────────────────────────

    private async Task UploadSingleFileAsync(CancellationToken ct)
    {
        var fileId = Guid.NewGuid().ToString("N");
        var relativePath = $"{_config.OutputGreenPath}/{fileId}.csv";
        var record = new UploadRecord
        {
            FileId = fileId,
            TargetPath = relativePath,
            UploadStart = DateTimeOffset.UtcNow
        };

        var data = GenerateCsvData();
        var sizeKb = data.Length / 1024;
        var success = false;
        var attempts = 0;

        try
        {
            // Match production pattern: retry loop with backoff, no directory creation per-upload.
            do
            {
                if (attempts > 5)
                {
                    record.UploadEnd = DateTimeOffset.UtcNow;
                    record.Success = false;
                    record.ErrorMessage = $"Failed after {attempts} attempts";
                    TimestampedLogger.Error($"  ✗ Upload {fileId}.csv failed after {attempts} attempts");
                    break;
                }

                try
                {
                    attempts++;
                    var fileClient = _fsClient.GetFileClient(relativePath);
                    using var stream = new MemoryStream(data);
                    // Production does NOT pass overwrite: true
                    await fileClient.UploadAsync(stream, overwrite: false, cancellationToken: ct);

                    record.UploadEnd = DateTimeOffset.UtcNow;
                    record.Success = true;
                    record.Attempts = attempts;
                    success = true;

                    TimestampedLogger.Info($"  ↑ Uploaded {fileId}.csv ({sizeKb} KB) attempt {attempts}");
                }
                catch (RequestFailedException rfe) when (rfe.Status == 409)
                {
                    // File already exists — treat as success (matches production)
                    record.UploadEnd = DateTimeOffset.UtcNow;
                    record.Success = true;
                    record.Attempts = attempts;
                    success = true;
                    TimestampedLogger.Info($"  ↑ Upload {fileId}.csv — already exists (409), treating as success");
                }
                catch (RequestFailedException rfe) when (rfe.Status == 404)
                {
                    // Not found — folder doesn't exist (may have been renamed away)
                    TimestampedLogger.Warn(
                        $"  ⚠ Upload {fileId}.csv attempt {attempts}: folder not found ({rfe.ErrorCode}), retrying...");
                }

                if (!success)
                {
                    // Random backoff 1-5s (matches production)
                    var backoff = Random.Shared.Next(1000, 5000);
                    await Task.Delay(backoff, ct);
                }

            } while (!success);
        }
        catch (OperationCanceledException)
        {
            record.UploadEnd = DateTimeOffset.UtcNow;
            record.Success = false;
            record.ErrorMessage = "Cancelled";
        }
        catch (Exception ex)
        {
            record.UploadEnd = DateTimeOffset.UtcNow;
            record.Success = false;
            record.ErrorMessage = ex.Message;
            TimestampedLogger.Error($"  ✗ Upload {fileId}.csv failed: {ex.Message}");
        }
        finally
        {
            lock (_recordsLock) { _records.Add(record); }
        }
    }

    private byte[] GenerateCsvData()
    {
        var sizeKb = Random.Shared.Next(_config.MinFileSizeKb, _config.MaxFileSizeKb + 1);
        var buffer = new byte[sizeKb * 1024];
        Random.Shared.NextBytes(buffer);
        return buffer;
    }

    /// <summary>
    /// Ensure the output/ directory exists. Called once at the start of each iteration
    /// (matches production pattern). The "green" subdirectory is created implicitly by ADLS
    /// when files are uploaded to output/green/xxx.csv.
    /// </summary>
    public async Task EnsureOutputDirectoryExistsAsync(CancellationToken ct)
    {
        try
        {
            var dirClient = _fsClient.GetDirectoryClient(_config.OutputPath);
            await dirClient.CreateIfNotExistsAsync(cancellationToken: ct);
        }
        catch (Exception ex)
        {
            TimestampedLogger.Warn($"  ⚠ EnsureDirectory failed: {ex.Message}");
        }
    }
}

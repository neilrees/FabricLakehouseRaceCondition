using Azure;
using Azure.Storage.Files.DataLake;
using FabricLakehouseRaceCondition.Config;
using FabricLakehouseRaceCondition.Logging;

namespace FabricLakehouseRaceCondition.Operations;

/// <summary>
/// Record tracking a single rename attempt.
/// </summary>
public sealed record RenameRecord
{
    public required string RenameId { get; init; }
    public required string SourcePath { get; init; }
    public required string DestinationPath { get; init; }
    public required DateTimeOffset RenameStart { get; init; }
    public DateTimeOffset RenameEnd { get; set; }
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
}

/// <summary>
/// Fires two near-simultaneous renames of output/green → loading/{runId}/green
/// to reproduce the Fabric Lakehouse race condition.
/// </summary>
public sealed class FolderRenamer
{
    private readonly DataLakeFileSystemClient _fsClient;
    private readonly HarnessConfig _config;
    private readonly List<RenameRecord> _records = [];

    public FolderRenamer(DataLakeFileSystemClient fsClient, HarnessConfig config)
    {
        _fsClient = fsClient;
        _config = config;
    }

    /// <summary>All rename records for the current iteration.</summary>
    public IReadOnlyList<RenameRecord> Records => _records.ToList();

    /// <summary>Clear records between iterations.</summary>
    public void Reset() => _records.Clear();

    /// <summary>
    /// Fire two concurrent renames of output/green → loading/{runId}/green.
    /// Returns the two run IDs used for the rename targets.
    /// </summary>
    public async Task<(string RunIdA, string RunIdB)> RenameWithRaceAsync(CancellationToken ct = default)
    {
        var runIdA = Guid.NewGuid().ToString("N")[..12];
        var runIdB = Guid.NewGuid().ToString("N")[..12];

        var sourcePath = _config.OutputGreenPath;
        var destPathA = _config.LoadingRunPath(runIdA);
        var destPathB = _config.LoadingRunPath(runIdB);

        TimestampedLogger.Phase($"  🏁 Firing 2 concurrent renames:");
        TimestampedLogger.Phase($"     A: {sourcePath} → {destPathA}");
        TimestampedLogger.Phase($"     B: {sourcePath} → {destPathB}");

        // Ensure the loading parent directories exist.
        await EnsureParentDirectoryAsync($"{_config.BasePath}/loading/{runIdA}", ct);
        await EnsureParentDirectoryAsync($"{_config.BasePath}/loading/{runIdB}", ct);

        // Fire both renames at the same instant.
        var taskA = ExecuteRenameAsync("A", runIdA, sourcePath, destPathA, ct);
        var taskB = ExecuteRenameAsync("B", runIdB, sourcePath, destPathB, ct);

        await Task.WhenAll(taskA, taskB);

        return (runIdA, runIdB);
    }

    // ── Internals ────────────────────────────────────────────────────

    private async Task ExecuteRenameAsync(
        string label,
        string runId,
        string sourcePath,
        string destPath,
        CancellationToken ct)
    {
        var record = new RenameRecord
        {
            RenameId = runId,
            SourcePath = sourcePath,
            DestinationPath = destPath,
            RenameStart = DateTimeOffset.UtcNow
        };

        try
        {
            var sourceClient = _fsClient.GetDirectoryClient(sourcePath);
            await sourceClient.RenameAsync(destPath, cancellationToken: ct);

            record.RenameEnd = DateTimeOffset.UtcNow;
            record.Success = true;

            TimestampedLogger.Success(
                $"  ✓ Rename {label} succeeded in {(record.RenameEnd - record.RenameStart).TotalMilliseconds:F0}ms " +
                $"(run {runId})");
        }
        catch (RequestFailedException ex)
        {
            record.RenameEnd = DateTimeOffset.UtcNow;
            record.Success = false;
            record.ErrorMessage = $"{ex.ErrorCode}: {ex.Message}";

            TimestampedLogger.Warn(
                $"  ⚠ Rename {label} failed ({ex.ErrorCode}) in " +
                $"{(record.RenameEnd - record.RenameStart).TotalMilliseconds:F0}ms — " +
                $"this is expected if the other rename won the race.");
        }
        catch (Exception ex)
        {
            record.RenameEnd = DateTimeOffset.UtcNow;
            record.Success = false;
            record.ErrorMessage = ex.Message;

            TimestampedLogger.Error($"  ✗ Rename {label} unexpected error: {ex.Message}");
        }
        finally
        {
            _records.Add(record);
        }
    }

    private async Task EnsureParentDirectoryAsync(string path, CancellationToken ct)
    {
        try
        {
            var dirClient = _fsClient.GetDirectoryClient(path);
            await dirClient.CreateIfNotExistsAsync(cancellationToken: ct);
        }
        catch (Exception ex)
        {
            TimestampedLogger.Warn($"  ⚠ EnsureParentDirectory({path}) failed: {ex.Message}");
        }
    }
}

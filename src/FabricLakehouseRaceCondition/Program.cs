using Azure.Identity;
using Azure.Storage.Files.DataLake;
using FabricLakehouseRaceCondition.Config;
using FabricLakehouseRaceCondition.Logging;
using FabricLakehouseRaceCondition.Operations;
using Microsoft.Extensions.Configuration;

// ══════════════════════════════════════════════════════════════════════
//  Fabric Lakehouse Race Condition Test Harness
//  Reproduces concurrent rename anomalies on OneLake via ADLS Gen2.
// ══════════════════════════════════════════════════════════════════════

TimestampedLogger.Banner("Fabric Lakehouse Race Condition Test Harness");

// ── Configuration ────────────────────────────────────────────────────

var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
    .AddCommandLine(args)
    .Build();

var config = new HarnessConfig();
configuration.Bind(config);

try
{
    config.Validate();
}
catch (InvalidOperationException ex)
{
    TimestampedLogger.Error(ex.Message);
    return 1;
}

TimestampedLogger.Info($"Workspace : {config.WorkspaceId}");
TimestampedLogger.Info($"Lakehouse : {config.LakehouseId}");
TimestampedLogger.Info($"Seed files: {config.SeedFileCount} | Race files: {config.ConcurrentUploadCount}");
TimestampedLogger.Info($"File size : {config.MinFileSizeKb}-{config.MaxFileSizeKb} KB | Upload delay: {config.UploadDelayMs}ms | Concurrent: {config.MaxConcurrentUploads}");
TimestampedLogger.Info($"Rename trigger after {config.RenameDelayAfterSeedFiles} background uploads");
TimestampedLogger.Info($"Max iterations: {(config.MaxIterations == 0 ? "∞ (until anomaly or Ctrl+C)" : config.MaxIterations)}");
TimestampedLogger.Separator();

// ── Azure clients ────────────────────────────────────────────────────

TimestampedLogger.Info("Authenticating with DefaultAzureCredential...");
var credential = new DefaultAzureCredential();
var dlsClient = new DataLakeServiceClient(HarnessConfig.OneLakeEndpoint, credential);
var fsClient = dlsClient.GetFileSystemClient(config.FileSystemName);
TimestampedLogger.Success("ADLS client initialised.");

// ── Services ─────────────────────────────────────────────────────────

var producer = new FileProducer(fsClient, config);
var renamer = new FolderRenamer(fsClient, config);
var verifier = new FileVerifier(fsClient, config);

// ── Graceful shutdown ────────────────────────────────────────────────

using var cts = new CancellationTokenSource();

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true; // Prevent immediate kill.
    TimestampedLogger.Warn("Ctrl+C received — finishing current iteration then stopping...");
    cts.Cancel();
};

// ══════════════════════════════════════════════════════════════════════
//  Main loop — 6 phases per iteration
// ══════════════════════════════════════════════════════════════════════

var iteration = 0;

while (!cts.Token.IsCancellationRequested)
{
    iteration++;

    if (config.MaxIterations > 0 && iteration > config.MaxIterations)
    {
        TimestampedLogger.Info($"Reached max iterations ({config.MaxIterations}). Exiting.");
        break;
    }

    TimestampedLogger.Separator();
    TimestampedLogger.Banner($"Iteration {iteration}");

    // Reset trackers.
    producer.Reset();
    renamer.Reset();

    string runIdA = "", runIdB = "";

    try
    {
        // ── Phase 1 — SEED ───────────────────────────────────────────

        TimestampedLogger.Phase($"▶ Phase 1 — SEED: Uploading {config.SeedFileCount} files to output/green/");

        // Ensure output/ directory exists once at start (matches production startup pattern).
        // The "green" subdirectory is created implicitly by ADLS when files are uploaded.
        await producer.EnsureOutputDirectoryExistsAsync(cts.Token);

        await producer.UploadBatchAsync(config.SeedFileCount, ct: cts.Token);

        var seedSuccess = producer.Records.Count(r => r.Success);
        TimestampedLogger.Info($"  Seed complete: {seedSuccess}/{config.SeedFileCount} files uploaded.");

        if (cts.Token.IsCancellationRequested) break;

        // ── Phase 2 — RACE ───────────────────────────────────────────

        TimestampedLogger.Phase(
            $"▶ Phase 2 — RACE: Starting background upload of {config.ConcurrentUploadCount} files; " +
            $"renames fire after {config.RenameDelayAfterSeedFiles} uploads");

        var renameTriggered = false;
        var renameTcs = new TaskCompletionSource<(string, string)>();

        // Background upload stream with rename trigger callback.
        var uploadTask = Task.Run(async () =>
        {
            await producer.UploadBatchAsync(
                config.ConcurrentUploadCount,
                onFileUploaded: uploadNumber =>
                {
                    if (uploadNumber == config.RenameDelayAfterSeedFiles && !renameTriggered)
                    {
                        renameTriggered = true;
                        TimestampedLogger.Phase(
                            $"  🔥 Background upload #{uploadNumber} done — triggering concurrent renames NOW");

                        // Fire renames on a separate thread so uploads continue.
                        _ = Task.Run(async () =>
                        {
                            try
                            {
                                var result = await renamer.RenameWithRaceAsync(cts.Token);
                                renameTcs.TrySetResult(result);
                            }
                            catch (Exception ex)
                            {
                                renameTcs.TrySetException(ex);
                            }
                        });
                    }
                },
                ct: cts.Token);
        }, cts.Token);

        // ── Phase 3 — DRAIN ──────────────────────────────────────────

        TimestampedLogger.Phase("▶ Phase 3 — DRAIN: Waiting for background uploads to complete...");

        await uploadTask;

        // Wait for renames to finish too (they should be done by now).
        if (renameTriggered)
        {
            try
            {
                var (rA, rB) = await renameTcs.Task.WaitAsync(TimeSpan.FromSeconds(60), cts.Token);
                runIdA = rA;
                runIdB = rB;
            }
            catch (TimeoutException)
            {
                TimestampedLogger.Error("  ✗ Renames did not complete within 60 seconds.");
            }
        }
        else
        {
            TimestampedLogger.Warn(
                "  ⚠ Renames were never triggered (not enough background uploads completed).");
        }

        var totalSuccess = producer.Records.Count(r => r.Success);
        var totalFailed = producer.Records.Count(r => !r.Success);
        TimestampedLogger.Info($"  Drain complete: {totalSuccess} succeeded, {totalFailed} failed.");

        if (cts.Token.IsCancellationRequested) break;

        // ── Phase 3.5 — POST-RENAME UPLOADS ──────────────────────────
        // These uploads deliberately target output/green/ AFTER renames have
        // completed. In production, producers keep uploading to the same path
        // for minutes/hours after a rename. If files land in loading/ this
        // proves the stale-path write-through bug.

        if (config.PostRenameUploadCount > 0 && renameTriggered)
        {
            // Wait a beat to ensure rename is fully settled
            await Task.Delay(500, cts.Token);

            TimestampedLogger.Phase(
                $"▶ Phase 3.5 — POST-RENAME: Uploading {config.PostRenameUploadCount} files to output/green/ " +
                "(path should have been renamed away)");

            await producer.UploadBatchAsync(config.PostRenameUploadCount, ct: cts.Token);

            var postSuccess = producer.Records.Count(r => r.Success) - totalSuccess;
            var postFailed = producer.Records.Count(r => !r.Success) - totalFailed;
            TimestampedLogger.Info(
                $"  Post-rename: {postSuccess} succeeded, {postFailed} failed. " +
                "If succeeded → check whether they landed in output/ or loading/.");
        }

        if (cts.Token.IsCancellationRequested) break;

        // ── Phase 4 — VERIFY ─────────────────────────────────────────

        TimestampedLogger.Phase("▶ Phase 4 — VERIFY: Cross-referencing files...");

        var result = await verifier.VerifyAsync(producer.Records, renamer.Records, cts.Token);

        // ── Phase 5 — REPORT ─────────────────────────────────────────

        TimestampedLogger.Phase("▶ Phase 5 — REPORT");
        TimestampedLogger.Info($"  Total uploaded : {result.TotalUploaded}");
        TimestampedLogger.Info($"  Successful     : {result.SuccessfulUploads}");
        TimestampedLogger.Info($"  In output/     : {result.FilesFoundInOutput}");
        TimestampedLogger.Info($"  In loading/    : {result.FilesFoundInLoading}");

        // Log rename details.
        foreach (var r in renamer.Records)
        {
            var status = r.Success ? "✓ SUCCESS" : "✗ FAILED";
            TimestampedLogger.Info(
                $"  Rename {r.RenameId}: {status} " +
                $"(start={r.RenameStart:HH:mm:ss.fff}, end={r.RenameEnd:HH:mm:ss.fff})");
        }

        if (result.HasAnomalies)
        {
            var errors = result.Anomalies.Where(a => a.Severity == "Error").ToList();
            var warnings = result.Anomalies.Where(a => a.Severity == "Warning").ToList();

            if (warnings.Count > 0)
            {
                TimestampedLogger.Warn($"  ⚠ {warnings.Count} warning(s):");
                foreach (var a in warnings)
                {
                    TimestampedLogger.Warn($"     [{a.Type}] {a.Description}");
                }
            }

            if (errors.Count > 0)
            {
                TimestampedLogger.Error($"  🚨 {errors.Count} ERROR ANOMALY/ANOMALIES DETECTED:");
                foreach (var a in errors)
                {
                    TimestampedLogger.Error($"     [{a.Type}] {a.Description}");
                }

                if (config.PauseOnAnomaly)
                {
                    TimestampedLogger.Warn("  ⏸ Pausing — press any key to continue, or Ctrl+C to exit...");
                    TimestampedLogger.Warn("  ⏸ Files are preserved for inspection.");
                    Console.ReadKey(intercept: true);
                }
            }
            else
            {
                TimestampedLogger.Success("  ✓ No errors this iteration (warnings only).");
            }
        }
        else
        {
            TimestampedLogger.Success("  ✓ No anomalies detected this iteration.");
        }

        if (cts.Token.IsCancellationRequested) break;

        // ── Phase 6 — CLEANUP ────────────────────────────────────────

        TimestampedLogger.Phase("▶ Phase 6 — CLEANUP: Deleting test directories...");

        await CleanupDirectoryAsync(fsClient, config.OutputGreenPath, cts.Token);

        if (!string.IsNullOrEmpty(runIdA))
            await CleanupDirectoryAsync(fsClient, config.LoadingRunPath(runIdA), cts.Token);
        if (!string.IsNullOrEmpty(runIdB))
            await CleanupDirectoryAsync(fsClient, config.LoadingRunPath(runIdB), cts.Token);

        TimestampedLogger.Success("  ✓ Cleanup complete.");
    }
    catch (OperationCanceledException)
    {
        TimestampedLogger.Warn("Iteration cancelled.");
        break;
    }
    catch (Exception ex)
    {
        TimestampedLogger.Error($"Unhandled error in iteration {iteration}: {ex}");
        // Continue to next iteration rather than crashing.
    }
}

TimestampedLogger.Separator();
TimestampedLogger.Banner("Test harness stopped");
return 0;

// ══════════════════════════════════════════════════════════════════════
//  Helpers
// ══════════════════════════════════════════════════════════════════════

static async Task CleanupDirectoryAsync(
    DataLakeFileSystemClient fsClient, string path, CancellationToken ct)
{
    try
    {
        var dirClient = fsClient.GetDirectoryClient(path);
        await dirClient.DeleteIfExistsAsync(conditions: null, cancellationToken: ct);
        TimestampedLogger.Info($"    Deleted {path}");
    }
    catch (Exception ex)
    {
        TimestampedLogger.Warn($"    ⚠ Could not delete {path}: {ex.Message}");
    }
}

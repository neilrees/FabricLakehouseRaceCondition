using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using FabricLakehouseRaceCondition.Config;
using FabricLakehouseRaceCondition.Logging;
using System.Security.Cryptography;

namespace FabricLakehouseRaceCondition.Operations;

/// <summary>
/// Describes a single anomaly found during verification.
/// </summary>
public sealed record Anomaly
{
    public required string Type { get; init; }        // "MissingFile", "WrongLocation", "Duplicate"
    public required string FileId { get; init; }
    public required string Description { get; init; }
    /// <summary>"Error" = stops harness (if PauseOnAnomaly), "Warning" = logged but harness continues.</summary>
    public string Severity { get; init; } = "Error";
}

/// <summary>
/// The result of a verification pass.
/// </summary>
public sealed class VerificationResult
{
    public int TotalUploaded { get; init; }
    public int SuccessfulUploads { get; init; }
    public int FilesFoundInOutput { get; init; }
    public int FilesFoundInLoading { get; init; }
    public IReadOnlyList<Anomaly> Anomalies { get; init; } = [];
    public bool HasAnomalies => Anomalies.Count > 0;
    public bool HasErrors => Anomalies.Any(a => a.Severity == "Error");
    public bool HasWarnings => Anomalies.Any(a => a.Severity == "Warning");
}

/// <summary>
/// Lists files in output/ and loading/ and cross-references them against the
/// FileProducer's upload tracker to detect race-condition anomalies.
/// </summary>
public sealed class FileVerifier
{
    private readonly DataLakeFileSystemClient _fsClient;
    private readonly HarnessConfig _config;

    public FileVerifier(DataLakeFileSystemClient fsClient, HarnessConfig config)
    {
        _fsClient = fsClient;
        _config = config;
    }

    /// <summary>
    /// Verify all uploaded files are in the correct location.
    /// Files uploaded BEFORE the rename should be in loading/{runId}/green/.
    /// Files uploaded AFTER the rename should be in output/green/.
    /// </summary>
    public async Task<VerificationResult> VerifyAsync(
        IReadOnlyList<UploadRecord> uploads,
        IReadOnlyList<RenameRecord> renames,
        CancellationToken ct = default)
    {
        TimestampedLogger.Info("  📂 Listing files in output/ ...");
        var outputFiles = await ListFilesRecursiveAsync(_config.OutputPath, ct);
        TimestampedLogger.Info($"     Found {outputFiles.Count} file(s) in output/");

        TimestampedLogger.Info("  📂 Listing files in loading/ ...");
        var loadingFiles = await ListFilesRecursiveAsync(_config.LoadingPath, ct);
        TimestampedLogger.Info($"     Found {loadingFiles.Count} file(s) in loading/");

        // Build lookup: fileId → list of (path, size) where found.
        var locationMap = new Dictionary<string, List<FileInfo>>();

        foreach (var file in outputFiles)
        {
            var fileId = ExtractFileId(file.Path);
            if (fileId is null) continue;
            if (!locationMap.ContainsKey(fileId)) locationMap[fileId] = [];
            locationMap[fileId].Add(file);
        }

        foreach (var file in loadingFiles)
        {
            var fileId = ExtractFileId(file.Path);
            if (fileId is null) continue;
            if (!locationMap.ContainsKey(fileId)) locationMap[fileId] = [];
            locationMap[fileId].Add(file);
        }

        // Determine the latest rename completion time.
        var successfulRenames = renames.Where(r => r.Success).ToList();
        var renameCompletedAt = successfulRenames.Count > 0
            ? successfulRenames.Max(r => r.RenameEnd)
            : DateTimeOffset.MaxValue;

        var anomalies = new List<Anomaly>();
        var successfulUploads = uploads.Where(u => u.Success).ToList();

        foreach (var upload in successfulUploads)
        {
            if (!locationMap.TryGetValue(upload.FileId, out var locations) || locations.Count == 0)
            {
                // File uploaded successfully but not found anywhere.
                anomalies.Add(new Anomaly
                {
                    Type = "MissingFile",
                    FileId = upload.FileId,
                    Description =
                        $"File {upload.FileId}.csv was uploaded successfully " +
                        $"(start={upload.UploadStart:HH:mm:ss.fff}, end={upload.UploadEnd:HH:mm:ss.fff}) " +
                        "but was NOT found in output/ or loading/."
                });
                continue;
            }

            // Check for duplicates — compare sizes and content hashes.
            if (locations.Count > 1)
            {
                var sizeInfo = string.Join(", ", locations.Select(l =>
                    $"{ShortenPath(l.Path)} ({l.SizeBytes} bytes)"));

                // Download and hash each copy to compare content.
                TimestampedLogger.Info($"     🔍 Hashing {locations.Count} copies of {upload.FileId}.csv ...");
                var hashes = new List<(string Path, string? Hash, long Size)>();
                foreach (var loc in locations)
                {
                    var (hash, size) = await GetFileHashAsync(loc.Path, ct);
                    hashes.Add((loc.Path, hash, size));
                }

                var hashSummary = string.Join(" | ", hashes.Select(h =>
                    $"{ShortenPath(h.Path)}: {h.Size}B, SHA256={h.Hash?[..16] ?? "FAILED"}..."));

                var distinctHashes = hashes.Where(h => h.Hash is not null).Select(h => h.Hash).Distinct().Count();
                var allZeroLength = hashes.All(h => h.Size == 0);
                var hasZeroLength = hashes.Any(h => h.Size == 0);
                var hasData = hashes.Any(h => h.Size > 0);
                var contentVerdict = allZeroLength
                    ? "ALL ZERO-LENGTH"
                    : distinctHashes == 1
                        ? "IDENTICAL CONTENT"
                        : $"DIFFERENT CONTENT ({distinctHashes} distinct hashes)";

                // Downgrade to warning if one copy is empty and the other has data,
                // OR if both copies have identical content (expected retry behavior).
                // The real bug we're hunting is WrongLocation (uploads to stale path).
                var severity = (hasZeroLength && hasData) || distinctHashes == 1
                    ? "Warning"
                    : "Error";

                anomalies.Add(new Anomaly
                {
                    Type = "Duplicate",
                    FileId = upload.FileId,
                    Severity = severity,
                    Description =
                        $"File {upload.FileId}.csv found in {locations.Count} locations. " +
                        $"Verdict: {contentVerdict}. " +
                        $"Details: [{hashSummary}]"
                });
            }

            // Check for wrong location: file uploaded AFTER rename completed
            // but found in loading/ (it should be in output/green/).
            if (upload.UploadStart >= renameCompletedAt)
            {
                var inLoading = locations.Any(l => l.Path.Contains("/loading/"));
                if (inLoading)
                {
                    var firstTry = upload.Attempts == 1 ? "FIRST-TRY (no 404!)" : $"after {upload.Attempts} attempts";
                    anomalies.Add(new Anomaly
                    {
                        Type = "WrongLocation",
                        FileId = upload.FileId,
                        Description =
                            $"🔥 File {upload.FileId}.csv was uploaded AFTER renames completed " +
                            $"(upload={upload.UploadStart:HH:mm:ss.fff}, rename done={renameCompletedAt:HH:mm:ss.fff}) " +
                            $"succeeded {firstTry} " +
                            $"but was found in loading/: {string.Join(", ", locations.Where(l => l.Path.Contains("/loading/")).Select(l => l.Path))}"
                    });
                }
            }
        }

        return new VerificationResult
        {
            TotalUploaded = uploads.Count,
            SuccessfulUploads = successfulUploads.Count,
            FilesFoundInOutput = outputFiles.Count,
            FilesFoundInLoading = loadingFiles.Count,
            Anomalies = anomalies
        };
    }

    // ── Internals ────────────────────────────────────────────────────

    /// <summary>Info about a discovered file.</summary>
    private sealed record FileInfo(string Path, long SizeBytes);

    private async Task<List<FileInfo>> ListFilesRecursiveAsync(string directoryPath, CancellationToken ct)
    {
        var files = new List<FileInfo>();

        try
        {
            await foreach (var item in _fsClient.GetPathsAsync(directoryPath, recursive: true, cancellationToken: ct))
            {
                if (ct.IsCancellationRequested) break;

                if (item.IsDirectory != true)
                {
                    files.Add(new FileInfo(item.Name, item.ContentLength ?? 0));
                }
            }
        }
        catch (RequestFailedException ex) when (ex.Status == 404 || ex.ErrorCode == "PathNotFound")
        {
            TimestampedLogger.Info($"     Directory {directoryPath} does not exist (yet).");
        }
        catch (Exception ex)
        {
            TimestampedLogger.Error($"  ✗ Error listing {directoryPath}: {ex.Message}");
        }

        return files;
    }

    /// <summary>
    /// Download a file and compute its SHA-256 hash.
    /// Returns (hash, sizeBytes) or (null, 0) on failure.
    /// </summary>
    private async Task<(string? Hash, long Size)> GetFileHashAsync(string path, CancellationToken ct)
    {
        try
        {
            var fileClient = _fsClient.GetFileClient(path);
            var response = await fileClient.ReadAsync(ct);
            using var stream = response.Value.Content;
            using var sha = SHA256.Create();
            var hash = await sha.ComputeHashAsync(stream, ct);
            return (Convert.ToHexString(hash), response.Value.ContentLength);
        }
        catch (Exception ex)
        {
            TimestampedLogger.Warn($"     ⚠ Could not hash {path}: {ex.Message}");
            return (null, 0);
        }
    }

    /// <summary>
    /// Extract the file ID (GUID without extension) from a full ADLS path.
    /// Expected format: .../something/{guid}.csv
    /// </summary>
    private static string? ExtractFileId(string path)
    {
        var fileName = path.Split('/').LastOrDefault();
        if (fileName is null || !fileName.EndsWith(".csv")) return null;
        return fileName[..^4]; // Remove ".csv"
    }

    /// <summary>Shorten a full ADLS path to just the meaningful tail (e.g., "output/green/xxx.csv" or "loading/run/green/xxx.csv").</summary>
    private static string ShortenPath(string path)
    {
        var outputIdx = path.IndexOf("/output/");
        if (outputIdx >= 0) return path[(outputIdx + 1)..];
        var loadingIdx = path.IndexOf("/loading/");
        if (loadingIdx >= 0) return path[(loadingIdx + 1)..];
        return path;
    }
}

namespace FabricLakehouseRaceCondition.Config;

/// <summary>
/// Configuration POCO for the race condition test harness.
/// All settings can be overridden via appsettings.json or command-line arguments.
/// </summary>
public sealed class HarnessConfig
{
    /// <summary>Fabric workspace GUID (required).</summary>
    public string WorkspaceId { get; set; } = string.Empty;

    /// <summary>Lakehouse artifact GUID (required).</summary>
    public string LakehouseId { get; set; } = string.Empty;

    /// <summary>Number of files to upload before triggering renames.</summary>
    public int SeedFileCount { get; set; } = 20;

    /// <summary>Number of files to upload during/after the rename race.</summary>
    public int ConcurrentUploadCount { get; set; } = 30;

    /// <summary>Minimum size of each uploaded file in KB.</summary>
    public int MinFileSizeKb { get; set; } = 16;

    /// <summary>Maximum size of each uploaded file in KB.</summary>
    public int MaxFileSizeKb { get; set; } = 8192;

    /// <summary>Delay between file uploads in milliseconds.</summary>
    public int UploadDelayMs { get; set; } = 50;

    /// <summary>Maximum number of uploads in flight simultaneously.</summary>
    public int MaxConcurrentUploads { get; set; } = 5;

    /// <summary>
    /// Number of background-stream files to upload before triggering the concurrent renames.
    /// </summary>
    public int RenameDelayAfterSeedFiles { get; set; } = 10;

    /// <summary>Number of files to upload AFTER renames have completed (stale-path test).</summary>
    public int PostRenameUploadCount { get; set; } = 10;

    /// <summary>Maximum iterations. 0 = run forever until anomaly or Ctrl+C.</summary>
    public int MaxIterations { get; set; } = 0;

    /// <summary>Pause and wait for keypress when an anomaly is detected.</summary>
    public bool PauseOnAnomaly { get; set; } = true;

    // ── Derived helpers ──────────────────────────────────────────────

    /// <summary>OneLake DFS endpoint.</summary>
    public static readonly Uri OneLakeEndpoint = new("https://onelake.dfs.fabric.microsoft.com");

    /// <summary>ADLS filesystem name (= WorkspaceId).</summary>
    public string FileSystemName => WorkspaceId;

    /// <summary>Base path under which all files live.</summary>
    public string BasePath => $"{LakehouseId}/Files";

    /// <summary>Path to the output/green directory.</summary>
    public string OutputGreenPath => $"{BasePath}/output/green";

    /// <summary>Path to the output directory.</summary>
    public string OutputPath => $"{BasePath}/output";

    /// <summary>Path to the loading directory.</summary>
    public string LoadingPath => $"{BasePath}/loading";

    /// <summary>Build a loading target path for a given run ID.</summary>
    public string LoadingRunPath(string runId) => $"{BasePath}/loading/{runId}/green";

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(WorkspaceId))
            throw new InvalidOperationException("WorkspaceId is required. Set it in appsettings.json or via --WorkspaceId.");
        if (string.IsNullOrWhiteSpace(LakehouseId))
            throw new InvalidOperationException("LakehouseId is required. Set it in appsettings.json or via --LakehouseId.");
    }
}

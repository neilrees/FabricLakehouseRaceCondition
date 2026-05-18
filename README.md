# Fabric Lakehouse Race Condition Test Harness

A .NET 10 console application that attempts to test a hypothesised rename race condition on OneLake via ADLS Gen2. This harness was unable to reproduce the issue but is shared for reference.

## How It Works

The harness runs a multi-phase loop per iteration:

1. **SEED** — Upload N files to `Files/output/green/` (establishes baseline)
2. **RACE** — Start background uploads, then fire 2 simultaneous renames of `output/green` → `loading/{runId}/green` mid-stream
3. **DRAIN** — Wait for all in-flight uploads to complete (retries on 404)
4. **POST-RENAME** — Upload additional files to `output/green/` *after* renames have completed (tests stale-path write-through hypothesis)
5. **VERIFY** — List all files in `output/` and `loading/`, cross-reference against upload records, download and SHA-256 hash any duplicates
6. **REPORT** — Log results; pause on errors, continue on warnings
7. **CLEANUP** — Delete test directories and repeat

### Upload Pattern

The upload logic mirrors our production uploader:
- No overwrite flag (default = false)
- HTTP 409 Conflict treated as success (file already exists)
- HTTP 404 triggers retry with random 1–5s backoff, up to 5 attempts
- No per-upload directory creation; `output/` created once at startup, `green/` is implicit

### Anomaly Detection

| Type | Severity | Meaning |
|------|----------|---------|
| **WrongLocation** | Error | File uploaded AFTER rename completed but found in `loading/` — would indicate stale-path issue |
| **MissingFile** | Error | Upload succeeded but file not found anywhere |
| **Duplicate** (different content) | Error | Same filename in multiple locations with different data |
| **Duplicate** (identical content) | Warning | Expected retry behaviour (SDK resets stream) |
| **Duplicate** (one zero-length) | Warning | Expected rename-during-write side effect |

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)
- Azure credentials configured (`az login` or environment variables for `DefaultAzureCredential`)
- A Fabric Workspace with a Lakehouse (the harness creates/deletes subdirectories under `Files/`)

## Configuration

Edit `src/FabricLakehouseRaceCondition/appsettings.json` or pass settings as command-line arguments:

```bash
dotnet run --project src/FabricLakehouseRaceCondition -- \
  --WorkspaceId "your-workspace-guid" \
  --LakehouseId "your-lakehouse-guid"
```

| Setting | Default | Description |
|---------|---------|-------------|
| WorkspaceId | *(required)* | Fabric workspace GUID |
| LakehouseId | *(required)* | Lakehouse artifact GUID |
| SeedFileCount | 20 | Files to upload before renames |
| ConcurrentUploadCount | 30 | Files to upload during the rename race |
| PostRenameUploadCount | 10 | Files to upload after renames complete |
| MinFileSizeKb | 16 | Minimum file size (KB) |
| MaxFileSizeKb | 8192 | Maximum file size (KB) |
| UploadDelayMs | 50 | Stagger between upload launches (ms) |
| MaxConcurrentUploads | 5 | Max simultaneous uploads |
| RenameDelayAfterSeedFiles | 10 | Background uploads before triggering renames |
| MaxIterations | 0 | 0 = run forever until error or Ctrl+C |
| PauseOnAnomaly | true | Pause on error-severity anomalies |

### Recommended settings for Azure VM (co-located with Fabric capacity)

```json
{
  "SeedFileCount": 30,
  "ConcurrentUploadCount": 50,
  "PostRenameUploadCount": 20,
  "MinFileSizeKb": 512,
  "MaxFileSizeKb": 8192,
  "UploadDelayMs": 10,
  "MaxConcurrentUploads": 15,
  "RenameDelayAfterSeedFiles": 15
}
```

## Building & Running

```bash
dotnet build
dotnet run --project src/FabricLakehouseRaceCondition -- \
  --WorkspaceId "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" \
  --LakehouseId "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
```

Press **Ctrl+C** to stop gracefully after the current iteration completes.

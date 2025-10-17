# BlobBatchDelete

High-throughput Azure Blob Storage deleter for massive clean-ups (including **versioned** blobs).  
Uses **Blob Batch** (≤256 deletes/request), **parallel workers**, and safe URI handling (works with blob names containing `{}`, spaces, etc.).  
Supports **folder-by-folder processing**, **enumeration heartbeats**, and **API version pinning** for compatibility with newer storage accounts.

---

## Requirements

### To build from source
- **.NET SDK 8.0+** (Windows/Linux/macOS)
  - Check with: `dotnet --version`
- **NuGet access** to `https://api.nuget.org/v3/index.json`
- **Packages** (resolved automatically on restore):
  - `Azure.Storage.Blobs` ≥ **12.22.0**
  - `Azure.Storage.Blobs.Batch` ≥ **12.16.0**

### To run
- **Option A (recommended): Self-contained single EXE** – no .NET install needed.
- **Option B: From source** – requires .NET SDK 8.0+.

### Azure permissions & setup
You need one of:
- **SAS token** with **Delete**, **Write**, **List**, and **Object Version Delete** rights.
- **Account key** (Shared Key).

Role examples if using Azure RBAC:
- **Storage Blob Data Contributor** or **Storage Blob Data Owner**.
- Must include “Object Version Delete”.

Notes:
- Blobs under **immutability policy** or **legal hold** can’t be deleted until expired.
- Tool pins the REST API version to `V2023_11_03`.

---

## Build

```bash
dotnet restore
dotnet build -c Release
```

## Publish as a single EXE

```bash
dotnet publish -c Release -r win-x64 --self-contained true -p:PublishSingleFile=true
```

Output:  
`bin\Release\net8.0\win-x64\publish\BlobBatchDelete.exe`

---

## Run

### With SAS
```powershell
.\BlobBatchDelete.exe `
  --service "https://<account>.blob.core.windows.net/?<SAS>" `
  --container immutable `
  --prefix "Folder1/Folder2/..." `
  --batch 256 `
  --concurrency 64 `
  --foldersParallel 1 `
  --heartbeat 10000 `
  --log "C:\Logs\ImmutableCleanup.log"
```

### With Account Key
```powershell
.\BlobBatchDelete.exe `
  --service "https://<account>.blob.core.windows.net" `
  --key "<ACCOUNT_KEY>" `
  --container immutable `
  --prefix "Folder1/Folder2/..." `
  --batch 256 --concurrency 64
```

✅ Deletes **versions first**, then **root blobs**.  
✅ Logs continuously (you can tail it while running).

---

## Command-line Options and Defaults

| Switch | Required | Default | Description |
|--------|-----------|----------|-------------|
| `--service` | ✅ | — | Storage service URI (may include SAS) |
| `--key` | ❌ | — | Storage account key (omit if SAS) |
| `--container` | ✅ | — | Target container |
| `--prefix` | ✅ | — | Prefix path under which to delete |
| `--batch` | ❌ | 256 | Blobs per batch (max 256) |
| `--concurrency` | ❌ | 64 | Parallel workers |
| `--foldersParallel` | ❌ | 1 | Folders processed simultaneously |
| `--heartbeat` | ❌ | 10000 | Log every N blobs enumerated |
| `--log` | ❌ | batch-delete.log | Log file name/path |
| `--logSuccess` | ❌ | false | Log every successful delete (large log) |

---

## Logging

Default: `batch-delete.log` in working directory.  
Override with `--log "C:\Logs\MyRun.log"`.

Example entries:
```
2025-10-15 09:21:55Z Start. container=immutable prefix=... batch=256 concurrency=64
2025-10-15 09:22:10Z HEARTBEAT Folder1/... queued=10000
2025-10-15 09:23:01Z FOLDER-START Folder2/.../folder3/
2025-10-15 09:40:02Z FOLDER-END Folder2/.../folder3/
2025-10-15 09:40:02Z FOLDER-SUMMARY ... enumerated=128000 deleted=127500 failed=500
2025-10-15 09:40:03Z Done.
```

---

## Performance Tuning

| Goal | Batch | Concurrency | Notes |
|------|--------|--------------|-------|
| Fastest | 256 | 64 | Ideal for large cleanups |
| Moderate | 128 | 32 | Safer for throttled accounts |
| Safe / Test | 64 | 8 | Low load for trial runs |

Use `--foldersParallel 1` for sequential folders or higher for parallel ones.

---

## Troubleshooting

- **403 AuthorizationFailure** → SAS/key missing permissions.
- **404 BlobNotFound** → Expected (already deleted).
- **409 BlobImmutableDueToPolicy** → Blob still immutable.
- **No logs after “Start.”** → Still enumerating. Use `--heartbeat 5000` for progress.
- **429 / ServerBusy** → Lower `--concurrency`.

---

## Security

- Use **short-lived SAS** for runs.  
- Rotate account keys after use.

---

## License

MIT License — see [LICENSE](./LICENSE)

---

## Disclaimer

Use at your own risk. Deletions are permanent.  
Test on non-production prefixes first.

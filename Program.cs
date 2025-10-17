using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Batch;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace BlobBatchDeleteApp;

class Options
{
    public required Uri ServiceUri { get; init; }
    public string? AccountKey { get; init; }              // optional if SAS on ServiceUri
    public required string Container { get; init; }
    public required string Prefix { get; init; }          // root prefix to scan
    public int BatchSize { get; init; } = 256;            // max 256 per request
    public int Concurrency { get; init; } = 64;           // workers per folder
    public int FoldersParallel { get; init; } = 1;        // how many folders to process in parallel (1 = folder-by-folder)
    public int HeartbeatEvery { get; init; } = 10000;     // log heartbeat every N items discovered
    public string LogPath { get; init; } = "batch-delete.log";
    public bool LogSuccess { get; init; } = false;        // set true to log every success
}

static class Args
{
    public static Options Parse(string[] args)
    {
        string? service = null, key = null, container = null, prefix = null, log = null;
        int batch = 256, conc = 64, hb = 10000, fp = 1; bool logSuccess = false;

        for (int i = 0; i < args.Length; i++)
        {
            string a = args[i];
            string Next() => (i + 1 < args.Length) ? args[++i] : throw new ArgumentException($"Missing value after {a}");
            switch (a)
            {
                case "--service": service = Next(); break;
                case "--key": key = Next(); break;
                case "--container": container = Next(); break;
                case "--prefix": prefix = Next(); break;
                case "--batch": batch = int.Parse(Next()); break;
                case "--concurrency": conc = int.Parse(Next()); break;
                case "--foldersParallel": fp = int.Parse(Next()); break;
                case "--heartbeat": hb = int.Parse(Next()); break;
                case "--log": log = Next(); break;
                case "--logSuccess": logSuccess = bool.Parse(Next()); break;
                default: throw new ArgumentException($"Unknown arg: {a}");
            }
        }

        if (string.IsNullOrWhiteSpace(service)) throw new ArgumentException("--service is required");
        if (string.IsNullOrWhiteSpace(container)) throw new ArgumentException("--container is required");
        if (string.IsNullOrWhiteSpace(prefix)) throw new ArgumentException("--prefix is required");

        return new Options
        {
            ServiceUri = new Uri(service!, UriKind.Absolute),
            AccountKey = key,
            Container = container!,
            Prefix = prefix!,
            BatchSize = Math.Clamp(batch, 1, 256),
            Concurrency = Math.Max(conc, 1),
            FoldersParallel = Math.Max(fp, 1),
            HeartbeatEvery = Math.Max(hb, 1000),
            LogPath = log ?? "batch-delete.log",
            LogSuccess = logSuccess
        };
    }
}

class Program
{
    static async Task<int> Main(string[] args)
    {
        Options opt;
        try { opt = Args.Parse(args); }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Argument error: {ex.Message}");
            Console.Error.WriteLine(@"Usage:
  dotnet run -- --service https://<account>.blob.core.windows.net[?<SAS>]
                    [--key <ACCOUNT_KEY>] --container <name>
                    --prefix ""Veeam/Backup/..."" [--batch 256] [--concurrency 64]
                    [--foldersParallel 1] [--heartbeat 10000]
                    [--log batch-delete.log] [--logSuccess true|false]");
            return 2;
        }

        // Clients (pin API version for compatibility with newer accounts)
        BlobServiceClient svc = BuildServiceClient(opt);
        var container = svc.GetBlobContainerClient(opt.Container);
        var batchClient = new BlobBatchClient(svc);

        // Logging
        using var logWriter = new StreamWriter(File.Open(opt.LogPath, FileMode.Create, FileAccess.Write, FileShare.Read))
        { AutoFlush = true };
        void Log(string msg) => logWriter.WriteLine($"{DateTime.UtcNow:yyyy-MM-dd HH:mm:ssZ} {msg}");

        Log($"Start. container={opt.Container} prefix={opt.Prefix} batch={opt.BatchSize} concurrency={opt.Concurrency} foldersParallel={opt.FoldersParallel} heartbeatEvery={opt.HeartbeatEvery}");

        // 1) Discover immediate child "folders" under the root prefix (virtual directories)
        var childPrefixes = new System.Collections.Generic.List<string>();
        await foreach (var hi in container.GetBlobsByHierarchyAsync(prefix: NormalizePrefix(opt.Prefix), delimiter: "/"))
        {
            if (hi.IsPrefix && !string.IsNullOrEmpty(hi.Prefix))
                childPrefixes.Add(hi.Prefix);
        }

        // If no child prefixes were found, process the root prefix itself
        if (childPrefixes.Count == 0)
            childPrefixes.Add(NormalizePrefix(opt.Prefix));

        Log($"Discovered {childPrefixes.Count} folder(s) under prefix.");

        // 2) Process folders, folder-by-folder (or a few in parallel if FoldersParallel > 1)
        var folderQueue = new ConcurrentQueue<string>(childPrefixes);
        var folderWorkers = Enumerable.Range(0, opt.FoldersParallel).Select(_ => Task.Run(async () =>
        {
            while (folderQueue.TryDequeue(out var folderPrefix))
            {
                Log($"FOLDER-START {folderPrefix}");
                await ProcessFolderAsync(container, batchClient, folderPrefix, opt, Log);
                Log($"FOLDER-END   {folderPrefix}");
            }
        })).ToArray();

        await Task.WhenAll(folderWorkers);

        Log("All folders completed.");
        Console.WriteLine($"Done. See log: {Path.GetFullPath(opt.LogPath)}");
        return 0;
    }

    // Process one folder prefix: enumerate with heartbeat, then delete in batches with per-folder workers
    static async Task ProcessFolderAsync(BlobContainerClient container, BlobBatchClient batchClient, string folderPrefix, Options opt, Action<string> Log)
    {
        var toDelete = new BlockingCollection<(string name, string? versionId, bool isBase)>(
            boundedCapacity: opt.Concurrency * opt.BatchSize * 4);
        var cts = new CancellationTokenSource();

        long enumerated = 0, deleted = 0, deletedRoots = 0, failed = 0;

        // Producer: versions then base blobs, with heartbeat logs
        var producer = Task.Run(async () =>
        {
            try
            {
                await foreach (var item in container.GetBlobsAsync(BlobTraits.None, BlobStates.Version, folderPrefix))
                {
                    toDelete.Add((item.Name, item.VersionId, false), cts.Token);
                    var e = Interlocked.Increment(ref enumerated);
                    if (e % opt.HeartbeatEvery == 0) Log($"HEARTBEAT {folderPrefix} queued={e}");
                }

                await foreach (var item in container.GetBlobsAsync(BlobTraits.None, BlobStates.None, folderPrefix))
                {
                    toDelete.Add((item.Name, null, true), cts.Token);
                    var e = Interlocked.Increment(ref enumerated);
                    if (e % opt.HeartbeatEvery == 0) Log($"HEARTBEAT {folderPrefix} queued={e}");
                }
            }
            catch (Exception ex)
            {
                Log("ENUM-FAILED " + ex);
                cts.Cancel();
            }
            finally { toDelete.CompleteAdding(); }
        });

        // Consumers: build explicit batches; don't fail whole batch on 404s
        var consumers = Enumerable.Range(0, opt.Concurrency).Select(_ => Task.Run(async () =>
        {
            var names = new System.Collections.Generic.List<(string name, string? versionId, bool isBase)>(opt.BatchSize);

            while (!toDelete.IsCompleted && !cts.IsCancellationRequested)
            {
                try
                {
                    names.Clear();
                    while (names.Count < opt.BatchSize && toDelete.TryTake(out var it, 100))
                        names.Add(it);
                    if (names.Count == 0) continue;

                    var batch = batchClient.CreateBatch();

                    foreach (var (nm, versionId, _) in names)
                    {
                        var bub = new BlobUriBuilder(container.Uri) { BlobName = nm };
                        if (versionId is not null) bub.VersionId = versionId;
                        batch.DeleteBlob(bub.ToUri(), DeleteSnapshotsOption.None);
                    }

                    try
                    {
                        await batchClient.SubmitBatchAsync(batch, throwOnAnyFailure: false, cancellationToken: cts.Token);

                        foreach (var (nm, versionId, isBase) in names)
                        {
                            Interlocked.Increment(ref deleted);
                            if (isBase) Interlocked.Increment(ref deletedRoots);
                            if (opt.LogSuccess)
                                Log(isBase ? $"DELETED-ROOT {nm}" : $"DELETED {nm} v={versionId}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Log("BATCH-FAILED " + ex);
                        Interlocked.Add(ref failed, names.Count);
                    }
                }
                catch (InvalidOperationException) { break; }   // collection completed
                catch (Exception ex) { Log("WORKER-FAILED " + ex); }
            }
        })).ToArray();

        await Task.WhenAll(consumers.Prepend(producer));

        Log($"FOLDER-SUMMARY {folderPrefix} enumerated={enumerated} deleted={deleted} roots={deletedRoots} failed={failed}");
    }

    static string NormalizePrefix(string p) => p.EndsWith("/") ? p : p + "/";

    static BlobServiceClient BuildServiceClient(Options opt)
    {
        // Pick a version supported by your packages; if this enum isn't available in your SDK,
        // switch to the newest one you see (e.g., V2021_12_02).
        var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2023_11_03);

        if (!string.IsNullOrEmpty(opt.AccountKey) && string.IsNullOrEmpty(opt.ServiceUri.Query))
        {
            var accountName = opt.ServiceUri.Host.Split('.')[0];
            var creds = new StorageSharedKeyCredential(accountName, opt.AccountKey);
            return new BlobServiceClient(opt.ServiceUri, creds, options);
        }
        return new BlobServiceClient(opt.ServiceUri, options);   // SAS in URI
    }
}

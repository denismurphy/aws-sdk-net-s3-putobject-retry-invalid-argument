# AWSSDK.S3 PutObject retry races the failed attempt's body read

Self-contained reproduction (SSCCE) for a bug in the AWS SDK for .NET (`AWSSDK.S3`, `AWSSDK.Core`).

Upstream issue: _<link to aws/aws-sdk-net issue>_

When a `PutObject` attempt fails because its connection was reset, the SDK retries with the **same request
stream** and rewinds it without waiting for the failed attempt's body copy to stop reading it. Depending on
timing, the retry then either:

- **A.** fails with `IOException: Invalid argument` (the stream position went negative), or
- **B.** sends an **empty or truncated body** (the stream position moved forward).

No AWS account or network is needed. The "S3 endpoint" is a local server, and the credentials are dummies.

## Quick start

Requires the .NET 10 SDK.

```
dotnet run -c Release                  # 200 uploads over HTTPS, thread pool busy (default)
dotnet run -c Release -- 200 0         # control: thread pool idle
dotnet run -c Release -- 200 20 http   # plain HTTP instead of HTTPS
dotnet run -c Release -- mechanism     # SDK-free demonstration of the FileStream behaviour behind A
```

Arguments are positional: `<uploads> <busy-workers> <http|https>`. Defaults: 200, 2 × CPU count, https.

Typical output (macOS, HTTPS, busy pool):

```
upload 124 FAILED: IOException: Invalid argument : '/tmp/putobject-repro-.../part-0124'
scheme=https uploads=200 busyWorkers=20 resets=200 failedAttempts=400 invalidArgumentAttempts=200 succeeded=124 failed=76 bodiesAccepted=124 bodyMismatches=0
```

`failedAttempts` counts failed HTTP attempts seen by the recording handler: the injected reset (1 per upload) plus
every retry that failed. `invalidArgumentAttempts` is the subset caused by outcome A. `bodyMismatches` counts bodies
the server accepted whose decoded payload differs from the file (outcome B).

## How the bug works

1. `PutObjectRequest.SetupForFilePath()` opens the file once per request:
   `new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.Read)`.
2. `PutObjectRequest` sends `Expect: 100-continue`, so `SocketsHttpHandler` copies the body **concurrently** with
   reading the response. When the response side fails (connection reset) on a connection that is not yet
   disposed, .NET's `HttpConnection.SendAsync` only observes the body-send task with `LogExceptions(...)`,
   disposes the connection and throws ([`HttpConnection.cs` L862-L894, .NET 10.0.12][net]). The copy can
   therefore still be inside `FileStream.ReadAsync` when the SDK sees the failure.
3. `RetryHandler` calls `PrepareForRetry`, which sets `seekableStream.Position = OriginalStreamPosition`
   ([`RetryHandler.cs` L276-L299][retry]). On the stale-connection path it retries immediately, with no backoff.
4. `HttpHandler` builds the next attempt's body from the same stream. If the position is not 0 it sends only
   `Length - Position` bytes ([`HttpHandler.cs` L348-L351][http], [L405-L408][http2]).
5. .NET's `FileStream.ReadAsync` advances the position by the **requested** count before reading
   (`Interlocked.Add(ref _filePosition, destination.Length)`) and subtracts the shortfall when a short read
   completes (`OnIncompleteOperation`) ([`OSFileStreamStrategy.cs`, .NET 10.0.12][fs]).

Two timelines follow from the race between the abandoned read and step 3:

```
A. read in flight when the rewind happens
   t0  abandoned attempt: ReadAsync(8192) on a 200-byte file   position 0 -> 8192
   t1  SDK rewinds                                             position -> 0
   t2  the read completes with 200 bytes, subtracts 7992       position -> -7992
   t3  retry's first read: pread at offset -7992               EINVAL -> IOException "Invalid argument"

B. read starts after the rewind
   t1  SDK rewinds                                             position -> 0
   t2  abandoned copy issues its read                          position -> 200 (EOF)
   t3  HttpHandler sees Position != 0 and sends Length - Position = 0 bytes
       Content-Length 207, X-Amz-Decoded-Content-Length: 200, body = final "0" chunk + CRC32 of nothing
```

Small files are the worst case for A: the first read of the file is always short, so an outstanding read always
reserves more than it returns.

### Why uploads fail, not just attempts (outcome A)

Each retry acquires capacity from the process-wide, per-endpoint `CapacityManager` (500 tokens, retry cost 5 in
[`StandardRetryPolicy.cs` L36-L38][std]); a success releases one retry cost. A reset costs one retry, and the
`Invalid argument` attempt costs a second, so a burst of resets drains the quota and later uploads throw the
`IOException` with no retry. Setting `ThrottleRetries = false` removes the failures (see
[`results/macos-experiments-https-busy.txt`](results/macos-experiments-https-busy.txt)).

## What `Program.cs` does

| Piece | Purpose |
|---|---|
| busy-pool loop (`busyWorkers` tasks, each spins ~2 ms then `Task.Yield()`) | Keeps the thread pool saturated so the `FileStream`'s queued read is still outstanding when the reset is reported. This widens the race window on macOS; on Linux the bug also occurs with an idle pool. |
| `ResetFirstAttemptServer` | Local TCP server (TLS with an in-process self-signed certificate for HTTPS). For the first request per key it answers `100 Continue`, then closes with `LingerState(true, 0)`, which sends a TCP RST. Later attempts for the key are read in full. |
| body check in `Handle` | Decodes the `aws-chunked` body (`DecodeAwsChunked`) and compares it byte for byte with the file. Counts and prints any difference. The server does **not** verify signatures, so it accepts bodies real S3 rejects. |
| `RecordingFactory` / `Recorder` | A `HttpClientFactory` wrapping the default `SocketsHttpHandler` to log each failed attempt's exception, and to accept the self-signed certificate. It does not alter behaviour. |
| `Mechanism()` | 14 lines, no SDK: start a `FileStream.ReadAsync(8192)` on a 200-byte file, set `Position = 0`, await the read, check the position, then read again. Shows the negative position and the `Invalid argument` read. |

`experiments/Program.cs` is the same program with environment switches (`REPRO_THROTTLE=false`,
`REPRO_MAX_ERROR_RETRY`, `REPRO_STALE_RETRIES`, `REPRO_APP_RETRIES`, `REPRO_STREAM=memory`) used for the workaround
runs below. Its defaults behave like `Program.cs`.

## Results

Each row is **one run** of 200 uploads of 200-byte files, AWSSDK.S3 4.0.104 / AWSSDK.Core 4.0.102.8, first
connection of every upload reset right after `100 Continue`. Counts vary from run to run: see the raw files in
[`results/`](results/) for repeated runs (for example, Linux HTTP/busy gave 21 and 37 failed uploads in two runs).

| Platform | Scheme | Thread pool | `Invalid argument` attempts | Uploads failed |
|---|---|---|---|---|
| macOS 27.0 arm64 | HTTPS | busy | 200 | 76 |
| macOS 27.0 arm64 | HTTPS | idle | 0 | 0 |
| macOS 27.0 arm64 | HTTP | busy | 200 | 76 |
| macOS 27.0 arm64 | HTTP | idle | 0 | 0 |
| Ubuntu 24.04 container, aarch64 | HTTPS | idle | 7 | 0 |
| Ubuntu 24.04 container, aarch64 | HTTPS | busy | 108 | 0 |
| Ubuntu 24.04 container, aarch64 | HTTP | idle | 19 | 0 |
| Ubuntu 24.04 container, aarch64 | HTTP | busy | 168 | 37 |

Other package versions (macOS, HTTPS, [`results/macos-package-versions.txt`](results/macos-package-versions.txt)):

- **AWSSDK.S3 4.0.104.1** (latest on NuGet when tested): busy 198 attempts / 73 failed; idle 3 / 0.
- **AWSSDK.S3 3.7.511.8** (AWSSDK.Core 3.7.501.1): `Invalid argument` attempts also occur (3 to 8 idle, 54 to 56
  busy, four runs). Most failed uploads on this version are plain
  `IOException: Unable to read data from the transport connection: Connection reset by peer`, so its
  failed-upload counts are **not** attributable to this bug alone.

`mechanism` (macOS: 999 and 997 of 1000 across two runs; Linux: 999 of 1000):

```
trials=1000 negativePosition=999 (e.g. -7992) nextReadThrew=999
  System.IO.IOException: Invalid argument : '/tmp/tmpZJ1o6O.tmp'
```

### Outcome B (empty retry body)

When the retry's stream position has moved forward, the retry sends fewer bytes than the file. The harness's
server decodes every accepted `aws-chunked` body and compares it with the file. On Linux, plain HTTP, idle thread
pool, 12 runs of 300 uploads (3,600 uploads) it accepted **5 empty bodies** and answered 200
([`results/linux-http-idle-hunt-3600.txt`](results/linux-http-idle-hunt-3600.txt)). Earlier saved runs in the
same configuration show 4 more in 1,600 uploads ([`results/linux-hunt-mismatch.txt`](results/linux-hunt-mismatch.txt),
[`results/linux-https-and-http.txt`](results/linux-https-and-http.txt),
[`results/linux-http-first-runs.txt`](results/linux-http-first-runs.txt)). Every one has the same form: a
request carrying `amz-sdk-request: attempt=1`, `Content-Length: 207`, `X-Amz-Decoded-Content-Length: 200`, and a body that is
only the final zero-length chunk plus the CRC32 of empty data. `attempt=1` on a request whose first connection was
already reset fits the stale-connection retry path, which retries without updating the attempt header (inferred, not
traced). It is rarer than outcome A and was not seen on macOS or with HTTPS in the saved runs.

The local server does not verify signatures. This repo does not show what real S3 does with such a request.

### Workarounds (macOS, HTTPS, busy pool, 200 uploads; [`results/macos-experiments-https-busy.txt`](results/macos-experiments-https-busy.txt))

| Change | `Invalid argument` attempts | Uploads failed |
|---|---|---|
| none (baseline) | 200 | 76 |
| `ThrottleRetries = false` | 199 | 0 |
| `MaxErrorRetry = 0` and `MaxStaleConnectionRetries = 0`, retry in application code with a new request (so a new `FileStream`) per attempt | 0 | 0 |
| `InputStream = new MemoryStream(File.ReadAllBytes(path))` | 0 | 0 |
| application-level retries only, SDK retries left on | 199 | 0 (retries mask the failures; the SDK still hits the bug) |

`ThrottleRetries = false` hides the failed uploads but not the failed attempts. The `MemoryStream` variant costs
memory for large files. All of these were run against the local server only.

## Caveats

- The reset is injected by a local server, immediately after `100 Continue`. Real networks reset at other points;
  the race needs an attempt that fails while its body copy is still running.
- The local server does not verify signatures, so "accepted" here does not mean real S3 would accept the body.
- Results depend on thread-pool load and OS. Treat the numbers above as examples, not rates.
- Tested on .NET 10.0.x only (runtime 10.0.12 on Linux).

## Environment

- macOS 27.0 (arm64) and Ubuntu 24.04.5 LTS container (Linux 7.0.14, aarch64)
- .NET 10 (runtime 10.0.12 on Linux)
- AWSSDK.S3 4.0.104 (also 4.0.104.1 and 3.7.511.8), AWSSDK.Core 4.0.102.8 (3.7.501.1 for 3.7.511.8)

[retry]: https://github.com/aws/aws-sdk-net/blob/cebda0b9f508146a4d4e5965a1041fafef7ea210/sdk/src/Core/Amazon.Runtime/Pipeline/RetryHandler/RetryHandler.cs#L276-L299
[http]: https://github.com/aws/aws-sdk-net/blob/cebda0b9f508146a4d4e5965a1041fafef7ea210/sdk/src/Core/Amazon.Runtime/Pipeline/HttpHandler/HttpHandler.cs#L348-L351
[http2]: https://github.com/aws/aws-sdk-net/blob/cebda0b9f508146a4d4e5965a1041fafef7ea210/sdk/src/Core/Amazon.Runtime/Pipeline/HttpHandler/HttpHandler.cs#L405-L408
[std]: https://github.com/aws/aws-sdk-net/blob/cebda0b9f508146a4d4e5965a1041fafef7ea210/sdk/src/Core/Amazon.Runtime/Pipeline/RetryHandler/StandardRetryPolicy.cs#L36-L38
[net]: https://github.com/dotnet/runtime/blob/v10.0.12/src/libraries/System.Net.Http/src/System/Net/Http/SocketsHttpHandler/HttpConnection.cs#L862-L894
[fs]: https://github.com/dotnet/runtime/blob/v10.0.12/src/libraries/System.Private.CoreLib/src/System/IO/Strategies/OSFileStreamStrategy.cs#L81-L82

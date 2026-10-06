// PutObjectAsync(FilePath): after a connection reset, the retry reads the request's FileStream at a
// negative offset and fails with "IOException: Invalid argument".
//
//   dotnet run -c Release                        200 uploads over HTTPS, thread pool busy (default)
//   dotnet run -c Release -- 200 0               same, thread pool idle (control)
//   dotnet run -c Release -- 200 20 http         plain HTTP instead of HTTPS
//   dotnet run -c Release -- mechanism           the FileStream behaviour the SDK retry runs into
//
// Self-contained: the "S3" endpoint is a local server (TLS with an in-process self-signed certificate, or
// plain HTTP) and the credentials are dummies. The server answers the first attempt for each key with
// "100 Continue" (PutObject sends Expect: 100-continue, so the client starts copying the body) and then
// resets the TCP connection. Later attempts are accepted, and their body is decoded and compared byte for
// byte with the file.
using System.Diagnostics;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;

if (args.Length > 0 && args[0] == "mechanism") { await Mechanism(); return; }
var uploads = args.Length > 0 ? int.Parse(args[0]) : 200;
var busyWorkers = args.Length > 1 ? int.Parse(args[1]) : 2 * Environment.ProcessorCount;
var https = (args.Length > 2 ? args[2] : "https") == "https";

// A loaded process: CPU-bound work items keep the thread pool busy, so the FileStream's queued read
// can still be outstanding when HttpClient reports the reset to the SDK.
using var stopLoad = new CancellationTokenSource();
for (var w = 0; w < busyWorkers; w++)
    _ = Task.Run(async () =>
    {
        while (!stopLoad.IsCancellationRequested)
        {
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed.TotalMilliseconds < 2) Thread.SpinWait(50);
            await Task.Yield();
        }
    });

var server = new ResetFirstAttemptServer(https);
var errors = new List<string>();
var s3 = new AmazonS3Client(
    new BasicAWSCredentials("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
    Configure(new AmazonS3Config
    {
        ServiceURL = $"{(https ? "https" : "http")}://localhost:{server.Start()}",
        ForcePathStyle = true,
        // Observes failed attempts only (and accepts the local server's self-signed certificate).
        HttpClientFactory = new RecordingFactory(errors),
    }));

var dir = Directory.CreateTempSubdirectory("putobject-repro-");
int succeeded = 0, failed = 0;
for (var i = 0; i < uploads; i++)
{
    var path = Path.Combine(dir.FullName, $"part-{i:D4}");
    await File.WriteAllBytesAsync(path, ResetFirstAttemptServer.Content(i));   // 200 bytes
    try
    {
        var appRetries = int.TryParse(Environment.GetEnvironmentVariable("REPRO_APP_RETRIES"), out var ar) ? ar : 0;
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                var req = new PutObjectRequest { BucketName = "bucket", Key = $"part-{i:D4}" };
                if (Environment.GetEnvironmentVariable("REPRO_STREAM") == "memory") req.InputStream = new MemoryStream(File.ReadAllBytes(path));
                else req.FilePath = path;
                await s3.PutObjectAsync(req);
                break;
            }
            catch when (attempt < appRetries) { }
        }
        succeeded++;
    }
    catch (Exception e)
    {
        if (failed++ < 3) Console.WriteLine($"upload {i} FAILED: {e.GetType().Name}: {e.Message}");
    }
}
stopLoad.Cancel();
var invalidArgument = errors.Where(e => e.Contains("Invalid argument")).ToList();
Console.WriteLine($"scheme={(https ? "https" : "http")} uploads={uploads} busyWorkers={busyWorkers} resets={server.Resets} " +
                  $"failedAttempts={errors.Count} invalidArgumentAttempts={invalidArgument.Count} " +
                  $"succeeded={succeeded} failed={failed} bodiesAccepted={server.Accepted} bodyMismatches={server.Mismatches}");
lock (server.MismatchReports)
    foreach (var report in server.MismatchReports) Console.WriteLine("\nBODY MISMATCH: " + report);
if (invalidArgument.Count > 0) Console.WriteLine("\nFirst 'Invalid argument' attempt:\n" + invalidArgument[0]);
dir.Delete(true);

static AmazonS3Config Configure(AmazonS3Config c)
{
    if (Environment.GetEnvironmentVariable("REPRO_THROTTLE") == "false") c.ThrottleRetries = false;
    if (int.TryParse(Environment.GetEnvironmentVariable("REPRO_MAX_ERROR_RETRY"), out var r)) c.MaxErrorRetry = r;
    if (int.TryParse(Environment.GetEnvironmentVariable("REPRO_STALE_RETRIES"), out var st)) c.MaxStaleConnectionRetries = st;
    return c;
}

// An async read still outstanding while the stream is rewound, as the SDK's retry does it.
static async Task Mechanism()
{
    var path = Path.GetTempFileName();
    await File.WriteAllBytesAsync(path, new byte[200]);
    int trials = 1000, negative = 0, threw = 0;
    long example = 0;
    string message = null;
    for (var i = 0; i < trials; i++)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read); // as SetupForFilePath
        var buffer = new byte[8192];
        var outstanding = fs.ReadAsync(buffer, 0, buffer.Length);   // the failed attempt's read
        fs.Position = 0;                                              // RetryHandler.PrepareForRetry
        await outstanding;                                            // completes late: 200 of 8192 bytes
        if (fs.Position < 0) { negative++; example = fs.Position; }
        try { _ = await fs.ReadAsync(buffer, 0, buffer.Length); }     // the next attempt's first read
        catch (IOException e) { threw++; message ??= $"{e.GetType().FullName}: {e.Message}"; }
    }
    File.Delete(path);
    Console.WriteLine($"trials={trials} negativePosition={negative} (e.g. {example}) nextReadThrew={threw}\n  {message}");
}

sealed class RecordingFactory(List<string> sink) : HttpClientFactory
{
    public override HttpClient CreateHttpClient(IClientConfig clientConfig) => new(new Recorder(new SocketsHttpHandler
    {
        SslOptions = { RemoteCertificateValidationCallback = (_, _, _, _) => true },   // local self-signed cert
    }, sink));
    public override bool UseSDKHttpClientCaching(IClientConfig clientConfig) => true;
    public override bool DisposeHttpClientsAfterUse(IClientConfig clientConfig) => false;
    public override string GetConfigUniqueString(IClientConfig clientConfig) => "repro";
}

sealed class Recorder(HttpMessageHandler inner, List<string> sink) : DelegatingHandler(inner)
{
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        try { return await base.SendAsync(request, ct); }
        catch (Exception e) { lock (sink) sink.Add(e.ToString()); throw; }
    }
}

sealed class ResetFirstAttemptServer(bool https)
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly HashSet<string> _seen = new();
    private readonly X509Certificate2 _certificate = https ? SelfSignedCertificate() : null;
    public int Resets, Accepted, Mismatches;
    public readonly List<string> MismatchReports = new();

    private static X509Certificate2 SelfSignedCertificate()
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=localhost", key, HashAlgorithmName.SHA256);
        var names = new SubjectAlternativeNameBuilder();
        names.AddDnsName("localhost");
        request.CertificateExtensions.Add(names.Build());
        using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        return X509CertificateLoader.LoadPkcs12(certificate.Export(X509ContentType.Pfx), null);
    }

    public static byte[] Content(int index) { var b = new byte[200]; new Random(index).NextBytes(b); return b; }

    public int Start()
    {
        _listener.Start();
        _ = Task.Run(async () => { while (true) { var c = await _listener.AcceptTcpClientAsync(); _ = Task.Run(() => Handle(c)); } });
        return ((IPEndPoint)_listener.LocalEndpoint).Port;
    }

    private async Task Handle(TcpClient client)
    {
        using var _ = client;
        Stream stream = client.GetStream();
        try
        {
            if (https)
            {
                var tls = new SslStream(stream);
                await tls.AuthenticateAsServerAsync(_certificate);
                stream = tls;
            }
            var lines = (await ReadHeadAsync(stream)).Split("\r\n");
            var key = lines[0].Split(' ')[1];
            bool first;
            lock (_seen) first = _seen.Add(key);
            await stream.WriteAsync("HTTP/1.1 100 Continue\r\n\r\n"u8.ToArray());
            if (first)
            {
                client.Client.LingerState = new LingerOption(true, 0);   // close the TCP socket => RST
                client.Client.Close();
                Interlocked.Increment(ref Resets);
                return;
            }
            var body = new byte[int.Parse(Header(lines, "Content-Length"))];
            await stream.ReadExactlyAsync(body);
            var payload = (Header(lines, "Content-Encoding") ?? "").Contains("aws-chunked") ? DecodeAwsChunked(body) : body;
            var expected = Content(int.Parse(key[(key.LastIndexOf('-') + 1)..]));
            if (!payload.AsSpan().SequenceEqual(expected))
            {
                Interlocked.Increment(ref Mismatches);
                var firstDiff = Enumerable.Range(0, Math.Min(payload.Length, expected.Length))
                    .FirstOrDefault(i => payload[i] != expected[i], -1);
                lock (MismatchReports)
                    MismatchReports.Add($"key={key} expectedLength={expected.Length} decodedLength={payload.Length} " +
                        $"firstDifferentByte={firstDiff}\n  headers: {string.Join(" | ", lines.Skip(1).Where(l => l.Length > 0))}\n" +
                        $"  raw body ({body.Length} bytes): {Convert.ToHexString(body)}\n  expected: {Convert.ToHexString(expected)}");
            }
            await stream.WriteAsync("HTTP/1.1 200 OK\r\nETag: \"0\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"u8.ToArray());
            Interlocked.Increment(ref Accepted);
        }
        catch (Exception) { /* the client abandoned this connection */ }
    }

    private static string Header(string[] lines, string name) =>
        lines.FirstOrDefault(l => l.StartsWith(name + ":", StringComparison.OrdinalIgnoreCase))?.Split(':', 2)[1].Trim();

    private static byte[] DecodeAwsChunked(byte[] body)   // "<hex>[;...]\r\n<data>\r\n" ... "0[;...]\r\n<trailers>"
    {
        var output = new MemoryStream();
        for (var pos = 0; ;)
        {
            var eol = body.AsSpan(pos).IndexOf("\r\n"u8);
            var size = Convert.ToInt32(Encoding.ASCII.GetString(body, pos, eol).Split(';')[0], 16);
            pos += eol + 2;
            if (size == 0) return output.ToArray();
            output.Write(body, pos, size);
            pos += size + 2;
        }
    }

    private static async Task<string> ReadHeadAsync(Stream stream)
    {
        var bytes = new List<byte>();
        var one = new byte[1];
        while (!(bytes.Count >= 4 && bytes[^4] == '\r' && bytes[^3] == '\n' && bytes[^2] == '\r' && bytes[^1] == '\n'))
        {
            if (await stream.ReadAsync(one) == 0) throw new IOException("closed before headers");
            bytes.Add(one[0]);
        }
        return Encoding.ASCII.GetString(bytes.ToArray());
    }
}


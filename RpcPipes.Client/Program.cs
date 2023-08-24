using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RpcPipes.Models;
using RpcPipes.Transport;

const string sendPipe = "TestPipe";
const string receivePipe = "Client.TestPipe";
const string progressPipe = "Progress.TestPipe";

const int connections = 64;
const int tasks = 8196;
const int delay = 15;
const int progress = 3;
const int timeoutMinutes = 3;

var serviceProvider = new ServiceCollection()
    .AddLogging(loggingBuilder => 
    {
        loggingBuilder.SetMinimumLevel(LogLevel.Trace);
        loggingBuilder.AddSimpleConsole(options => 
        {
            options.IncludeScopes = true;
            options.SingleLine = true;
            options.UseUtcTimestamp = true;
            options.TimestampFormat = "u";
        });
    }).BuildServiceProvider();

var logger = serviceProvider.GetRequiredService<ILogger<PipeClient<ProgressMessage>>>();

var serializer = new PipeSerializer();
var progressReplies = new ConcurrentBag<ProgressMessage>();
var progressMessageReceiver = new PipeProgressReceiver(progressReplies);

var pipeClient = new PipeClient<ProgressMessage>(logger, sendPipe, receivePipe, progressPipe, connections, progressMessageReceiver, serializer)
{
    ProgressFrequency = TimeSpan.FromSeconds(progress)
};
Console.CancelKeyPress += delegate (object _, ConsoleCancelEventArgs e) {
    e.Cancel = true;    
    pipeClient.Dispose();
};

logger.LogInformation("Starting client, press Ctrl+C to stop");

var stopwatch = new Stopwatch();
stopwatch.Start();

var requests = Enumerable.Range(0, tasks).Select(i => Task.Run(() => RunTaskHandleError(i))).ToArray();
var replies = await Task.WhenAll(requests);
var errors = replies.Where(receivePipe => receivePipe.Error != null).ToArray();

stopwatch.Stop();

logger.LogInformation("Progress updated {Count}", progressReplies.Count);
logger.LogInformation("Replies {Count}", replies.Length);
logger.LogInformation("Errors {Count}", errors.Length);
logger.LogInformation("Completed in {Time}", stopwatch.Elapsed);
foreach (var e in errors)
{
    logger.LogError(e.Error.ToString());
}
Console.ReadKey();

async Task<(ReplyMessage Reply, Exception Error)> RunTaskHandleError(int i)
{
    var request = new RequestMessage($"Sample request {i}", delay);
    var cts = new CancellationTokenSource();
    cts.CancelAfter(TimeSpan.FromMinutes(timeoutMinutes));
    try
    {
        return (await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, cts.Token), null);
    }
    catch (Exception e)
    {
        return (null, e);
    }
}

try
{
    await pipeClient.DisposeAsync();
    return 0;
}
catch (OperationCanceledException)
{
    return 0;
}
catch (Exception e)
{
    logger.LogError(e.ToString());
    return 1;
}
finally
{
    logger.LogInformation("Server has been stopped");
}
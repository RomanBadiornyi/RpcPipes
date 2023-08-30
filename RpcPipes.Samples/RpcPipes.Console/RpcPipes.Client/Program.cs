using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RpcPipes;
using RpcPipes.Models;
using RpcPipes.Models.PipeSerializers;
using RpcPipes.Models.PipeProgress;

const string sendPipe = "TestPipe";
const string progressPipe = "Progress.TestPipe";

const int connections = 32;
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

await using (var pipeClient = new PipeClient<ProgressMessage>(
    logger, sendPipe, progressPipe, Guid.NewGuid(), connections, progressMessageReceiver, serializer)
    {
        ProgressFrequency = TimeSpan.FromSeconds(progress)
    }) 
{
    Console.CancelKeyPress += delegate (object _, ConsoleCancelEventArgs e) {
        e.Cancel = true;    
        pipeClient.Dispose();
    };

    logger.LogInformation("Starting client, press Ctrl+C to interrupt");

    var stopwatch = new Stopwatch();
    IEnumerable<Task<(ReplyMessage Reply, Exception Error)>> requests = null;
    (ReplyMessage Reply, Exception Error)[] replies = null;
    (ReplyMessage Reply, Exception Error)[] errors = null;
    try
    {
        stopwatch.Start();
        requests = Enumerable.Range(0, tasks).Select(i => RunTaskHandleError(i));
        replies = await Task.WhenAll(requests.ToArray());
        errors = replies.Where(receivePipe => receivePipe.Error != null).ToArray();
        stopwatch.Stop();        
    }
    catch (Exception e)
    {
        logger.LogError(e, "unhandled error");     
    }
    finally
    {
        logger.LogInformation("Progress updated {Count}", progressReplies.Count);
        logger.LogInformation("Replies {Count}", replies.Length);
        logger.LogInformation("Errors {Count}", errors.Length);
        logger.LogInformation("Completed in {Time}", stopwatch.Elapsed);
        foreach (var e in errors)
        {
            logger.LogError(e.Error.ToString());
        }        
    }
    
    await serviceProvider.DisposeAsync();

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
}
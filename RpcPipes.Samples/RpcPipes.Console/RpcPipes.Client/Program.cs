using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RpcPipes.Models;
using RpcPipes.Models.PipeSerializers;
using RpcPipes.Models.PipeProgress;
using RpcPipes.PipeClient;

const string sendPipe = "TestPipe";
const string progressPipe = "Progress.TestPipe";

const int connections = 32;
const int tasks = 8196 * 4;
const int delay = 30;
const int progress = 3;
const int timeoutMinutes = 10;

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
    logger, sendPipe, progressPipe, $"{sendPipe}.{Guid.NewGuid()}", connections, progressMessageReceiver, serializer)
    {
        ProgressFrequency = TimeSpan.FromSeconds(progress)
    }) 
{
    var c = pipeClient;
    Console.CancelKeyPress += delegate (object _, ConsoleCancelEventArgs e) {
        e.Cancel = true;    
        c.Dispose();
    };

    logger.LogInformation("Starting client, press Ctrl+C to interrupt");

    var stopwatch = new Stopwatch();
    var replies = Array.Empty<(ReplyMessage Reply, Exception Error)>();
    var errors = Array.Empty<(ReplyMessage Reply, Exception Error)>();
    try
    {
        stopwatch.Start();
        var requests = Enumerable.Range(0, tasks).Select(i => RunTaskHandleError(i, pipeClient)).ToArray();
        replies = await Task.WhenAll(requests);
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

        Console.WriteLine("Progress updated {0}", progressReplies.Count);
        Console.WriteLine("Replies {0}", replies.Length);
        Console.WriteLine("Errors {0}", errors.Length);
        Console.WriteLine("Completed in {0}", stopwatch.Elapsed);
        foreach (var e in errors)
        {
            logger.LogError(e.Error.ToString());
        }        
    }
    
    async Task<(ReplyMessage Reply, Exception Error)> RunTaskHandleError(int i, PipeClient<ProgressMessage> c)
    {
        var request = new RequestMessage($"Sample request {i}", delay);
        var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMinutes(timeoutMinutes));
        try
        {
            return (await c.SendRequest<RequestMessage, ReplyMessage>(request, cts.Token), null);
        }
        catch (Exception e)
        {
            return (null, e);
        }
    }    
}

await serviceProvider.DisposeAsync();
Console.WriteLine("Exit in 10 seconds");
await Task.Delay(TimeSpan.FromSeconds(10));

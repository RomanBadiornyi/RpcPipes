using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RpcPipes.Models;
using RpcPipes.Models.PipeSerializers;
using RpcPipes.Models.PipeHeartbeat;
using RpcPipes;

const string sendPipe = "rpc.pipe";

const int clientsCount = 10;
const int connections = 50 / clientsCount;
const int tasks = 25000;
const int delay = 30;
const int startUpDelay = 30;
const int heartbeat = 3;
const int timeoutMinutes = 10;

var serviceProvider = new ServiceCollection()
    .AddLogging(loggingBuilder => 
    {
        loggingBuilder.SetMinimumLevel(LogLevel.Warning);
        loggingBuilder.AddSimpleConsole(options => 
        {
            options.IncludeScopes = true;
            options.SingleLine = true;
            options.UseUtcTimestamp = true;
            options.TimestampFormat = "u";
        });
    }).BuildServiceProvider();

var rand = new Random((int)DateTime.UtcNow.Ticks);
var stopwatch = new Stopwatch();
stopwatch.Start();

var clients = new List<Task>();
for (var i = 0; i < clientsCount; i++)
{
    var taskNum = i;
    clients.Add(Task.Run(() => RunClientTasks(taskNum)));

    async Task RunClientTasks(int clientId)
    {
        var logger = serviceProvider.GetRequiredService<ILogger<PipeTransportClient<PipeHeartbeatMessage>>>();
        var serializer = new PipeSerializer();
        var heartbeatReplies = new ConcurrentBag<PipeHeartbeatMessage>();
        var heartbeatMessageReceiver = new PipeHeartbeatReceiver(heartbeatReplies);

        await using var pipeClient = new PipeTransportClient<PipeHeartbeatMessage>(
        logger, sendPipe, clientId.ToString(), connections, heartbeatMessageReceiver, serializer); 
        var c = pipeClient;
        Console.CancelKeyPress += delegate (object _, ConsoleCancelEventArgs e) {
            e.Cancel = true;    
            c.Dispose();
        };

        logger.LogInformation("Starting client, press Ctrl+C to interrupt");                        
        var replies = Array.Empty<(PipeReplyMessage Reply, Exception Error)>();
        var errors = Array.Empty<(PipeReplyMessage Reply, Exception Error)>();
        try
        {
            
            var requests = Enumerable.Range(0, tasks).Select(task => RunTaskHandleError(task, pipeClient)).ToArray();
            replies = await Task.WhenAll(requests);
            errors = replies.Where(receivePipe => receivePipe.Error != null).ToArray();                        
        }
        catch (Exception e)
        {
            logger.LogError(e, "unhandled error");     
        }
        finally
        {
            Console.WriteLine("Heartbeat updated {0}", heartbeatReplies.Count);
            Console.WriteLine("Replies {0}", replies.Length);
            Console.WriteLine("Errors {0}", errors.Length);                
            foreach (var e in errors)
            {
                logger.LogError(e.Error.ToString());
            }        
        }
        
        async Task<(PipeReplyMessage Reply, Exception Error)> RunTaskHandleError(int requestId, PipeTransportClient transport)
        {
            var request = new PipeRequestMessage($"Sample request {requestId}", delay);
            var cts = new CancellationTokenSource();
            var requestTime = TimeSpan.FromMinutes(timeoutMinutes);
            var heartbeatTime = TimeSpan.FromSeconds(heartbeat);
            cts.CancelAfter(requestTime);
            try
            {
                var requestContext = new PipeRequestContext
                {
                    Deadline = requestTime,
                    Heartbeat = heartbeatTime
                };
                await Task.Delay(TimeSpan.FromSeconds(rand.Next(0, startUpDelay)));
                return (await transport.SendRequest<PipeRequestMessage, PipeReplyMessage>(request, requestContext, cts.Token), null);
            }
            catch (Exception e)
            {
                return (null, e);
            }
        }
    }    
}

await Task.WhenAll(clients);
stopwatch.Stop();

await serviceProvider.DisposeAsync();

Console.WriteLine("Completed in {0}", stopwatch.Elapsed);
Console.WriteLine("Exit in 5 seconds");
await Task.Delay(TimeSpan.FromSeconds(5));
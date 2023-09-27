using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RpcPipes.Models;
using RpcPipes.Models.PipeSerializers;
using RpcPipes.Models.PipeHeartbeat;
using RpcPipes;

const string sendPipe = "rpc.pipe";

const int runsCount = 5;
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

var runCancellation = new CancellationTokenSource();
Console.CancelKeyPress += delegate (object _, ConsoleCancelEventArgs e) 
{
    e.Cancel = true;    
    runCancellation.Cancel();
};

var totalTime = new Stopwatch();
totalTime.Start();
for (var runIndex = 0; runIndex < runsCount; runIndex++)
{
    if (runCancellation.IsCancellationRequested)
        continue;
    
    totalTime.Stop();
    await Task.Delay(TimeSpan.FromSeconds(5));
    totalTime.Start();

    Console.WriteLine("=============================================");
    Console.WriteLine($"Running {runIndex + 1} time");    

    var rand = new Random((int)DateTime.UtcNow.Ticks);    
    var clients = new List<Task>();    

    var stopwatch = new Stopwatch();
    stopwatch.Start();        

    for (var clientIndex = 0; clientIndex < clientsCount; clientIndex++)
    {
        var taskNum = clientIndex;
        clients.Add(Task.Run(() => RunClientTasks(taskNum)));

        async Task RunClientTasks(int clientId)
        {
            var logger = serviceProvider.GetRequiredService<ILogger<PipeTransportClient<PipeHeartbeatMessage>>>();
            var serializer = new PipeMessagePackSerializer();
            var heartbeatReceiver = new PipeHeartbeatReceiverCounter();            
            var receivePipe = clientId.ToString();
            
            await using var pipeClient = new PipeTransportClient<PipeHeartbeatMessage>(logger, sendPipe, receivePipe, connections, heartbeatReceiver, serializer); 
            var c = pipeClient;
            
            runCancellation.Token.Register(async () => 
            {
                Console.WriteLine("Received signal to stop client and all pending tasks");
                await pipeClient.DisposeAsync();
                Console.WriteLine("Client has been stopped");
            });

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
                Console.WriteLine("Heartbeat updated {0}", heartbeatReceiver.ProgressMessages);
                Console.WriteLine("Replies {0}", replies.Length);
                Console.WriteLine("Errors {0}", errors.Length);                
                foreach (var e in errors.GroupBy(e => e.Error.ToString()).Take(10))
                {
                    var groupCount = e.Count();
                    var groupError = e.First().Error;
                    logger.LogError("Received {ErrorsCount} errors: {ErrorMessage}", groupCount, $"{groupError.GetType().Name}: {groupError.Message}");
                }        
            }
            
            async Task<(PipeReplyMessage Reply, Exception Error)> RunTaskHandleError(int requestId, PipeTransportClient transport)
            {
                var request = new PipeRequestMessage($"Sample request {requestId}", delay);
                var requestCancellation = CancellationTokenSource.CreateLinkedTokenSource(runCancellation.Token);
                var requestTime = TimeSpan.FromMinutes(timeoutMinutes);
                var heartbeatTime = TimeSpan.FromSeconds(heartbeat);
                requestCancellation.CancelAfter(requestTime);
                try
                {
                    var requestContext = new PipeRequestContext
                    {
                        Deadline = requestTime,
                        Heartbeat = heartbeatTime
                    };
                    await Task.Delay(TimeSpan.FromSeconds(rand.Next(0, startUpDelay)));
                    return (await transport.SendRequest<PipeRequestMessage, PipeReplyMessage>(request, requestContext, requestCancellation.Token), null);
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
    Console.WriteLine("Completed in {0}", stopwatch.Elapsed);
    Console.WriteLine("=============================================");    
}
totalTime.Stop();
Console.WriteLine("Total time spent {0}", totalTime.Elapsed);

await serviceProvider.DisposeAsync();

Console.WriteLine("Exit in 5 seconds");
await Task.Delay(TimeSpan.FromSeconds(5));
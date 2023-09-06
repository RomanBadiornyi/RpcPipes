﻿using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RpcPipes.Models;
using RpcPipes.Models.PipeSerializers;
using RpcPipes.Models.PipeHeartbeat;
using RpcPipes;

const string sendPipe = "TestPipe";
const string heartbeatPipe = "Heartbeat.TestPipe";

const int connections = 32;
const int tasks = 250000;
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

var logger = serviceProvider.GetRequiredService<ILogger<PipeTransportClient<HeartbeatMessage>>>();

var serializer = new PipeSerializer();
var heartbeatReplies = new ConcurrentBag<HeartbeatMessage>();
var heartbeatMessageReceiver = new PipeHeartbeatReceiver(heartbeatReplies);

await using (var pipeClient = 
    new PipeTransportClient<HeartbeatMessage>(logger, sendPipe, heartbeatPipe, $"{sendPipe}.{Guid.NewGuid()}", connections, heartbeatMessageReceiver, serializer)) 
{
    var c = pipeClient;
    Console.CancelKeyPress += delegate (object _, ConsoleCancelEventArgs e) {
        e.Cancel = true;    
        c.Dispose();
    };

    logger.LogInformation("Starting client, press Ctrl+C to interrupt");
    var rand = new Random((int)DateTime.UtcNow.Ticks);
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
        logger.LogInformation("Heartbeat updated {Count}", heartbeatReplies.Count);
        logger.LogInformation("Replies {Count}", replies.Length);
        logger.LogInformation("Errors {Count}", errors.Length);
        logger.LogInformation("Completed in {Time}", stopwatch.Elapsed);

        Console.WriteLine("Heartbeat updated {0}", heartbeatReplies.Count);
        Console.WriteLine("Replies {0}", replies.Length);
        Console.WriteLine("Errors {0}", errors.Length);
        Console.WriteLine("Completed in {0}", stopwatch.Elapsed);
        foreach (var e in errors)
        {
            logger.LogError(e.Error.ToString());
        }        
    }
    
    async Task<(ReplyMessage Reply, Exception Error)> RunTaskHandleError(int i, PipeTransportClient<HeartbeatMessage> c)
    {
        var request = new RequestMessage($"Sample request {i}", delay);
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
            return (await c.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, cts.Token), null);
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
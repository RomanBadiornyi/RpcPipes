﻿using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RpcPipes.Models;
using RpcPipes;

const string receivePipe = "TestPipe";
const string sendPipe = "Client.TestPipe";
const string progressPipe = "Progress.TestPipe";
const int connections = 32;

var cancellationTokenSource = new CancellationTokenSource();

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
var logger = serviceProvider.GetRequiredService<ILogger<PipeServer<ProgressMessage>>>();

var serializer = new PipeSerializer();
var messageHandler = new PipeMessageHandler();

var pipeServer = new PipeServer<ProgressMessage>(logger, sendPipe, receivePipe, progressPipe, connections, serializer);
Console.CancelKeyPress += delegate (object _, ConsoleCancelEventArgs e) {
    e.Cancel = true;
    cancellationTokenSource.Cancel();
};

logger.LogInformation("Starting Server, press Ctrl+C to stop");
var serverTask = Task.Run(() => pipeServer.Start(messageHandler, messageHandler, cancellationTokenSource.Token));

try
{
    await serverTask;
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
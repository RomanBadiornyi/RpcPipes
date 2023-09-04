using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RpcPipes.Models;
using RpcPipes.Models.PipeSerializers;
using RpcPipes.Models.PipeMessageHandlers;
using RpcPipes.Models.PipeHeartbeat;
using RpcPipes.PipeServer;

const string receivePipe = "TestPipe";
const string heartbeatPipe = "Heartbeat.TestPipe";
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
var logger = serviceProvider.GetRequiredService<ILogger<PipeServer<HeartbeatMessage>>>();

var serializer = new PipeSerializer();
var messageHandler = new PipeMessageHandler();
var heartbeatHandler = new PipeHeartbeatMessageHandler();

var pipeServer = new PipeServer<HeartbeatMessage>(logger, receivePipe, heartbeatPipe, connections, heartbeatHandler, serializer);
Console.CancelKeyPress += delegate (object _, ConsoleCancelEventArgs e) {
    e.Cancel = true;
    cancellationTokenSource.Cancel();
};

logger.LogInformation("Starting Server, press Ctrl+C to stop");
var serverTask = Task.Run(() => pipeServer.Start(messageHandler, cancellationTokenSource.Token));

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
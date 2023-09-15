using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Logger;
using RpcPipes.Models.PipeMessageHandlers;
using RpcPipes.Models.PipeHeartbeat;
using RpcPipes.Models.PipeSerializers;
using RpcPipes.Models;

namespace RpcPipes.Tests.PipeClientServer;

public class BasePipeClientServerTests
{
    private ServiceProvider _serviceProvider;

    protected TimeSpan ClientRequestTimeout = TimeSpan.FromDays(1);
    protected TimeSpan ServerTimeout = TimeSpan.FromDays(1);
        
    protected Dictionary<string, int> Messages;
    protected Dictionary<string, int> Connections;
    protected MeterListener MeterListener;

    protected CancellationTokenSource ServerStop;
    protected Task ServerTask;

    protected ILogger TestsLogger;
    protected ILogger<PipeTransportServer> ServerLogger;    
    protected ILogger<PipeTransportClient<PipeHeartbeatMessage>> ClientLogger;

    protected PipeSerializer Serializer;
    protected PipeMessageHandler MessageHandler;
    protected PipeHeartbeatMessageHandler HeartbeatHandler;
    protected ConcurrentBag<PipeHeartbeatMessage> HeartbeatReplies;
    protected PipeHeartbeatReceiver HeartbeatMessageReceiver;    

    [OneTimeSetUp]
    public void SetupTimeouts()
    {
        //if we are not debugging tests - ensure that they don't hang due to deadlocks and enforce client/server to be cancelled after timeout 
        if (!Debugger.IsAttached)
        {
            ClientRequestTimeout = TimeSpan.FromSeconds(20);
            ServerTimeout = TimeSpan.FromSeconds(20);
        }
    }

    [OneTimeSetUp]
    public void ListenMetrics()
    {
        StartMetrics();
        MeterListener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (Messages.ContainsKey($"{instrument.Name}"))
                    listener.EnableMeasurementEvents(instrument);
                if (Connections.ContainsKey($"{instrument.Meter.Name}.{instrument.Name}"))
                    listener.EnableMeasurementEvents(instrument);
            }
        };
        MeterListener.SetMeasurementEventCallback<int>(OnMeasurementRecorded);
        MeterListener.Start();        

        void OnMeasurementRecorded(Instrument instrument, int measurement, ReadOnlySpan<KeyValuePair<string, object>> tags, object state)
        {
            lock(Messages) 
            {
                var id = $"{instrument.Name}";
                if (Messages.TryGetValue(id, out var current))
                {
                    var next = current + measurement;                    
                    Messages[id] = next;
                    TestsLogger?.LogTrace("metrics {MetricsName} change {From} to {To}", id, current, next);
                }                
            }
            lock(Connections) 
            {
                var id = $"{instrument.Meter.Name}.{instrument.Name}";
                if (Connections.TryGetValue(id, out var current))
                {
                    var next = current + measurement;                    
                    Connections[id] = next;
                    TestsLogger?.LogTrace("metrics {MetricsName} change {From} to {To}", id, current, next);
                }
            }            
        }
    }    

    [SetUp]
    public void SetupLogging()
    {
        _serviceProvider = new ServiceCollection()
            .AddLogging(loggingBuilder => 
            {
                loggingBuilder.SetMinimumLevel(LogLevel.Trace);
                loggingBuilder.AddNUnitLogger();
            }).BuildServiceProvider();
        ClientLogger = _serviceProvider.GetRequiredService<ILogger<PipeTransportClient<PipeHeartbeatMessage>>>();
        ServerLogger = _serviceProvider.GetRequiredService<ILogger<PipeTransportServer>>();
        TestsLogger = _serviceProvider.GetRequiredService<ILogger<BasePipeClientServerTests>>();
        TestsLogger.LogInformation($"=== begin {TestContext.CurrentContext.Test.Name} ===");        
    }

    [TearDown]
    public void CleanupLogging()
    {                        
        _serviceProvider.Dispose();
        TestsLogger.LogInformation($"=== end   {TestContext.CurrentContext.Test.Name} ===");
    }

    [SetUp]
    public void SetupDependencies()
    {
        Serializer = new PipeSerializer();

        MessageHandler = new PipeMessageHandler();
        HeartbeatHandler = new PipeHeartbeatMessageHandler();

        HeartbeatReplies = new ConcurrentBag<PipeHeartbeatMessage>();
        HeartbeatMessageReceiver = new PipeHeartbeatReceiver(HeartbeatReplies);        
    }

    [TearDown]
    public void CleanupDependencies()
    {
        HeartbeatReplies.Clear();
    }         

    [SetUp]
    public void StartMetrics()
    {
        Messages = new Dictionary<string, int>
        {
            { "sent-messages", 0 },
            { "received-messages", 0 },

            { "pending-messages", 0 },
            { "active-messages", 0 },
            { "reply-messages", 0 },                        
            { "handled-messages", 0 }
        };
        Connections = new Dictionary<string, int>
        {
            { "PipeTransportClient.server-connections", 0 },
            { "PipeTransportClient.client-connections", 0 },
            { "PipeTransportServer.server-connections", 0 },
            { "PipeTransportServer.client-connections", 0 }
        };
    }

    [TearDown]
    public void CleanupMetrics()
    {        
        Messages.Clear();
        Connections.Clear();
    }

    [SetUp]
    public void SetupServer()   
    {        
        ServerStop = new CancellationTokenSource();
        ServerStop.CancelAfter(ServerTimeout);
        ServerTask = Task.CompletedTask;
    }

    [TearDown]
    public void CleanupServer()   
    {
        if (!ServerStop.IsCancellationRequested)
            ServerStop.Cancel();        
        ServerTask.Wait();
        ServerStop.Dispose();
    }
}
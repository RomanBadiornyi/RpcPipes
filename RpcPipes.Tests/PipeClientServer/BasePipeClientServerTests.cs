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
    protected TimeSpan _clientRequestTimeout = TimeSpan.FromDays(1);
    protected TimeSpan _serverTimeout = TimeSpan.FromDays(1);
    
    private ServiceProvider _serviceProvider;

    protected Dictionary<string, int> _messages;
    protected Dictionary<string, int> _connections;
    protected MeterListener _meterListener;

    protected CancellationTokenSource _serverStop;
    protected Task _serverTask;

    protected ILogger _testsLogger;
    protected ILogger<PipeTransportServer> _serverLogger;    
    protected ILogger<PipeTransportClient<HeartbeatMessage>> _clientLogger;

    protected PipeSerializer _serializer;
    protected PipeMessageHandler _messageHandler;
    protected PipeHeartbeatMessageHandler _heartbeatHandler;
    protected ConcurrentBag<HeartbeatMessage> _heartbeatReplies;
    protected PipeHeartbeatReceiver _heartbeatMessageReceiver;    

    [OneTimeSetUp]
    public void SetupTimeouts()
    {
        //if we are not debugging tests - ensure that they don't hang due to deadlocks and enforce client/server to be cancelled after timeout 
        if (!Debugger.IsAttached)
        {
            _clientRequestTimeout = TimeSpan.FromSeconds(20);
            _serverTimeout = TimeSpan.FromSeconds(20);
        }
    }

    [OneTimeSetUp]
    public void ListenMetrics()
    {
        StartMetrics();
        _meterListener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (_messages.ContainsKey($"{instrument.Name}"))
                    listener.EnableMeasurementEvents(instrument);
                if (_connections.ContainsKey($"{instrument.Meter.Name}.{instrument.Name}"))
                    listener.EnableMeasurementEvents(instrument);
            }
        };
        _meterListener.SetMeasurementEventCallback<int>(OnMeasurementRecorded);
        _meterListener.Start();        

        void OnMeasurementRecorded(Instrument instrument, int measurement, ReadOnlySpan<KeyValuePair<string, object>> tags, object state)
        {
            lock(_messages) 
            {
                var id = $"{instrument.Name}";
                if (_messages.TryGetValue(id, out var current))
                {
                    var next = current + measurement;                    
                    _messages[id] = next;
                    _testsLogger?.LogTrace("metrics {MetricsName} change {From} to {To}", id, current, next);
                }                
            }
            lock(_connections) 
            {
                var id = $"{instrument.Meter.Name}.{instrument.Name}";
                if (_connections.TryGetValue(id, out var current))
                {
                    var next = current + measurement;                    
                    _connections[id] = next;
                    _testsLogger?.LogTrace("metrics {MetricsName} change {From} to {To}", id, current, next);
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
        _clientLogger = _serviceProvider.GetRequiredService<ILogger<PipeTransportClient<HeartbeatMessage>>>();
        _serverLogger = _serviceProvider.GetRequiredService<ILogger<PipeTransportServer>>();
        _testsLogger = _serviceProvider.GetRequiredService<ILogger<BasePipeClientServerTests>>();
        _testsLogger.LogInformation($"=== begin {TestContext.CurrentContext.Test.Name} ===");        
    }

    [TearDown]
    public void CleanupLogging()
    {                        
        _serviceProvider.Dispose();
        _testsLogger.LogInformation($"=== end   {TestContext.CurrentContext.Test.Name} ===");
    }

    [SetUp]
    public void SetupDependencies()
    {
        _serializer = new PipeSerializer();

        _messageHandler = new PipeMessageHandler();
        _heartbeatHandler = new PipeHeartbeatMessageHandler();

        _heartbeatReplies = new ConcurrentBag<HeartbeatMessage>();
        _heartbeatMessageReceiver = new PipeHeartbeatReceiver(_heartbeatReplies);        
    }

    [TearDown]
    public void CleanupDependencies()
    {
        _heartbeatReplies.Clear();
    }         

    [SetUp]
    public void StartMetrics()
    {
        _messages = new Dictionary<string, int>
        {
            { "sent-messages", 0 },
            { "received-messages", 0 },

            { "pending-messages", 0 },
            { "active-messages", 0 },
            { "reply-messages", 0 },                        
            { "handled-messages", 0 }
        };
        _connections = new Dictionary<string, int>
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
        _messages.Clear();
        _connections.Clear();
    }

    [SetUp]
    public void SetupServer()   
    {        
        _serverStop = new CancellationTokenSource();
        _serverStop.CancelAfter(_serverTimeout);
        _serverTask = Task.CompletedTask;
    }

    [TearDown]
    public void CleanupServer()   
    {
        if (!_serverStop.IsCancellationRequested)
            _serverStop.Cancel();        
        _serverTask.Wait();
        _serverStop.Dispose();
    }
}
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using NSubstitute;
using RpcPipes.Models;
using RpcPipes.Models.PipeMessageHandlers;
using RpcPipes.Models.PipeHeartbeat;
using RpcPipes.Models.PipeSerializers;
using System.Diagnostics.Metrics;

namespace RpcPipes.Tests.PipeClientServer;

public class BasePipeClientServerTests
{
    protected Dictionary<string, int> _messages;
    protected Dictionary<string, int> _connections;
    protected MeterListener _meterListener;

    protected CancellationTokenSource _serverStop;
    protected Task _serverTask;

    protected ILogger<PipeTransportServer> _serverLogger;
    protected ILogger<PipeTransportClient<HeartbeatMessage>> _clientLogger;
    protected PipeSerializer _serializer;
    protected PipeMessageHandler _messageHandler;
    protected PipeHeartbeatMessageHandler _heartbeatHandler;
    protected ConcurrentBag<HeartbeatMessage> _heartbeatReplies;
    protected PipeHeartbeatReceiver _heartbeatMessageReceiver;

    [SetUp]
    public void Setup()
    {
        _serverLogger = Substitute.For<ILogger<PipeTransportServer>>();
        _clientLogger = Substitute.For<ILogger<PipeTransportClient<HeartbeatMessage>>>();

        _serializer = new PipeSerializer();

        _messageHandler = new PipeMessageHandler();
        _heartbeatHandler = new PipeHeartbeatMessageHandler();

        _heartbeatReplies = new ConcurrentBag<HeartbeatMessage>();
        _heartbeatMessageReceiver = new PipeHeartbeatReceiver(_heartbeatReplies);
    }

    [TearDown]
    public void TearDown()
    {
        _heartbeatReplies.Clear();
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
                if (_messages.ContainsKey($"{instrument.Name}"))
                    _messages[$"{instrument.Name}"] += measurement;
            }
            lock(_connections) 
            {
                if (_connections.ContainsKey($"{instrument.Meter.Name}.{instrument.Name}"))
                    _connections[$"{instrument.Meter.Name}.{instrument.Name}"] += measurement;
            }            
        }
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
    public void StopMetrics()
    {        
        _messages.Clear();
        _connections.Clear();
    }

    [SetUp]
    public void SetupServer()   
    {
        _serverStop = new CancellationTokenSource();
        _serverTask = Task.CompletedTask;
    }

    [TearDown]
    public void StopServer()   
    {
        if (!_serverStop.IsCancellationRequested)
            _serverStop.Cancel();
        _serverTask.Wait();
    }
}
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using NSubstitute;
using RpcPipes.Models;
using RpcPipes.Models.PipeMessageHandlers;
using RpcPipes.Models.PipeHeartbeat;
using RpcPipes.Models.PipeSerializers;
using RpcPipes.PipeClient;
using RpcPipes.PipeServer;

namespace RpcPipes.Tests.PipeClientServer;

public class BasePipeClientServerTests
{
    protected ILogger<PipeServer<HeartbeatMessage>> _serverLogger;
    protected ILogger<PipeClient<HeartbeatMessage>> _clientLogger;
    protected PipeSerializer _serializer;
    protected PipeMessageHandler _messageHandler;
    protected PipeHeartbeatMessageHandler _heartbeatHandler;
    protected ConcurrentBag<HeartbeatMessage> _heartbeatReplies;
    protected PipeHeartbeatReceiver _heartbeatMessageReceiver;

    [SetUp]
    public void Setup()
    {
        _serverLogger = Substitute.For<ILogger<PipeServer<HeartbeatMessage>>>();
        _clientLogger = Substitute.For<ILogger<PipeClient<HeartbeatMessage>>>();

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
}
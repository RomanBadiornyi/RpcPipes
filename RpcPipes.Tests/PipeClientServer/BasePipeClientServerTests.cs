using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using NSubstitute;
using RpcPipes.Models;
using RpcPipes.Models.PipeMessageHandlers;
using RpcPipes.Models.PipeProgress;
using RpcPipes.Models.PipeSerializers;
using RpcPipes.PipeClient;
using RpcPipes.PipeServer;

namespace RpcPipes.Tests.PipeClientServer;

public class BasePipeClientServerTests
{
    protected ILogger<PipeServer<ProgressMessage>> _serverLogger;
    protected ILogger<PipeClient<ProgressMessage>> _clientLogger;
    protected PipeSerializer _serializer;
    protected PipeMessageHandler _messageHandler;
    protected PipeProgressMessageHandler _progressHandler;
    protected ConcurrentBag<ProgressMessage> _progressReplies;
    protected PipeProgressReceiver _progressMessageReceiver;

    [SetUp]
    public void Setup()
    {
        _serverLogger = Substitute.For<ILogger<PipeServer<ProgressMessage>>>();        
        _clientLogger = Substitute.For<ILogger<PipeClient<ProgressMessage>>>();        

        _serializer = new PipeSerializer();
        
        _messageHandler = new PipeMessageHandler();
        _progressHandler = new PipeProgressMessageHandler();
        
        _progressReplies = new ConcurrentBag<ProgressMessage>();
        _progressMessageReceiver = new PipeProgressReceiver(_progressReplies);
    }

    [TearDown]
    public void TearDown()
    {
        _progressReplies.Clear();
    }
}
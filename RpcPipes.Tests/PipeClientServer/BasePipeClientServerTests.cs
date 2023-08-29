using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using NSubstitute;
using RpcPipes.Models;

namespace RpcPipes.PipeClientServer.Tests;

[TestFixture]
public class BasePipeClientServerTests
{
    protected ILogger<PipeServer<ProgressMessage>> _serverLogger;
    protected ILogger<PipeClient<ProgressMessage>> _clientLogger;
    protected PipeSerializer _serializer;
    protected PipeMessageHandler _messageHandler;
    protected ConcurrentBag<ProgressMessage> _progressReplies;
    protected PipeProgressReceiver _progressMessageReceiver;

    [SetUp]
    public void Setup()
    {
        _serverLogger = Substitute.For<ILogger<PipeServer<ProgressMessage>>>();        
        _clientLogger = Substitute.For<ILogger<PipeClient<ProgressMessage>>>();        

        _serializer = new PipeSerializer();
        
        _messageHandler = new PipeMessageHandler();
        
        _progressReplies = new ConcurrentBag<ProgressMessage>();
        _progressMessageReceiver = new PipeProgressReceiver(_progressReplies);
    }

    [TearDown]
    public void TearDown()
    {
        _progressReplies.Clear();
    }
}
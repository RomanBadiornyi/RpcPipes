using System.Collections.Concurrent;
using System.IO.Pipes;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeConnections;
using RpcPipes.PipeExceptions;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes;

public class PipeMessageDispatcher
{
    private readonly ILogger _logger;

    private CancellationToken _cancellation;
    private PipeConnectionPool _connectionPool;

    public int Instances { get; }
    public int HeaderBufferSize { get; }
    public int BufferSize { get; }

    public PipeMessageDispatcher(
        ILogger logger, PipeConnectionPool connectionPool, int instances, int headerBufferSize, int bufferSize, CancellationToken cancellation)
    {
        _logger = logger;
        _connectionPool = connectionPool;
        _cancellation = cancellation;

        Instances = instances;
        HeaderBufferSize = headerBufferSize;
        BufferSize = bufferSize;
    }

    public Task ProcessServerMessages(string pipeName, Func<PipeProtocol, CancellationToken, Task> action)
    {
        var pipeTasks = Enumerable
            .Range(0, Instances)
            .Select(_ => RunServerMessageLoop(pipeName, action))
            .ToArray();
        return Task.WhenAll(pipeTasks);
    }

    public Task ProcessClientMessages<T>(Channel<T> messagesQueue, Func<T, string> pipeTarget, Func<T, PipeProtocol, CancellationToken, Task> action)
        where T : IPipeMessage
    {
        var pipeTasks = Enumerable
            .Range(0, Instances)
            .Select(_ => RunClientMessageLoop(messagesQueue, pipeTarget, action))
            .ToArray();
        return Task.WhenAll(pipeTasks);
    }

    private async Task RunServerMessageLoop(string pipeName, Func<PipeProtocol, CancellationToken, Task> action)
    {
        while (!_cancellation.IsCancellationRequested)
        {
            var (connected, used, error) = await _connectionPool.UseServerConnection(pipeName, DispatchMessage);
            if (error != null && !IsConnectionCancelled(error) && !IsConnectionInterrupted(error, connected))
                _logger.LogError(error, "error occurred while waiting for message on pipe stream {PipeName}, message {Dispatched}", pipeName, used);
        }

        async Task DispatchMessage(NamedPipeServerStream stream)
        {
            var protocol = new PipeProtocol(stream, HeaderBufferSize, BufferSize);
            await action.Invoke(protocol, _cancellation);
        }
    }

    private async Task RunClientMessageLoop<T>(Channel<T> messagesQueue, Func<T, string> pipeTarget, Func<T, PipeProtocol, CancellationToken, Task> action)
        where T: IPipeMessage
    {
        while (!_cancellation.IsCancellationRequested)
        {
            //only once we picked item from messagesQueue - we know to what pipe we should send it to
            //however in order to send it we need to prepare connection (create it's instance and connect to server), and that will take time
            //and even can result error if server run out of free connections then we can't connect to it and will fail on timeout etc.

            //so to prevent message from being stalled in case if we see connection not yet established (see ValidateConnection callback)
            //we push message back into messagesQueue allowing other dispatchers to read same message and then we will bypass dispatching of this item
            //in this case at some point message will be picked by connection which is ready to serve this message and will eventually dispatch it
            var item = await messagesQueue.Reader.ReadAsync(_cancellation);
            var itemReadyToDispatch = true;
            var pipeName = pipeTarget.Invoke(item);

            var (connected, used, error) = await _connectionPool.UseClientConnection(pipeName, DispatchMessage, ValidateConnection);

            if (error != null && !IsConnectionCancelled(error) && !IsConnectionInterrupted(error, connected))
                _logger.LogError(error, "error occurred while processing message {MessageId} on pipe stream {PipeName}, message {Dispatched}", item.Id, pipeName, used);

            void ValidateConnection(IPipeConnection connection)
            {
                //indicate it's not ready to be dispatched so dispatch function will simply do nothing and
                //other dispatcher who is connected - will dispatch the message
                if (!connection.Connected && messagesQueue.Writer.TryWrite(item))
                    itemReadyToDispatch = false;
            }

            async Task DispatchMessage(NamedPipeClientStream stream)
            {
                if (itemReadyToDispatch)
                {
                    var protocol = new PipeProtocol(stream, HeaderBufferSize, BufferSize);
                    await action.Invoke(item, protocol, _cancellation);
                }
            }
        }
    }

    private bool IsConnectionCancelled(Exception e) => e is OperationCanceledException;
    private bool IsConnectionInterrupted(Exception e, bool connected) => e is PipeNetworkException && !connected;
}
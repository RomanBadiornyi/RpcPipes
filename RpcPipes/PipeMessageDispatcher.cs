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
            var (connected, dispatched, error) = await _connectionPool.UseServerConnection(pipeName, ShouldDispatchMessage, DispatchMessage);
            if (error != null && !IsConnectionCancelled(error, dispatched) && !IsConnectionInterrupted(error, connected))
                _logger.LogError(error, "error occurred while waiting for message on pipe stream {PipeName}, dispatched '{Dispatched}'", pipeName, dispatched);
        }

        bool ShouldDispatchMessage(IPipeConnection connection)
        {
            return true;
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
            var pipeName = pipeTarget.Invoke(item);

            var (connected, dispatched, error) = await _connectionPool.UseClientConnection(pipeName, ShouldDispatchMessage, DispatchMessage);
            //if message was not dispatched - return it back to channel for retry
            if (!dispatched)
                messagesQueue.Writer.TryWrite(item);
            //if some unexpected error - log, otherwise Cancelled or Network error while connection disconnected - considered normal cases 
            if (error != null && !IsConnectionCancelled(error, dispatched) && !IsConnectionInterrupted(error, connected))
                _logger.LogError(error, "error occurred while processing message {MessageId} on pipe stream {PipeName}, dispatched '{Dispatched}'", item.Id, pipeName, dispatched);

            bool ShouldDispatchMessage(IPipeConnection connection)
            {
                //indicate it's not ready to be dispatched so dispatch function will simply do nothing and
                //other dispatcher who is connected - will dispatch the message
                if (!connection.VerifyIfConnected() && messagesQueue.Writer.TryWrite(item))
                    return false;
                return true;
            }

            async Task DispatchMessage(NamedPipeClientStream stream)
            {
                var protocol = new PipeProtocol(stream, HeaderBufferSize, BufferSize);
                    await action.Invoke(item, protocol, _cancellation);
            }
        }
    }

    private bool IsConnectionCancelled(Exception e, bool dispatched) => 
        (e is OperationCanceledException || e is TimeoutException) && !dispatched;
    private bool IsConnectionInterrupted(Exception e, bool connected) => 
        (e is PipeNetworkException || e is OperationCanceledException || e is TimeoutException) && !connected;
}
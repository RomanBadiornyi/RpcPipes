using System.IO.Pipes;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeConnections;
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

    public Task ProcessClientMessages<T>(Channel<T> messages, Func<T, string> pipeTarget, Func<T, PipeProtocol, CancellationToken, Task> action)
        where T : IPipeMessage
    {
        var pipeTasks = Enumerable
            .Range(0, Instances)
            .Select(_ => RunClientMessageLoop(messages, pipeTarget, action))
            .ToArray();
        return Task.WhenAll(pipeTasks);
    }

    private async Task RunServerMessageLoop(string pipeName, Func<PipeProtocol, CancellationToken, Task> action)
    {
        while (!_cancellation.IsCancellationRequested)
        {
            var (_, error) = await _connectionPool.UseServerConnection(pipeName, DispatchMessage);
            if (error != null && error is not OperationCanceledException)
                _logger.LogError(error, "error occurred while waiting for message on pipe stream {PipeName}", pipeName);
        }

        async Task DispatchMessage(NamedPipeServerStream stream)
        {
            var protocol = new PipeProtocol(stream, HeaderBufferSize, BufferSize);
            await action.Invoke(protocol, _cancellation);
        }
    }

    private async Task RunClientMessageLoop<T>(Channel<T> messages, Func<T, string> pipeTarget, Func<T, PipeProtocol, CancellationToken, Task> action)
        where T: IPipeMessage
    {
        while (!_cancellation.IsCancellationRequested)
        {
            var item = await messages.Reader.ReadAsync(_cancellation);
            var pipeName = pipeTarget.Invoke(item);

            var (used, error) = await _connectionPool.UseClientConnection(pipeName, DispatchMessage);
            if (!used)
                await messages.Writer.WriteAsync(item, _cancellation);
            if (error != null && error is not OperationCanceledException)
                _logger.LogError(error, "error occurred while processing message {MessageId} on pipe stream {PipeName}", item.Id, pipeName);

            async Task DispatchMessage(NamedPipeClientStream stream)
            {                
                var protocol = new PipeProtocol(stream, HeaderBufferSize, BufferSize);
                await action.Invoke(item, protocol, _cancellation);
            }
        }
    }
}
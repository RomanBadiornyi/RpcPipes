using System.IO.Pipes;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeConnections;
using RpcPipes.PipeHandlers;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes;

public class PipeMessageDispatcher
{
    private readonly ILogger _logger;
    private readonly CancellationToken _cancellation;
    private readonly PipeConnectionPool _connectionPool;

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

    public Task ProcessServerMessages(IPipeMessageReceiver messageReceiver)
    {
        var pipeTasks = Enumerable
            .Range(0, Instances)
            .Select(_ => RunServerMessageLoop(messageReceiver))
            .ToArray();
        return Task.WhenAll(pipeTasks);
    }

    public Task ProcessClientMessages<T>(Channel<T> messagesQueue, IPipeMessageSender<T> messageSender)
        where T : IPipeMessage
    {
        var pipeTasks = Enumerable
            .Range(0, Instances)
            .Select(_ => RunClientMessageLoop(messagesQueue, messageSender))
            .ToArray();
        return Task.WhenAll(pipeTasks);
    }

    private async Task RunServerMessageLoop(IPipeMessageReceiver messageReceiver)
    {
        while (!_cancellation.IsCancellationRequested)
        {
            var (connected, dispatched, error) = await _connectionPool.Server.UseConnection(messageReceiver.Pipe, ShouldDispatchMessage, DispatchMessage);
            if (error != null)
                LogError(error, connected, dispatched);
        }

        bool ShouldDispatchMessage(IPipeConnection connection)
        {
            return true;
        }

        async Task<bool> DispatchMessage(NamedPipeServerStream stream)
        {
            var protocol = new PipeProtocol(stream, HeaderBufferSize, BufferSize);
            return await messageReceiver.ReceiveMessage(protocol, _cancellation);
        }

        void LogError(Exception error, bool connected, bool dispatched)     
        {
            if (error is not OperationCanceledException && error is not TimeoutException)
            {
                _logger.LogError(error, 
                    "error occurred while waiting for message on pipe stream {PipeName}, connected '{Connected}', dispatched '{Dispatched}'", 
                    messageReceiver.Pipe, connected, dispatched);
            }
            if (error is OperationCanceledException)
            {
                _logger.LogDebug("waiting for message on pipe stream {PipeName} has been cancelled", messageReceiver.Pipe);                
            }
            if (error is TimeoutException)
            {
                _logger.LogDebug("waiting for message on pipe stream {PipeName} has been timed out", messageReceiver.Pipe);                
            }
        }
    }

    private async Task RunClientMessageLoop<T>(Channel<T> messagesQueue, IPipeMessageSender<T> messageSender)
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
            T item;
            string pipeName;
            try
            {
                item = await messagesQueue.Reader.ReadAsync(_cancellation);
                pipeName = messageSender.TargetPipe(item);
            }
            catch (OperationCanceledException)
            {                
                break;
            }

            var (connected, dispatched, error) = await _connectionPool.Client.UseConnection(pipeName, ShouldDispatchMessage, DispatchMessage);            
            
            if (!dispatched && error != null && error is not OperationCanceledException)
            {           
                //if message was not dispatched - report error to sender and let it handle that                 
                await messageSender.HandleError(item, error, _cancellation);
            }
            else if (error != null)
            {
                LogError(error, connected, dispatched);
            }            

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
                await messageSender.HandleMessage(item, protocol, _cancellation);
            }

            void LogError(Exception error, bool connected, bool dispatched)     
            {
                if (error is not OperationCanceledException && error is not TimeoutException)
                {
                    _logger.LogError(error, 
                        "error occurred while processing message {MessageId} on pipe stream {PipeName}, connected '{Connected}', dispatched '{Dispatched}'", 
                        item.Id, pipeName, connected, dispatched);                
                }
                if (error is OperationCanceledException)
                {
                    _logger.LogDebug("processing of message {MessageId} on pipe stream {PipeName} has been cancelled", item.Id, pipeName);                
                }
                if (error is TimeoutException)
                {
                    _logger.LogDebug("processing of message {MessageId} on pipe stream {PipeName} has been timed out", item.Id, pipeName);                
                }
            }
        }        
    }    
}
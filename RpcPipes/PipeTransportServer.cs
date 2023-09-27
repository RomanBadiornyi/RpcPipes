using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeConnections;
using RpcPipes.PipeData;
using RpcPipes.PipeHandlers;
using RpcPipes.PipeHeartbeat;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes;

public class PipeTransportServer    
{
    private class PipeRequestResponse<TReq, TRep>
    {
        public PipeServerRequestMessage Handle { get; set; }
        public PipeMessageRequest<TReq> RequestMessage { get; set; }
        public PipeMessageResponse<TRep> ResponseMessage { get; set; }
    }
    
    private static readonly Meter Meter = new(nameof(PipeTransportServer));

    private readonly ILogger<PipeTransportServer> _logger;
    private readonly string _receivePipe;
    private readonly string _heartbeatPipe;
    private readonly int _instances;
    private readonly IPipeMessageWriter _messageWriter;

    private readonly object _sync = new();
    private bool _started;
    private Task _connectionsTask;
    private CancellationTokenSource _connectionsCancellation;

    internal PipeReplyOutHandler ReplyOut { get; private set; }
    internal PipeRequestInHandler RequestIn { get; private set; }
    internal PipeHeartbeatInHandler HeartbeatIn { get; private set; }

    public PipeConnectionPool ConnectionPool { get; private set; }
    public PipeMessageDispatcher MessageDispatcher { get; private set; }

    public PipeTransportServer(
        ILogger<PipeTransportServer> logger, string pipePrefix, int instances, IPipeMessageWriter messageWriter)
    {        
        _logger = logger;

        //limitation on unix
        const int maxPipeLength = 108;
        _receivePipe = $"{pipePrefix}";
        if (_receivePipe.Length > maxPipeLength)
            throw new ArgumentOutOfRangeException($"send pipe {_receivePipe} too long, limit is {maxPipeLength}");                
        _heartbeatPipe = $"{pipePrefix}.heartbeat";
        if (_heartbeatPipe.Length > maxPipeLength)
            throw new ArgumentOutOfRangeException($"send pipe {_heartbeatPipe} too long, limit is {maxPipeLength}");                        
        _instances = instances;
        _messageWriter = messageWriter;
    }

    public Task Start<TReq, TRep, TP>(
        IPipeMessageHandler<TReq, TRep> messageHandler,
        IPipeHeartbeatHandler<TP> heartbeatHandler,
        CancellationToken token)
        where TP: IPipeHeartbeat
    {
        lock (_sync)
        {
            if (_started)
                return _connectionsTask;                

            _connectionsCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);

            var headerBuffer = 1 * 1024;
            var contentBuffer = 4 * 1024;
            ConnectionPool = new PipeConnectionPool(_logger, Meter, _instances, contentBuffer, _connectionsCancellation.Token);
            MessageDispatcher = new PipeMessageDispatcher(_logger, ConnectionPool, _instances, headerBuffer, contentBuffer, _connectionsCancellation.Token);

            var heartbeatReporter = messageHandler as IPipeHeartbeatReporter;
            HeartbeatIn = new PipeHeartbeatInHandler<TP>(_logger, _heartbeatPipe, MessageDispatcher, heartbeatHandler, _messageWriter);

            RequestIn = new PipeRequestInHandler(_logger, _receivePipe, MessageDispatcher, MessageStarted, MessageCancellation);
            ReplyOut = new PipeReplyOutHandler(_logger, MessageDispatcher, MessageCompleted);

            _connectionsTask = Task
                //first complete all server connections
                .WhenAll(RequestIn.ServerTask, HeartbeatIn.ServerTask)                
                //wait also until we complete all client connections
                .ContinueWith(_ => ConnectionPool.Server.Dispose(), CancellationToken.None)
                .ContinueWith(_ => ReplyOut.ClientTask, CancellationToken.None).Unwrap()
                .ContinueWith(_ => ConnectionPool.Client.Dispose(), CancellationToken.None)
                .ContinueWith(_ => { _logger.LogDebug("server has been stopped"); }, CancellationToken.None);

            _started = true;
            return _connectionsTask;

            bool MessageStarted(PipeServerRequestMessage message, CancellationToken cancellation)
            {
                if (heartbeatHandler.StartMessageHandling(message.Id, cancellation, heartbeatReporter))
                {
                    SetupRequest(message, messageHandler, heartbeatHandler);
                    return true;
                }
                return false;
            }

            CancellationTokenSource MessageCancellation(PipeServerRequestMessage message)
            {
                return heartbeatHandler.TryGetMessageState(message.Id, out var messageState) 
                    ? messageState.Cancellation
                    : null;
            }

            bool MessageCompleted(PipeServerRequestMessage message)
            {
                return heartbeatHandler.EndMessageHandling(message.Id);
            }
        }
    }

    private bool SetupRequest<TReq, TRep>(
        PipeServerRequestMessage requestMessage, 
        IPipeMessageHandler<TReq, TRep> messageHandler,
        IPipeHeartbeatHandler heartbeatHandler)
    {
        var requestContainer = new PipeRequestResponse<TReq, TRep> 
        {
            Handle = requestMessage,
            RequestMessage = new PipeMessageRequest<TReq>(), 
            ResponseMessage = new PipeMessageResponse<TRep>() 
        };
        if (heartbeatHandler.TryGetMessageState(requestMessage.Id, out var messageState))
        {
            messageState.RequestState = requestContainer;

            requestMessage.ReadRequest = (protocol, token) => ReadRequest(requestContainer, protocol, token);
            requestMessage.RunRequest = (cancellation) => RunRequest(messageHandler, heartbeatHandler, requestContainer, cancellation);
            requestMessage.SendResponse = (protocol, token) => SendReply(requestContainer, protocol, token); 
            requestMessage.ReportError = (exception) => ReportError(requestContainer, exception); 
            return true;            
        }        
        return false;
    }

    private async Task<bool> ReadRequest<TReq, TRep>(PipeRequestResponse<TReq, TRep> requestContainer, PipeProtocol protocol, CancellationToken cancellation)
    {
        try
        {
            _logger.LogDebug("reading request message {MessageId}", requestContainer.Handle.Id);
            requestContainer.RequestMessage = await protocol.EndReceiveMessage(requestContainer.Handle.Id, Read, cancellation);
            _logger.LogDebug("read request message {MessageId}", requestContainer.Handle.Id);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        catch (Exception e)
        {
            _logger.LogWarning(e, "unable to consume message {MessageId} due to error, reply error back to client", requestContainer.Handle.Id);
            requestContainer.ResponseMessage.SetRequestException(e);
            await ReplyOut.Publish(requestContainer.Handle);
            return false;
        }

        async ValueTask<PipeMessageRequest<TReq>> Read(Stream stream, CancellationToken c)
            => await _messageWriter.ReadRequest<TReq>(stream, c);
        
    }

    private async Task RunRequest<TReq, TRep>(
        IPipeMessageHandler<TReq, TRep> messageHandler, 
        IPipeHeartbeatHandler heartbeatHandler,
        PipeRequestResponse<TReq, TRep> requestContainer, 
        CancellationTokenSource cancellation)
    {
        heartbeatHandler.StartMessageExecute(requestContainer.Handle.Id, requestContainer.RequestMessage.Request);            
        using var requestCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellation.Token);
        if (requestContainer.RequestMessage.Deadline.TotalMilliseconds > 0)
            cancellation.CancelAfter(requestContainer.RequestMessage.Deadline);
        
        try
        {
            _logger.LogDebug("request execution starts for message {MessageId}", requestContainer.Handle.Id);
            requestCancellation.Token.ThrowIfCancellationRequested();    
            requestContainer.ResponseMessage.Reply = await messageHandler.HandleRequest(requestContainer.RequestMessage.Request, requestCancellation.Token);
            requestCancellation.Token.ThrowIfCancellationRequested();    
            _logger.LogDebug("request execution completed for message {MessageId}", requestContainer.Handle.Id);
        }
        catch (OperationCanceledException e)
        {
            _logger.LogInformation("request execution was cancelled for message {MessageId}", requestContainer.Handle.Id);
            requestContainer.ResponseMessage.SetRequestException(e);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "request execution thrown unhandled error for message {MessageId}", requestContainer.Handle.Id);
            requestContainer.ResponseMessage.SetRequestException(e);
        }
        heartbeatHandler.EndMessageExecute(requestContainer.Handle.Id);
        await ReplyOut.Publish(requestContainer.Handle);
    }

    private async Task SendReply<TReq, TRep>(PipeRequestResponse<TReq, TRep> requestContainer, PipeProtocol protocol, CancellationToken cancellation)
    {
        _logger.LogDebug("sending reply for message {MessageId} back to client", requestContainer.Handle.Id);
        var pipeMessageHeader = new PipeMessageHeader { MessageId = requestContainer.Handle.Id };
        await protocol.TryTransferMessage(pipeMessageHeader, Write, cancellation);
        _logger.LogDebug("sent reply for message {MessageId} back to client", requestContainer.Handle.Id);

       async Task Write(Stream stream, CancellationToken c)
            => await _messageWriter.WriteResponse(requestContainer.ResponseMessage, stream, c);        
    }

    private async ValueTask<bool> ReportError<TReq, TRep>(PipeRequestResponse<TReq, TRep> requestContainer, Exception exception)
    {
        if (requestContainer.ResponseMessage.Reply != null && requestContainer.ResponseMessage.ReplyError == null)
        {
            requestContainer.RequestMessage = null;
            requestContainer.ResponseMessage = new PipeMessageResponse<TRep>();
            requestContainer.ResponseMessage.SetRequestException(exception);
            await ReplyOut.Publish(requestContainer.Handle);
            return true;
        }               
        return false; 
    }
}
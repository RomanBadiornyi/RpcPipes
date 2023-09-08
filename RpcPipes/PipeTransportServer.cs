using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.IO.Pipes;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeData;
using RpcPipes.PipeHandlers;
using RpcPipes.PipeHeartbeat;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes;

public class PipeTransportServer    
{
    class RequestResponse<TReq, TRep>
    {
        public PipeServerRequestMessage Handle { get; set; }
        public PipeMessageRequest<TReq> RequestMessage { get; set; }
        public PipeMessageResponse<TRep> ResponseMessage { get; set; }
    }
    
    private static Meter _meter = new(nameof(PipeTransportServer));

    private ILogger<PipeTransportServer> _logger;
    private string _receivePipe;
    private string _heartbeatPipe;
    private int _instances;
    private IPipeMessageWriter _messageWriter;

    private readonly object _sync = new();
    private bool _started;
    private Task _connectionsTask;
    private CancellationTokenSource _connectionsCancellation;

    internal PipeReplyOutHandler ReplyOut { get; private set; }
    internal PipeRequestInHandler RequestIn { get; private set; }
    internal PipeHeartbeatInHandler HeartbeatIn { get; private set; }

    public PipeConnectionManager ConnectionPool { get; private set; }
    public CancellationTokenSource Cancellation => _connectionsCancellation;

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

            var requests = new ConcurrentDictionary<Guid, RequestResponse<TReq, TRep>>();
            ConnectionPool = new PipeConnectionManager(
                _logger, _meter, _instances, 1 * 1024, 4 * 1024, PipeOptions.Asynchronous | PipeOptions.WriteThrough, _connectionsCancellation.Token);        

            HeartbeatIn = new PipeHeartbeatInHandler(
                _logger, _heartbeatPipe, ConnectionPool, _messageWriter);
            RequestIn = new PipeRequestInHandler(
                _logger, _receivePipe, ConnectionPool, heartbeatHandler);
            ReplyOut = new PipeReplyOutHandler(
                _logger, ConnectionPool, heartbeatHandler);

            _connectionsTask = Task
                .WhenAll(
                    RequestIn.Start(messageHandler as IPipeHeartbeatReporter, SetupRequestCallbacks),
                    HeartbeatIn.Start(heartbeatHandler)
                )
                //wait also until we complete all client connections
                .ContinueWith(_ => Task.WhenAll(ReplyOut.ChannelTasks), CancellationToken.None).Unwrap()
                .ContinueWith(_ => { _logger.LogDebug("server has been stopped"); }, CancellationToken.None);

            _started = true;
            return _connectionsTask;

            bool SetupRequestCallbacks(PipeServerRequestMessage request)
                => SetupRequest(request, requests, messageHandler, heartbeatHandler);
        }
    }

    private bool SetupRequest<TReq, TRep>(
        PipeServerRequestMessage requestMessage, 
        ConcurrentDictionary<Guid, RequestResponse<TReq, TRep>> requests, 
        IPipeMessageHandler<TReq, TRep> messageHandler,
        IPipeHeartbeatHandler heartbeatHandler)
    {
        var requestContainer = new RequestResponse<TReq, TRep> 
        {
            Handle = requestMessage,
            RequestMessage = new PipeMessageRequest<TReq>(), 
            ResponseMessage = new PipeMessageResponse<TRep>() 
        };
        if (requests.TryAdd(requestMessage.Id, requestContainer))
        {
            requestMessage.ReadRequest = (protocol, token) => ReadRequest(requestContainer, protocol, token);
            requestMessage.RunRequest = (cancellation) => RunRequest(messageHandler, heartbeatHandler, requestContainer, cancellation);
            requestMessage.SendResponse = (protocol, token) => SendRequest(requestContainer, protocol, token); 
            requestMessage.ReportError = (exception) => ReportError(requestContainer, exception); 
            requestMessage.OnMessageCompleted = (exception, result) => requests.TryRemove(requestMessage.Id, out _);
            return true;            
        }
        return false;
    }

    private async Task<bool> ReadRequest<TReq, TRep>(RequestResponse<TReq, TRep> requestContainer, PipeProtocol protocol, CancellationToken cancellation)
    {
        try
        {
            _logger.LogDebug("reading request message {MessageId}", requestContainer.Handle.Id);
            requestContainer.RequestMessage = await protocol.EndReceiveMessage(requestContainer.Handle.Id, _messageWriter.ReadRequest<TReq>, cancellation);
            _logger.LogDebug("read request message {MessageId}", requestContainer.Handle.Id);
            return true;
        }
        catch (Exception e)
        {
            _logger.LogWarning(e, "unable to consume message {MessageId} due to error, reply error back to client", requestContainer.Handle.Id);
            requestContainer.ResponseMessage.SetRequestException(e);
            ReplyOut.PublishResponseMessage(requestContainer.Handle);
            return false;
        }
    }

    private async Task RunRequest<TReq, TRep>(
        IPipeMessageHandler<TReq, TRep> messageHandler, 
        IPipeHeartbeatHandler heartbeatHandler,
        RequestResponse<TReq, TRep> requestContainer, 
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
            _logger.LogDebug("request execution completed for message {MessageId}", requestContainer.Handle.Id);
        }
        catch (OperationCanceledException e)
        {
            _logger.LogWarning("request execution was cancelled for message {MessageId}", requestContainer.Handle.Id);
            requestContainer.ResponseMessage.SetRequestException(e);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "request execution thrown unhandled error for message {MessageId}", requestContainer.Handle.Id);
            requestContainer.ResponseMessage.SetRequestException(e);
        }
        heartbeatHandler.EndMessageExecute(requestContainer.Handle.Id);
        ReplyOut.PublishResponseMessage(requestContainer.Handle);
    }

    private async Task SendRequest<TReq, TRep>(RequestResponse<TReq, TRep> requestContainer, PipeProtocol protocol, CancellationToken cancellation)
    {
        _logger.LogDebug("sending reply for message {MessageId} back to client", requestContainer.Handle.Id);
        var pipeMessageHeader = new PipeMessageHeader { MessageId = requestContainer.Handle.Id };
        await protocol.TransferMessage(pipeMessageHeader, Write, cancellation);
        _logger.LogDebug("sent reply for message {MessageId} back to client", requestContainer.Handle.Id);

       Task Write(Stream stream, CancellationToken cancellation)
            => _messageWriter.WriteResponse(requestContainer.ResponseMessage, stream, cancellation);        
    }

    private bool ReportError<TReq, TRep>(RequestResponse<TReq, TRep> requestContainer, Exception exception)
    {
        if (requestContainer.ResponseMessage.Reply != null && requestContainer.ResponseMessage.ReplyError == null)
        {
            requestContainer.RequestMessage = null;
            requestContainer.ResponseMessage = new PipeMessageResponse<TRep>();
            requestContainer.ResponseMessage.SetRequestException(exception);
            ReplyOut.PublishResponseMessage(requestContainer.Handle);
            return true;
        }               
        return false; 
    }
}
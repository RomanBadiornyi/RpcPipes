using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeConnections;
using RpcPipes.PipeData;
using RpcPipes.PipeHandlers;
using RpcPipes.PipeHeartbeat;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes;

public class PipeRequestContext
{
    public TimeSpan Heartbeat = TimeSpan.FromSeconds(5);
    public TimeSpan Deadline = TimeSpan.FromHours(1);
}

public abstract class PipeRequestHandler
{
    internal abstract PipeClientRequestMessage GetRequestMessageById(Guid id);
    internal abstract ValueTask RequestMessageSent(PipeClientHeartbeatMessage requestMessage);
}

public abstract class PipeTransportClient : PipeRequestHandler
{
    protected readonly static Meter Meter = new(nameof(PipeTransportClient));

    public abstract PipeConnectionPool ConnectionPool { get; }
    public abstract PipeMessageDispatcher MessageDispatcher { get; }

    public abstract CancellationTokenSource Cancellation { get; }

    public abstract Task<TRep> SendRequest<TReq, TRep>(TReq request, PipeRequestContext context, CancellationToken cancellation);
}

public class PipeTransportClient<TP> : PipeTransportClient, IDisposable, IAsyncDisposable
    where TP : class
{
    private readonly ILogger<PipeTransportClient<TP>> _logger;
    private readonly CancellationTokenSource _requestsCancellation;

    private readonly IPipeMessageWriter _messageWriter;
    private readonly ConcurrentDictionary<Guid, PipeClientRequestMessage> _requestQueue;

    internal PipeHeartbeatOutHandler HeartbeatOut { get; }
    internal PipeRequestOutHandler RequestOut { get; }
    internal PipeReplyInHandler ReplyIn { get; }

    public override PipeConnectionPool ConnectionPool { get; }
    public override PipeMessageDispatcher MessageDispatcher { get; }

    public override CancellationTokenSource Cancellation { get; }

    public TimeSpan DisposeTimeout { get; } = TimeSpan.FromSeconds(30);

    public PipeTransportClient(
        ILogger<PipeTransportClient<TP>> logger,
        string pipePrefix,
        string clientId,
        int instances,
        IPipeHeartbeatReceiver<TP> heartbeatReceiver,
        IPipeMessageWriter messageWriter)
    {
        _logger = logger;
        _messageWriter = messageWriter;

        _requestsCancellation = new CancellationTokenSource();
        _requestQueue = new ConcurrentDictionary<Guid, PipeClientRequestMessage>();

        Cancellation = new CancellationTokenSource();

        var headerBuffer = 1 * 1024;
        var contentBuffer = 4 * 1024;
        ConnectionPool = new PipeConnectionPool(logger, Meter, instances, contentBuffer, Cancellation.Token);
        MessageDispatcher = new PipeMessageDispatcher(logger, ConnectionPool, instances, headerBuffer, contentBuffer, Cancellation.Token);

        //limitation on unix
        const int maxPipeLength = 108;

        var sendPipe = $"{pipePrefix}";
        if (sendPipe.Length > maxPipeLength)
            throw new ArgumentOutOfRangeException($"send pipe {sendPipe} too long, limit is {maxPipeLength}");
        var heartBeatPipe = $"{pipePrefix}.heartbeat";
        if (heartBeatPipe.Length > maxPipeLength)
            throw new ArgumentOutOfRangeException($"send pipe {heartBeatPipe} too long, limit is {maxPipeLength}");
        var receivePipe = $"{pipePrefix}.{clientId}.receive";
        if (receivePipe.Length > maxPipeLength)
            throw new ArgumentOutOfRangeException($"send pipe {receivePipe} too long, limit is {maxPipeLength}");

        HeartbeatOut = new PipeHeartbeatOutHandler<TP>(logger, heartBeatPipe, MessageDispatcher, heartbeatReceiver, _messageWriter);
        RequestOut = new PipeRequestOutHandler(logger, sendPipe, MessageDispatcher, this);
        ReplyIn = new PipeReplyInHandler(logger, receivePipe, MessageDispatcher, this);
    }

    internal override PipeClientRequestMessage GetRequestMessageById(Guid id)
    {
        if (_requestQueue.TryGetValue(id, out var requestMessage))
            return requestMessage;
        return null;
    }

    internal override async ValueTask RequestMessageSent(PipeClientHeartbeatMessage requestMessage)
    {
        requestMessage.HeartbeatStarted = true;
        await HeartbeatOut.Publish(requestMessage);
    }

    public override async Task<TRep> SendRequest<TReq, TRep>(TReq request, PipeRequestContext context, CancellationToken cancellation)
    {
        //fail early if request is canalled
        if (cancellation.IsCancellationRequested)
            throw new TaskCanceledException("Request cancelled due to cancellation of client");
        if (_requestsCancellation.Token.IsCancellationRequested)
            throw new TaskCanceledException("Request cancelled due to dispose of client");            

        var requestTaskSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var heartbeatSync = new SemaphoreSlim(1);

        //if cancellation received from heartbeat - FORCEFULLY cancel request
        using var heartbeatCancellation = new CancellationTokenSource();
        heartbeatCancellation.Token.Register(() =>
            requestTaskSource.SetException(new TaskCanceledException("Request cancelled due to failed heartbeat")));

        //if client gets disposed or user cancels execution - MARK request as canceled and let it complete using heartbeat logic
        using var requestCancellation = CancellationTokenSource.CreateLinkedTokenSource(_requestsCancellation.Token, cancellation);        
        var requestMessage = new PipeClientRequestMessage(Guid.NewGuid())
        {
            RequestTask = requestTaskSource,
            RequestCancellation = requestCancellation.Token,

            HeartbeatCancellation = heartbeatCancellation,
            HeartbeatCheckHandle = heartbeatSync,            
            HeartbeatCheckFrequency = context.Heartbeat
        };

        requestCancellation.Token.Register(async () => {
            //we run heartbeat with timeout but if user cancels currently running task -
            //we force heartbeat to be send to server to notify it about cancellation as soon as possible
            if (requestMessage.HeartbeatStarted)
            {
                requestMessage.HeartbeatForced = true;
                await HeartbeatOut.Publish(requestMessage);
            }            
        });

        var pipeRequest = new PipeMessageRequest<TReq>();
        var pipeResponse = new PipeMessageResponse<TRep>();

        pipeRequest.Request = request;
        pipeRequest.Heartbeat = context.Heartbeat;
        pipeRequest.Deadline = context.Deadline;

        requestMessage.SendAction =
            (c, t) => SendRequest(requestMessage.Id, pipeRequest, c, t);
        requestMessage.ReceiveAction =
            async (p, t) => { pipeResponse = await ReadReply<TRep>(requestMessage.Id, p, t); };

        if (_requestQueue.TryAdd(requestMessage.Id, requestMessage))
        {
            //avoid race condition with Dispose which on cancellation awaits all pending tasks
            //if we managed to add item to requests queue but we see client was stopped,
            //that means that possibly Dispose call doesn't know about this task and
            //to resolve it do not send this message to queue and manually cancel it here
            var requestCancelledError = new TaskCanceledException("Request execution cancelled due to cancellation of client");
            if (requestCancellation.IsCancellationRequested)
                requestMessage.RequestTask.TrySetException(requestCancelledError);
            else
                await RequestOut.Publish(requestMessage);
        }
        else
        {
            throw new InvalidOperationException($"Message with {requestMessage.Id} already scheduled");
        }

        _logger.LogDebug("scheduled request execution for message {MessageId}", requestMessage.Id);
        try
        {
            //async block this task until we notified that request is received
            await requestTaskSource.Task;
            _logger.LogDebug("received reply for message {MessageId} from server", requestMessage.Id);
        }
        catch (OperationCanceledException)
        {
            requestMessage.RequestCompleted = true;
            throw;
        }
        finally
        {
            _requestQueue.TryRemove(requestMessage.Id, out _);
        }

        var responseError = pipeResponse?.ReplyError;
        if (responseError != null)
            throw responseError.ToException();
        return pipeResponse.Reply;
    }

    private async Task SendRequest<TReq>(Guid id, PipeMessageRequest<TReq> request, PipeProtocol protocol, CancellationToken cancellation)
    {
        _logger.LogDebug("sending request message {MessageId} to server", id);
        var header = new PipeAsyncMessageHeader { MessageId = id, ReplyPipe = ReplyIn.Pipe };
        var accept = await protocol.BeginTransferMessage(header, cancellation);
        if (accept)
        {
            await protocol.EndTransferMessage(id, Write, cancellation);        
            _logger.LogDebug("sent request message {MessageId} to server", id);
        }        
        else 
        {
            _logger.LogDebug("server rejected request message {MessageId}", id);
        }

        async Task Write(Stream stream, CancellationToken c)
            => await _messageWriter.WriteRequest(request, stream, c);
    }

    private async Task<PipeMessageResponse<TRep>> ReadReply<TRep>(Guid id, PipeProtocol protocol, CancellationToken cancellation)
    {
        _logger.LogDebug("receiving reply for message {MessageId} from server", id);
        var response = await protocol.EndReceiveMessage(id, Read, cancellation);
        _logger.LogDebug("received reply for message {MessageId} from server", id);
        return response;

        async ValueTask<PipeMessageResponse<TRep>> Read(Stream stream, CancellationToken c)
            => await _messageWriter.ReadResponse<TRep>(stream, c);
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().Wait();
        _logger.LogDebug("client has been disposed");
    }

    public async ValueTask DisposeAsync()
    {
        _logger.LogDebug("disposing client");
        await StopPendingTasks(DisposeTimeout);

        Cancellation.Cancel();
        await RequestOut.ClientTask;
        await HeartbeatOut.ClientTask;
        ConnectionPool.Client.Dispose();

        await ReplyIn.ServerTask;
        ConnectionPool.Server.Dispose();

        _logger.LogDebug("client has been disposed");
    }

    private async Task StopPendingTasks(TimeSpan timeout)
    {
        _requestsCancellation.Cancel();
        var requestTasks = _requestQueue.Values.ToArray();
        try
        {
            await Task.WhenAny(
                Task.WhenAll(requestTasks.Select(t => t.RequestTask.Task)),
                Task.Delay(timeout));
        }
        catch (OperationCanceledException)
        {
            //
        }

        var forcedStop = 0;
        var error = new TaskCanceledException("Request forcefully cancelled due to cancellation of client");
        foreach (var request in requestTasks)
        {
            if (!request.RequestCompleted)
            {
                request.RequestTask.TrySetException(error);
                forcedStop++;
            }
        }
        if (forcedStop > 0)
            _logger.LogWarning("time out while stopping {TimeoutTasks} tasks out of {PendingTasks}", forcedStop, requestTasks.Length);
        else
            _logger.LogDebug("all pending tasks completed");
    }
}
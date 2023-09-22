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
    where TP : IPipeHeartbeat
{
    private readonly ILogger<PipeTransportClient<TP>> _logger;

    private readonly IPipeMessageWriter _messageWriter;

    private readonly ConcurrentDictionary<Guid, PipeClientRequestMessage> _requestQueue = new();

    internal PipeHeartbeatOutHandler HeartbeatOut { get; }
    internal PipeRequestOutHandler RequestOut { get; }
    internal PipeReplyInHandler ReplyIn { get; }

    public override PipeConnectionPool ConnectionPool { get; }
    public override PipeMessageDispatcher MessageDispatcher { get; }

    public override CancellationTokenSource Cancellation { get; }

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

    internal override ValueTask RequestMessageSent(PipeClientHeartbeatMessage requestMessage)
        => HeartbeatOut.Publish(requestMessage);

    public override async Task<TRep> SendRequest<TReq, TRep>(TReq request, PipeRequestContext context, CancellationToken requestCancellation)
    {
        requestCancellation.ThrowIfCancellationRequested();
        
        var requestTaskSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var heartbeatSync = new SemaphoreSlim(1);
        //cancel request if cancellation received from heartbeat
        using var heartbeatCancellation = new CancellationTokenSource();
        heartbeatCancellation.Token.Register(() =>
            requestTaskSource.SetException(new TaskCanceledException("Request cancelled due to failed heartbeat")));

        //cancel request if client got disposed.
        using var clientStopCancellation = CancellationTokenSource.CreateLinkedTokenSource(Cancellation.Token);
        clientStopCancellation.Token.Register(() =>
            requestTaskSource.SetException(new TaskCanceledException("Request cancelled due to cancellation of client")));

        //once all cancellation token callbacks setup - verify if we are not already in cancelled state
        //and if so - simply throw
        if (Cancellation.IsCancellationRequested)
            throw new TaskCanceledException("Request cancelled due to cancellation of client");

        var requestMessage = new PipeClientRequestMessage(Guid.NewGuid())
        {
            RequestTask = requestTaskSource,
            RequestCancellation = requestCancellation,

            HeartbeatCancellation = heartbeatCancellation,
            HeartbeatCheckHandle = heartbeatSync,
            HeartbeatCheckTime = DateTime.Now,
            HeartbeatCheckFrequency = context.Heartbeat
        };

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
            await RequestOut.Publish(requestMessage);
        else
            throw new InvalidOperationException($"Message with {requestMessage.Id} already scheduled");

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
        await protocol.BeginTransferMessageAsync(header, cancellation);
        await protocol.EndTransferMessage(id, Write, cancellation);
        _logger.LogDebug("sent request message {MessageId} to server", id);

        Task Write(Stream stream, CancellationToken c)
            => _messageWriter.WriteRequest(request, stream, c);
    }

    private async Task<PipeMessageResponse<TRep>> ReadReply<TRep>(Guid id, PipeProtocol protocol, CancellationToken cancellation)
    {
        _logger.LogDebug("receiving reply for message {MessageId} from server", id);
        var response = await protocol.EndReceiveMessage(id, Read, cancellation);
        _logger.LogDebug("received reply for message {MessageId} from server", id);
        return response;

        ValueTask<PipeMessageResponse<TRep>> Read(Stream stream, CancellationToken c)
            => _messageWriter.ReadResponse<TRep>(stream, c);

    }

    public void Dispose()
    {
        DisposeAsync().AsTask().Wait();
        _logger.LogDebug("client has been disposed");
    }

    public async ValueTask DisposeAsync()
    {
        Cancellation.Cancel();

        await Task.WhenAll(RequestOut.ClientTask, HeartbeatOut.ClientTask);
        ConnectionPool.Client.Dispose();

        await ReplyIn.ServerTask;
        ConnectionPool.Server.Dispose();

        _logger.LogDebug("client has been disposed");
    }
}
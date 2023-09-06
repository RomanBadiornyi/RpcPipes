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

public class PipeRequestContext
{
    public TimeSpan Heartbeat = TimeSpan.FromSeconds(5);
    public TimeSpan Deadline = TimeSpan.FromHours(1);
}

public abstract class PipeTransportClient
{
    public abstract Task<TRep> SendRequest<TReq, TRep>(TReq request, PipeRequestContext context, CancellationToken token);
}

public class PipeTransportClient<TP> : PipeTransportClient, IDisposable, IAsyncDisposable
    where TP : IPipeHeartbeat
{
    private static Meter _meter = new(nameof(PipeTransportClient));
    private readonly ILogger<PipeTransportClient<TP>> _logger;    

    private readonly IPipeMessageWriter _messageWriter;

    private readonly Task _serverTask;
    private readonly CancellationTokenSource _serverTaskCancellation;

    private readonly ConcurrentDictionary<Guid, PipeClientRequestMessage> _requestQueue = new();

    internal PipeHeartbeatOutHandler<TP> HeartbeatOut { get; }
    internal PipeRequestOutHandler RequestOut { get; }
    internal PipeReplyInHandler ReplyIn { get; }    

    public PipeConnectionManager ConnectionPool { get; }

    public PipeTransportClient(
        ILogger<PipeTransportClient<TP>> logger,
        string sendPipe,
        string heartBeatPipe,
        string receivePipe,
        int instances,
        IPipeHeartbeatReceiver<TP> heartbeatReceiver,
        IPipeMessageWriter messageWriter)
    {
        _logger = logger;
        _messageWriter = messageWriter;

        _serverTaskCancellation = new CancellationTokenSource();

        ConnectionPool = new PipeConnectionManager(logger, _meter, instances, 1 * 1024, 4 * 1024, PipeOptions.Asynchronous | PipeOptions.WriteThrough);

        HeartbeatOut = new PipeHeartbeatOutHandler<TP>(logger, heartBeatPipe, ConnectionPool, heartbeatReceiver, _messageWriter, _serverTaskCancellation);
        RequestOut = new PipeRequestOutHandler(logger, sendPipe, ConnectionPool, _serverTaskCancellation);
        ReplyIn = new PipeReplyInHandler(logger, receivePipe, ConnectionPool, _serverTaskCancellation);

        _serverTask = Task
            .WhenAll(ReplyIn.Start(GetRequestMessageById))
            //wait until we complete all client connections
            .ContinueWith(_ => Task.WhenAll(RequestOut.ChannelTasks), CancellationToken.None)
            .ContinueWith(_ => Task.WhenAll(HeartbeatOut.ChannelTasks), CancellationToken.None);

        PipeClientRequestMessage GetRequestMessageById(Guid id)
        {
            if (_requestQueue.TryGetValue(id, out var requestMessage))
                return requestMessage;
            return null;
        }
    }

    public override async Task<TRep> SendRequest<TReq, TRep>(TReq request, PipeRequestContext context, CancellationToken token)
    {
        var receiveTaskSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var requestCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        //cancel receive task when cancellation token triggered
        requestCancellation.Token.Register(() => receiveTaskSource.TrySetCanceled());

        var requestMessage = new PipeClientRequestMessage(Guid.NewGuid())
        {
            RequestCancellation = requestCancellation,
            ReceiveTask = receiveTaskSource,
            HeartbeatCheckHandle = new SemaphoreSlim(1),
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
            RequestOut.PublishRequestMessage(requestMessage, 
                (r) => HeartbeatOut.PublishHeartbeatMessage(r));
        else
            throw new InvalidOperationException($"Message with {requestMessage.Id} already scheduled");

        _logger.LogDebug("scheduled request execution for message {MessageId}", requestMessage.Id);
        try
        {
            //async block this task until we notified that request is received
            await receiveTaskSource.Task;
            _logger.LogDebug("received reply for message {MessageId} from server", requestMessage.Id);
        }
        catch (OperationCanceledException)
        {            
            _logger.LogDebug("cancelled request message {MessageId}", requestMessage.Id);
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

    private async Task SendRequest<TReq>(Guid id, PipeMessageRequest<TReq> request, PipeProtocol protocol, CancellationToken token)
    {
        try
        {
            _logger.LogDebug("sending request message {MessageId} to server", id);
            var header = new PipeAsyncMessageHeader { MessageId = id, ReplyPipe = ReplyIn.PipeName };
            await protocol.BeginTransferMessageAsync(header, token);
            await protocol.EndTransferMessage(id, Write, token);
            _logger.LogDebug("sent request message {MessageId} to server", id);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "unhandled error occurred while sending request message {MessageId} to server", id);
            throw;
        }

        Task Write(Stream stream, CancellationToken cancellation)
            => _messageWriter.WriteRequest(request, stream, cancellation);
    }

    private async Task<PipeMessageResponse<TRep>> ReadReply<TRep>(Guid id, PipeProtocol protocol, CancellationToken token)
    {
        try
        {
            _logger.LogDebug("receiving reply for message {MessageId} from server", id);
            var response = await protocol.EndReceiveMessage(id, Read, token);
            _logger.LogDebug("received reply for message {MessageId} from server", id);
            return response;
        }
        catch (Exception e)
        {
            _logger.LogError(e, "unhandled error occurred while receiving reply for message {MessageId} from server", id);
            throw;
        }

        ValueTask<PipeMessageResponse<TRep>> Read(Stream stream, CancellationToken cancellation)
            => _messageWriter.ReadResponse<TRep>(stream, cancellation);

    }

    public void Dispose()
    {
        _serverTaskCancellation.Cancel();
        _serverTask.Wait();
    }

    public async ValueTask DisposeAsync()
    {
        _serverTaskCancellation.Cancel();
        await _serverTask;
    }
}
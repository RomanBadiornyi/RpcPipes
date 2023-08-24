using System.Collections.Concurrent;
using System.IO.Pipes;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace RpcPipes.Transport;

public class PipeClient<TP> : PipeTransport, IDisposable, IAsyncDisposable
    where TP : IPipeProgress
{
    private class RequestMessage
    {
        public Guid Id { get; set; }
        public SemaphoreSlim ReceiveHandle { get; set; }

        public DateTime ProgressCheckTime { get; set; }
        public SemaphoreSlim ProgressCheckHandle { get; set; }

        public Func<NamedPipeClientStream, CancellationToken, Task> SendAction { get; set; }
        public Func<NamedPipeServerStream, int, CancellationToken, Task> ReceiveAction { get; set; }

        public CancellationTokenSource RequestCancellation { get; set; }

        public int Retries { get; set; }
        public Exception Exception { get; set; }
    }

    private readonly ILogger<PipeClient<TP>> _logger;

    private readonly IPipeProgressReceiver<TP> _progressHandler;
    private readonly IPipeMessageSerializer _serializer;

    private readonly Task _serverTask;
    private readonly CancellationTokenSource _serverTaskCancellation;

    private int _activeConnections;
    private int _clientConnections;
    private int _sentMessages;
    private int _receivedMessages;

    private readonly ConcurrentDictionary<Guid, RequestMessage> _requestQueue = new();
    private readonly Channel<RequestMessage> _requestChannel = Channel.CreateUnbounded<RequestMessage>();
    private readonly Channel<RequestMessage> _progressChannel = Channel.CreateUnbounded<RequestMessage>();

    public TimeSpan ConnectionTimeout = TimeSpan.FromSeconds(60);
    public TimeSpan ProgressFrequency = TimeSpan.FromSeconds(5);

    public PipeClient(ILogger<PipeClient<TP>> logger, string sendPipe, string receivePipe, string progressPipe, int instances, IPipeProgressReceiver<TP> progressHandler, IPipeMessageSerializer serializer) :
        base(logger, instances, 4 * 1024, PipeOptions.Asynchronous | PipeOptions.WriteThrough)
    {
        _logger = logger;

        _progressHandler = progressHandler;
        _serializer = serializer;

        _serverTaskCancellation = new CancellationTokenSource();

        var timer = new Timer(_ => {
            _logger.LogTrace(
                "DIAGNOSTIC: active connections {ActiveConnections}. client connections {ClientConnections}",
                _activeConnections, _clientConnections
            );
            _logger.LogTrace(
                "DIAGNOSTIC: sent messages {SentMessages}, received messages {ReceivedMessages}",
                _sentMessages, _receivedMessages
            );
        }, null, 0, 30000);

        _serverTask = Task.WhenAll(
                StartServerListener(Instances, () => {
                    return RunClientMessageLoop(
                        _serverTaskCancellation.Token,
                        sendPipe,
                        ConnectionTimeout,
                        HandleSendMessage,
                        onConnect: () => Interlocked.Increment(ref _clientConnections),
                        onDisconnect: () => Interlocked.Decrement(ref _clientConnections)
                    );
                }),
                StartServerListener(Instances, () => {
                    return RunClientMessageLoop(
                        _serverTaskCancellation.Token,
                        progressPipe,
                        ConnectionTimeout,
                        HandleProgressMessage,
                        onConnect: () => Interlocked.Increment(ref _clientConnections),
                        onDisconnect: () => Interlocked.Decrement(ref _clientConnections)
                    );
                }),
                StartServerListener(Instances, () => {
                    return RunServerMessageLoop(
                        _serverTaskCancellation.Token,
                        receivePipe,
                        HandleReceiveMessage,
                        onConnect: () => Interlocked.Increment(ref _activeConnections),
                        onDisconnect: () => Interlocked.Decrement(ref _activeConnections)
                    );
                })
            )
            .ContinueWith(_ => {
                timer.Dispose();
            }, CancellationToken.None);
    }

    public async Task<TRep> SendRequest<TReq, TRep>(TReq request, CancellationToken token)
    {
        using var requestCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);

        var requestMessage = new RequestMessage
        {
            Id = Guid.NewGuid(),

            RequestCancellation = requestCancellation,
            ReceiveHandle = new SemaphoreSlim(0),
            ProgressCheckHandle = new SemaphoreSlim(1),
            ProgressCheckTime = DateTime.Now
        };

        var responseMessage = default(PipeMessageResponse<TRep>);
        requestMessage.SendAction = 
            (c, t) => SendRequest(requestMessage.Id, request, c, t);
        requestMessage.ReceiveAction = 
            async (s, bs, t) => { responseMessage = await ReadReply<PipeMessageResponse<TRep>>(requestMessage.Id, s, bs, t); };

        if (_requestQueue.TryAdd(requestMessage.Id, requestMessage))
            await _requestChannel.Writer.WriteAsync(requestMessage, token);        
        else
            throw new InvalidOperationException($"Message with {requestMessage.Id} already scheduled");

        _logger.LogDebug("scheduled request execution for mesage {MessageId}", requestMessage.Id);
        try
        {
            await requestMessage.ReceiveHandle.WaitAsync(requestCancellation.Token);
            _logger.LogDebug("received reply for message {MessageId} from server", requestMessage.Id);
        }
        catch (OperationCanceledException)
        {
            _requestQueue.TryRemove(requestMessage.Id, out _);
            _logger.LogDebug("cancelled request message {MessageId}", requestMessage.Id);
            throw;
        }

        if (requestMessage.Exception != null)
            throw requestMessage.Exception;
        var responseError = responseMessage?.Exception;
        if (responseError != null)
            throw responseError.ToException();
        return responseMessage.Reply;
    }

    private async Task HandleSendMessage(NamedPipeClientStream clientPipeStream, CancellationToken cancellation)
    {
        var requestMessage = await _requestChannel.Reader.ReadAsync(cancellation);

        try
        {
            if (!cancellation.IsCancellationRequested)
            {
                var requestMessageSent = false;
                try
                {
                    await requestMessage.SendAction.Invoke(clientPipeStream, requestMessage.RequestCancellation.Token);
                    requestMessageSent = true;
                }
                catch (Exception e)
                {
                    requestMessage.Retries++;
                    if (requestMessage.Retries < 3)
                    {
                        await _requestChannel.Writer.WriteAsync(requestMessage, cancellation);
                    }
                    else 
                    {
                        requestMessage.Exception = e;
                        throw;
                    }
                }

                if (requestMessageSent)
                {
                    Interlocked.Increment(ref _sentMessages);
                    await _progressChannel.Writer.WriteAsync(requestMessage, cancellation);
                    _logger.LogDebug("sent message {MessageId} for execution", requestMessage.Id);
                }
            }
        }
        catch (Exception e)
        {
            _logger.LogError(e, "unhandled error occurred while sending message {MessageId} for execution", requestMessage.Id);
            throw;
        }
    }

    private async Task HandleProgressMessage(Stream progressPipeStream, CancellationToken cancellation)
    {
        var requestMessage = await _progressChannel.Reader.ReadAsync(cancellation);

        var requestMessageActive = false;
        var lastCheckInterval = DateTime.Now - requestMessage.ProgressCheckTime;
        if (lastCheckInterval < ProgressFrequency) 
        {
            await Task.Delay(ProgressFrequency - lastCheckInterval, cancellation);
            await _progressChannel.Writer.WriteAsync(requestMessage, cancellation);
            return;
        }

        if (!cancellation.IsCancellationRequested)
        {
            await requestMessage.ProgressCheckHandle.WaitAsync(cancellation);
            try
            {
                requestMessageActive = _requestQueue.ContainsKey(requestMessage.Id);
                if (requestMessageActive)
                {
                    var protocol = new PipeProtocol(progressPipeStream, _serializer);
                    var progressToken = new ProgressToken { Id = requestMessage.Id, Active = !requestMessage.RequestCancellation.IsCancellationRequested };
                    await protocol.TransferMessage(requestMessage.Id, BufferSize, progressToken, cancellation);
                    var progress = await protocol.ReceiveMessage<TP>(cancellation);
                    if (progress != null)
                    {
                        if (progress.Progress > 0)
                        {
                            _logger.LogDebug("received progress update for message {MessageId} with value {Progress}", requestMessage.Id, progress.Progress);
                            await _progressHandler.ReceiveProgress(progress);
                        }
                    }
                    else 
                    {
                        _logger.LogWarning("cancelling execution of request message {MessageId} as it's not handled by the server", requestMessage.Id);
                        requestMessage.RequestCancellation.Cancel();
                    }
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "unhandled error occured while requesting progress for message {MessageId}", requestMessage.Id);                
                throw;
            }
            finally 
            {
                requestMessage.ProgressCheckHandle.Release();
                if (requestMessageActive)
                {
                    requestMessage.ProgressCheckTime = DateTime.Now;
                    await _progressChannel.Writer.WriteAsync(requestMessage, cancellation);
                }
            }
        }
    }

    private async Task HandleReceiveMessage(NamedPipeServerStream serverPipeStream, CancellationToken cancellation)
    {
        RequestMessage requestMessage = null;
        var protocol = new PipeProtocol(serverPipeStream, _serializer);        
        var (messageId, bufferSize) = await protocol
            .BeginReceiveMessage(id => {
                //ensure we stop progress task as soon as we started receiving reply
                if (_requestQueue.TryGetValue(id, out requestMessage)) 
                {
                    _requestQueue.TryRemove(id, out _);
                    _logger.LogDebug("received reply message {MessageId}, cancelled progress updated", requestMessage.Id);
                }
                else 
                {
                    _logger.LogWarning("received reply message {MessageId} not found in request queue");
                }
            }, cancellation);

        if (messageId != null && requestMessage != null)
        {
            await requestMessage.ProgressCheckHandle.WaitAsync(requestMessage.RequestCancellation.Token);
            try
            {
                await requestMessage.ReceiveAction.Invoke(serverPipeStream, bufferSize, requestMessage.RequestCancellation.Token);
            }
            catch (Exception e)
            {
                requestMessage.Exception = e;                
            }
            finally
            {
                requestMessage.ProgressCheckHandle.Release();
                requestMessage.ReceiveHandle.Release();
                _logger.LogDebug("completed processing of reply message {MessageId}", messageId);
                Interlocked.Increment(ref _receivedMessages);
            }
        }
    }

    private async Task SendRequest<TReq>(Guid id, TReq request, Stream client, CancellationToken token)
    {
        try
        {
            _logger.LogDebug("sending request message {MessageId} to server", id);
            var protocol = new PipeProtocol(client, _serializer);
            await protocol.TransferMessage(id, BufferSize, request, token);
            _logger.LogDebug("sent request message {MessageId} to server", id);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "unhandled error occurred while sending request message {MessageId} to server", id);
            throw;
        }
    }

    private async Task<TRep> ReadReply<TRep>(Guid id, Stream server, int bufferSize, CancellationToken token)
    {
        try
        {
            _logger.LogDebug("receiving reply for message {MessageId} from server", id);
            var protocol = new PipeProtocol(server, _serializer);
            var response = await protocol.EndReceiveMessage<TRep>(id, bufferSize, token);
            _logger.LogDebug("received reply for message {MessageId} from server", id);
            return response;
        }
        catch (Exception e)
        {
            _logger.LogError(e, "unhandled error occurred while receiving reply for message {MessageId} from server", id);
            throw;
        }
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
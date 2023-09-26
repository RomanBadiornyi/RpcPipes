using System.Diagnostics.Metrics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeExceptions;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

internal class PipeReplyOutHandler : IPipeMessageSender<PipeServerRequestMessage>
{
    private readonly static Meter Meter = new(nameof(PipeReplyOutHandler));
    private readonly static Counter<int> ReplyMessagesCounter = Meter.CreateCounter<int>("reply-messages");

    private readonly ILogger _logger;
    private readonly Func<PipeServerRequestMessage, bool> _onMessageCompleted;
    private readonly Channel<PipeServerRequestMessage> _responseChannel;

    public Task<bool> ClientTask { get; }

    public PipeReplyOutHandler(
        ILogger logger,
        PipeMessageDispatcher connectionPool,
        Func<PipeServerRequestMessage, bool> onMessageCompleted)
    {
        _logger = logger;
        _onMessageCompleted = onMessageCompleted;
        _responseChannel = Channel.CreateUnbounded<PipeServerRequestMessage>();
        ClientTask = connectionPool.ProcessClientMessages(_responseChannel, this)
            .ContinueWith(t => t.IsCompleted, CancellationToken.None);
    }

    public async ValueTask Publish(PipeServerRequestMessage message)
    {
        _logger.LogDebug("scheduling reply for message {MessageId}", message.Id);
        ReplyMessagesCounter.Add(1);
        await _responseChannel.Writer.WriteAsync(message);
    }

    public string TargetPipe(PipeServerRequestMessage message)
        => message.Pipe;

    public async Task HandleMessage(PipeServerRequestMessage message, PipeProtocol protocol, CancellationToken cancellation)
    {
        _logger.LogDebug("scheduled reply for message {MessageId}", message.Id);
        await message.SendResponse.Invoke(protocol, cancellation);
        _onMessageCompleted.Invoke(message);
        ReplyMessagesCounter.Add(-1);
    }

    public async ValueTask HandleError(PipeServerRequestMessage message, Exception error, CancellationToken cancellation)
    {
        if (error is PipeDataException || cancellation.IsCancellationRequested)
            await ReportErrorOrComplete(message, error);
        else
            await RetryOrComplete(message, error);
    }

    private async ValueTask RetryOrComplete(PipeServerRequestMessage message, Exception error)
    {
        message.Retries += 1;
        if (error is PipeConnectionsExhausted)
        {
            //we did retry 3 times, if still no luck - drop message
            _onMessageCompleted.Invoke(message);
            _logger.LogError("unable to send message {MessageId} due to error '{ErrorMessage}'", message.Id, error.Message);
        }
        else if (message.Retries >= 3)
        {
            //we did retry 3 times, if still no luck - drop message
            _onMessageCompleted.Invoke(message);
            _logger.LogError(error, "unable to send message {MessageId} due to error '{ErrorMessage}'", message.Id, error.Message);
        }
        else
        {
            //publish to retry
            _logger.LogDebug("retry sending reply for message {MessageId} due to error '{ErrorMessage}'", message.Id, error.Message);
            await Publish(message);
        }
    }

    private async Task ReportErrorOrComplete(PipeServerRequestMessage message, Exception error)
    {
        message.Retries += 1;
        if (message.Retries >= 3 || !await message.ReportError(error))
        {
            //this means that we were not able to send error back to client, in this case simply drop message
            _onMessageCompleted.Invoke(message);
        }
    }
}
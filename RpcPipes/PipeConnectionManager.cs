using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.IO.Pipes;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeTransport;

namespace RpcPipes;

public class PipeMessageChannel<T>
{
    public Channel<T> Channel;
    public Task ChannelTask;
    public bool Completed;
}

public class PipeMessageChannelQueue<T>
{
    public ConcurrentDictionary<string, PipeMessageChannel<T>> MessageChannels { get; set; }
    public T Message { get; set; }
}

public class PipeConnectionManager
{    
    private readonly ILogger _logger;

    private Counter<int> _serverConnectionsCounter;
    private Counter<int> _clientConnectionsCounter;


    public PipeOptions Options { get; }
    public int Instances { get; }

    public int HeaderBufferSize { get; }
    public int BufferSize { get; }

    public TimeSpan ConnectionTimeout = TimeSpan.FromSeconds(60);
    public TimeSpan ClientConnectionExpiryTimeout = TimeSpan.FromSeconds(5);
    public TimeSpan ConnectionRetryTimeout = TimeSpan.FromSeconds(20);

    public PipeConnectionManager(ILogger logger, Meter meter, int instances, int headerBufferSize, int bufferSize, PipeOptions options)
    {
        _logger = logger;

        Instances = instances;

        HeaderBufferSize = headerBufferSize;
        BufferSize = bufferSize;

        Options = options;

        _serverConnectionsCounter = meter.CreateCounter<int>("server-connections");
        _clientConnectionsCounter = meter.CreateCounter<int>("client-connections");
    }    

    public Task ProcessServerMessages(string pipeName, Func<PipeProtocol, CancellationToken, Task> action, CancellationToken token)
        => StartServerListener(taskAction: () => RunServerMessageLoop(pipeName, action, token));    

    public void ProcessClientMessage<T>(        
        string pipeName,
        PipeMessageChannelQueue<T> messageQueueRequest,
        Func<T, PipeProtocol, CancellationToken, Task> messageDispatch,
        CancellationToken token)
    {
        var messageChannelCreated = false;
        while (!messageChannelCreated && !token.IsCancellationRequested)
        {
            var messageChannel = messageQueueRequest.MessageChannels.GetOrAdd(pipeName, new PipeMessageChannel<T>());
            lock (messageChannel)
            {
                //avoid race condition between creation and removal of channel
                if (messageChannel.Completed)
                    continue;
                messageChannel.Channel ??= Channel.CreateUnbounded<T>();
                messageChannelCreated = true;
                //we use unbound channel so TryWrite always will return true, no need to check here
                messageChannel.Channel.Writer.TryWrite(messageQueueRequest.Message);
                if (messageChannel.ChannelTask == null)
                    //if during write time we don't have any active connection tasks - start one
                    StartClientMessageLoop(pipeName, messageQueueRequest.MessageChannels, messageChannel, messageDispatch, token);
            }
        }
    }

    private Task StartServerListener(Func<Task> taskAction)
    {
        var pipeTasks = new List<Task>();        
        for (var i = 0; i < Instances; i++)
        {
            pipeTasks.Add(Task.Run(taskAction));
        }
        return Task.WhenAll(pipeTasks);
    }
    
    private async Task RunServerMessageLoop(string pipeName, Func<PipeProtocol, CancellationToken, Task> action, CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            var isConnected = false;
            var serverPipeStream = new NamedPipeServerStream(pipeName, PipeDirection.InOut, Instances, PipeTransmissionMode.Byte, Options, BufferSize, BufferSize);
            try
            {
                if (await WaitForClientConnection(serverPipeStream, pipeName, token))
                {
                    isConnected = true;
                    _serverConnectionsCounter.Add(1);

                    var protocol = new PipeProtocol(serverPipeStream, HeaderBufferSize, BufferSize);
                    while (!token.IsCancellationRequested && serverPipeStream.IsConnected)
                    {
                        await action.Invoke(protocol, token);
                    }
                }
                else
                {
                    await Task.Delay(ConnectionRetryTimeout, token);
                }
            }
            catch (IOException) when (serverPipeStream.IsConnected == false)
            {
                //happens on disconnect
            }            
            catch (OperationCanceledException)
            {
            }
            catch (Exception e)
            {
                _logger.LogError(e, "unhandled error occurred while connecting to the client pipe stream {Pipe}", pipeName);
            }
            finally
            {
                DisconnectServer(serverPipeStream, pipeName);
                if (isConnected)
                    _serverConnectionsCounter.Add(-1);
            }
        }
    }    

    private void StartClientMessageLoop<T>(
        string pipeName,
        ConcurrentDictionary<string, PipeMessageChannel<T>> messageChannels,
        PipeMessageChannel<T> messageChannel,
        Func<T, PipeProtocol, CancellationToken, Task> messageDispatch,
        CancellationToken token)
    {
        //after we added item to message channel - start connection tasks which wil connect to client
        //and dispatch all messages from the message channel
        messageChannel.ChannelTask = StartServerListener(
            () => RunClientMessageLoop(pipeName, messageChannel.Channel, messageDispatch, token));

        //run follow up task to cleanup connection if it is no longer in use or spin out new connections if more messages available
        _ = messageChannel.ChannelTask.ContinueWith(_ =>
        {
            //lock message channel so no incoming messages can be added to it while we cleaning up
            lock (messageChannel)
            {
                messageChannel.ChannelTask = null;
                //we are completing connection task here, so check if channel still has active messages
                //if so - roll out new connection task
                if (messageChannel.Channel.Reader.Count > 0)
                    StartClientMessageLoop(pipeName, messageChannels, messageChannel, messageDispatch, token);
                else
                {
                    //and if no new pending messages - mark message channel as completed
                    //to indicate that we can't longer use it during add operation and remove it from channels collection (e.g. full cleanup)
                    messageChannels.TryRemove(pipeName, out var _);
                    messageChannel.Completed = true;
                }
            }
        }, CancellationToken.None);
    }

    private async Task RunClientMessageLoop<T>(
        string pipeName,
        Channel<T> queue,
        Func<T, PipeProtocol, CancellationToken, Task> action,
        CancellationToken token)
    {
        var readExpired = false;
        while (!token.IsCancellationRequested && !readExpired)
        {
            var isConnected = false;
            var clientPipeStream = new NamedPipeClientStream(".", pipeName, PipeDirection.InOut, Options);
            try
            {
                if (await TryConnectToServer(clientPipeStream, pipeName, ConnectionTimeout, token))
                {
                    isConnected = true;
                    _clientConnectionsCounter.Add(1);

                    var protocol = new PipeProtocol(clientPipeStream, HeaderBufferSize, BufferSize);
                    while (!token.IsCancellationRequested && clientPipeStream.IsConnected)
                    {
                        if (!queue.Reader.TryRead(out var item))
                        {
                            try
                            {
                                using var readCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
                                readCancellation.CancelAfter(ClientConnectionExpiryTimeout);
                                item = await queue.Reader.ReadAsync(readCancellation.Token);
                            }
                            catch (OperationCanceledException)
                            {
                                readExpired = true;
                            }
                        }
                        if (!readExpired)
                        {
                            await action.Invoke(item, protocol, token);
                        }
                    }
                }
                else
                {
                    await Task.Delay(ConnectionRetryTimeout, token);
                }
            }
            catch (IOException) when (clientPipeStream.IsConnected == false)
            {
                //happens on disconnect
            }
            catch (InvalidOperationException)
            {
                //happens in case of missing Ack exchange
            }
            catch (InvalidDataException)
            {
                //happens in case of incorrect Ack exchange
            }
            catch (OperationCanceledException)
            {                
            }
            catch (Exception e)
            {
                _logger.LogError(e, "unhandled error occurred while connecting to the server pipe stream {Pipe}", pipeName);
            }
            finally
            {
                DisconnectClient(clientPipeStream, pipeName);
                if (isConnected)
                    _clientConnectionsCounter.Add(-1);                
            }
        }
    }

    private async Task<bool> WaitForClientConnection(NamedPipeServerStream server, string pipeName, CancellationToken token)
    {
        try
        {
            await server.WaitForConnectionAsync(token);
            return true;
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("connection to {Type} stream pipe {Pipe} closed", "server", pipeName);
            return false;
        }
        catch (IOException)
        {
            _logger.LogDebug("connection to {Type} stream pipe {Pipe} got interrupted", "server", pipeName);
            return false;
        }
        catch (UnauthorizedAccessException e)
        {
            _logger.LogDebug(e, "connection to {Type} stream pipe {Pipe} got Unauthorized error", "client", pipeName);
            return false;
        }
        catch (Exception e)
        {
            _logger.LogError(e, "connection to {Type} stream pipe {Pipe} got unhandled error", "server", pipeName);
            return false;
        }
    }

    private async Task<bool> TryConnectToServer(NamedPipeClientStream client, string pipeName, TimeSpan timeout, CancellationToken token)
    {
        using var connectionCancellation = new CancellationTokenSource();
        connectionCancellation.CancelAfter(timeout);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(connectionCancellation.Token, token);
        try
        {
            await client.ConnectAsync(Timeout.Infinite, cancellation.Token);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        catch (IOException)
        {
            _logger.LogDebug("connection to {Type} stream pipe {Pipe} got interrupted", "client", pipeName);
            return false;
        }
        catch (UnauthorizedAccessException e)
        {
            _logger.LogDebug(e, "connection to {Type} stream pipe {Pipe} got Unauthorized error", "client", pipeName);
            return false;
        }
        catch (Exception e)
        {
            _logger.LogError(e, "connection to {Type} stream pipe {Pipe} got unhandled error", "client", pipeName);
            return false;
        }
    }

    private void DisconnectServer(NamedPipeServerStream server, string pipeName)
    {
        try
        {
            try
            {
                if (server.IsConnected)
                    server.Disconnect();
            }
            finally
            {
                server.Dispose();
            }
        }
        catch (Exception e)
        {
            _logger.LogError(e, "unhandled error occurred when handling {Type} stream pipe {Pipe} got unhandled error", "server", pipeName);
        }
    }

    private void DisconnectClient(NamedPipeClientStream client, string pipeName)
    {
        try
        {
            try
            {
                client.Close();
            }
            finally
            {
                client.Dispose();
            }
        }
        catch (Exception e)
        {
            _logger.LogError(e, "unhandled error occurred when handling {Type} stream pipe {Pipe} got unhandled error", "client", pipeName);
        }
    }
}
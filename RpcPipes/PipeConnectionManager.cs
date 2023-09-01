using System.Collections.Concurrent;
using System.IO.Pipes;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace RpcPipes;

public class PipeConnectionManager
{
    protected class PipeMessageChannel<T>
    {
        public Channel<T> Channel;
        public Task ChannelTask;
        public bool Completed;
    }

    private readonly ILogger _logger;

    protected readonly PipeOptions Options;
    protected readonly int Instances;

    protected readonly int HeaderBufferSize;
    protected readonly int BufferSize;

    protected Action OnClientConnect;
    protected Action OnClientDisconnect;
    protected Action OnServerConnect;
    protected Action OnServerDisconnect;

    public TimeSpan ConnectionTimeout = TimeSpan.FromSeconds(60);

    public PipeConnectionManager(ILogger logger, int instances, int headerBufferSize, int bufferSize, PipeOptions options)
    {
        _logger = logger;

        Instances = instances;

        HeaderBufferSize = headerBufferSize;
        BufferSize = bufferSize;

        Options = options;
    }

    protected Task StartServerListener(int count, Func<Task> taskAction)
    {
        var pipeTasks = new List<Task>();
        for (var i = 0; i < count; i++)
        {
            pipeTasks.Add(
                Task.Factory.StartNew(
                    taskAction.Invoke,
                    CancellationToken.None,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default)
                .Unwrap());
        }
        return Task.WhenAll(pipeTasks);
    }

    protected async Task RunServerMessageLoop(
        CancellationToken token,
        string pipeName,
        Func<PipeProtocol, CancellationToken, Task> action)
    {
        var connectionBuffer = new byte[8];
        while (!token.IsCancellationRequested)
        {
            var isConnected = false;
            var serverPipeStream = new NamedPipeServerStream(pipeName, PipeDirection.InOut, Instances, PipeTransmissionMode.Byte, Options, BufferSize, BufferSize);
            try
            {
                if (await WaitForClientConnection(serverPipeStream, pipeName, token))
                {
                    isConnected = true;
                    OnServerConnect?.Invoke();
                    Array.Copy(BitConverter.GetBytes(HeaderBufferSize), 0, connectionBuffer, 0, 4);
                    Array.Copy(BitConverter.GetBytes(BufferSize), 0, connectionBuffer, 4, 4);
                    await serverPipeStream.WriteAsync(connectionBuffer, 0, connectionBuffer.Length, token);
                    var protocol = new PipeProtocol(serverPipeStream, HeaderBufferSize, BufferSize);
                    while (!token.IsCancellationRequested && serverPipeStream.IsConnected)
                    {
                        await action.Invoke(protocol, token);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception e)
            {
                _logger.LogError(e, "unhandled error occurred while connecting to the client pipe stream {Pipe}", pipeName);
            }
            finally
            {
                DisconnectServer(serverPipeStream, pipeName);
                if (isConnected)
                    OnServerDisconnect?.Invoke();
            }
        }
    }

    protected void ProcessClientMessage<T>(
        ConcurrentDictionary<string, PipeMessageChannel<T>> messageChannels,
        string pipeName,
        T message,
        Func<T, PipeProtocol, CancellationToken, Task> messageDispatch,
        CancellationToken token)
    {
        var messageChannelCreated = false;
        while (!messageChannelCreated)
        {
            PipeMessageChannel<T> messageChannel = messageChannels.GetOrAdd(pipeName, new PipeMessageChannel<T>());
            lock (messageChannel)
            {
                //avoid race condition between creation and removal of channel
                if (messageChannel.Completed)
                    continue;
                messageChannel.Channel ??= Channel.CreateUnbounded<T>();
                messageChannelCreated = true;
                //we use unbound channel so TryWrite always will return true, no need to check here
                messageChannel.Channel.Writer.TryWrite(message);
                if (messageChannel.ChannelTask == null)
                    //if during write time we don't have any active connection tasks - start one
                    StartClientMessageLoop(pipeName, messageChannels, messageChannel, messageDispatch, token);
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
            Instances,
            () => RunClientMessageLoop(
                    token,
                    pipeName,
                    TimeSpan.FromSeconds(5),
                    messageChannel.Channel,
                    messageDispatch
                ));
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
        CancellationToken token,
        string pipeName,
        TimeSpan readTimeout,
        Channel<T> queue,
        Func<T, PipeProtocol, CancellationToken, Task> action)
    {
        var connectionBuffer = new byte[8];
        
        var item = default(T);
        var itemAvailable = false;

        while (!token.IsCancellationRequested)
        {
            var isConnected = false;
            var clientPipeStream = new NamedPipeClientStream(".", pipeName, PipeDirection.InOut, Options);
            try
            {
                if (await TryConnectToServer(clientPipeStream, pipeName, ConnectionTimeout, token))
                {
                    isConnected = true;
                    OnClientConnect?.Invoke();

                    _ = await clientPipeStream.ReadAsync(connectionBuffer, 0, connectionBuffer.Length, token);
                    var protocol = new PipeProtocol(
                        clientPipeStream,
                        BitConverter.ToInt32(connectionBuffer, 0),
                        BitConverter.ToInt32(connectionBuffer, 4));
                    while (!token.IsCancellationRequested && clientPipeStream.IsConnected)
                    {
                        if (!queue.Reader.TryRead(out item))
                        {
                            itemAvailable = false;
                            using var readCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
                            readCancellation.CancelAfter(readTimeout);
                            item = await queue.Reader.ReadAsync(readCancellation.Token);
                        }
                        itemAvailable = true;                                                
                        await action.Invoke(item, protocol, token);
                    }
                }
            }
            catch (IOException) when (clientPipeStream.IsConnected == false)
            {
                break;
            }
            catch (InvalidOperationException)
            {
                //happens in case of missing Ack exchange
                break;
            }
            catch (InvalidDataException)
            {
                //happens in case of incorrect Ack exchange
                break;
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception e)
            {
                _logger.LogError(e, "unhandled error occurred while connecting to the server pipe stream {Pipe}", pipeName);
            }
            finally
            {
                //in case of handled exception - put item back to queue so we retry action on reconnect
                if (itemAvailable)
                    await queue.Writer.WriteAsync(item, token);
                DisconnectClient(clientPipeStream, pipeName);
                if (isConnected)
                    OnClientDisconnect?.Invoke();
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
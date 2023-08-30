using System.Collections.Concurrent;
using System.IO.Pipes;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace RpcPipes;

public class PipeTransport
{
    protected class MessageChannel<T>
    {
        public Channel<T> Channel;
        public Task ChannelTask;
        public bool Completed;
    }

    private readonly ILogger _logger;

    protected readonly PipeOptions Options;
    protected readonly int Instances;
    protected readonly int BufferSize;

    protected Action OnClientConnect;
    protected Action OnClientDisconnect;
    protected Action OnServerConnect;
    protected Action OnServerDisconnect;

    public TimeSpan ConnectionTimeout = TimeSpan.FromSeconds(60);

    public PipeTransport(ILogger logger, int instances, int bufferSize, PipeOptions options)
    {
        _logger = logger;

        Instances = instances;
        BufferSize = bufferSize;
        Options = options;
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
        Func<NamedPipeServerStream, CancellationToken, Task> action)
    {        
        while (!token.IsCancellationRequested)
        {
            bool isConnected = false;
            var serverPipeStream = new NamedPipeServerStream(pipeName, PipeDirection.InOut, Instances, PipeTransmissionMode.Byte, Options, BufferSize, BufferSize);
            try
            {
                if (await WaitForClientConnection(serverPipeStream, pipeName, token))
                {
                    isConnected = true;
                    OnServerConnect?.Invoke();
                    while (!token.IsCancellationRequested && serverPipeStream.IsConnected)
                    {
                        await action.Invoke(serverPipeStream, token);
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
        ConcurrentDictionary<string, MessageChannel<T>> messageChannels, 
        string pipeName,
        T message,                       
        Func<T, NamedPipeClientStream, CancellationToken, Task> messageDispatch,
        CancellationToken token)
    {
        var messageChannelCreated = false;
        while (!messageChannelCreated)
        {
            MessageChannel<T> messageChannel = messageChannels.GetOrAdd(pipeName, new MessageChannel<T>());
            lock (messageChannel)
            {
                //avoid race condition between creation and removal of channel
                if (messageChannel.Completed)
                    continue;
                if (messageChannel.Channel == null)
                    messageChannel.Channel = Channel.CreateUnbounded<T>();
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
        ConcurrentDictionary<string, MessageChannel<T>> messageChannels,         
        MessageChannel<T> messageChannel,
        Func<T, NamedPipeClientStream, CancellationToken, Task> messageDispatch,
        CancellationToken token)
    {
        //after we added item to message channel - start connection tasks which wil connect to client
        //and dispatch all messages from the message channel
        messageChannel.ChannelTask = StartServerListener(Instances, () => {
            return RunClientMessageLoop(
                        token,
                        pipeName,
                        TimeSpan.FromSeconds(5),
                        messageChannel.Channel,
                        messageDispatch
                    );
                });
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
                    messageChannels.TryRemove(pipeName, out var removedMessageChannel);
                    messageChannel.Completed = true;
                }
            }            
        });
    }

    private async Task RunClientMessageLoop<T>(
        CancellationToken token, 
        string pipeName, 
        TimeSpan readTimeout, 
        Channel<T> queue, 
        Func<T, NamedPipeClientStream, CancellationToken, Task> action)
    {
        var available = true;
        while (!token.IsCancellationRequested && available)
        {
            available = queue.Reader.TryRead(out var item);
            if (!available)
                break;

            bool isConnected = false;
            var clientPipeStream = new NamedPipeClientStream(".", pipeName, PipeDirection.InOut, Options);
            try
            {
                if (await TryConnectToServer(clientPipeStream, pipeName, ConnectionTimeout, token))
                {
                    isConnected = true;
                    OnClientConnect?.Invoke();
                    while (!token.IsCancellationRequested && clientPipeStream.IsConnected)
                    {
                        await action.Invoke(item, clientPipeStream, token);
                        available = false;
                        item = default;
                        if (!queue.Reader.TryRead(out item))
                        {
                            var readCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
                            readCancellation.CancelAfter(readTimeout);
                            item = await queue.Reader.ReadAsync(readCancellation.Token);
                            available = true;
                        }
                    }
                }
            }
            catch (IOException) when (clientPipeStream.IsConnected == false)
            {
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
                //put back item to the queue if we read it from channel but did not process
                //not that available is set to false on successfully execution of action
                if (available)
                    await queue.Writer.WriteAsync(item, token);
                DisconnectClient(clientPipeStream, pipeName);
                if (isConnected)
                    OnClientDisconnect?.Invoke();
            }
        }
    }
}
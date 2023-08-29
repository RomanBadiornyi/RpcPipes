using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes;

public class PipeTransport
{
    private readonly ILogger _logger;

    protected readonly PipeOptions Options;
    protected readonly int Instances;
    protected readonly int BufferSize;

    public PipeTransport(ILogger logger, int instances, int bufferSize, PipeOptions options)
    {
        _logger = logger;

        Instances = instances;
        BufferSize = bufferSize;
        Options = options;
    }

    protected Task StartServerListener(int count, Func<Task> taskAction)
    {
        var pipeTasks = new List<Task>();
        for (var i = 0; i < count; i++)
        {
            pipeTasks.Add(Task.Factory.StartNew(taskAction.Invoke, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap());
        }
        return Task.WhenAll(pipeTasks);
    }

    protected async Task<bool> WaitForClientConnection(NamedPipeServerStream server, string pipeName, CancellationToken token)
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

    protected async Task<bool> TryConnectToServer(NamedPipeClientStream client, string pipeName, TimeSpan timeout, CancellationToken token)
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

    protected void DisconnectServer(NamedPipeServerStream server, string pipeName)
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

    protected void DisconnectClient(NamedPipeClientStream client, string pipeName)
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

    public async Task RunClientMessageLoop(
        CancellationToken token, string pipeName, TimeSpan connectionTimeout, Func<NamedPipeClientStream, CancellationToken, Task> action, Action onConnect, Action onDisconnect)
    {
        while (!token.IsCancellationRequested) 
        {
            bool isConnected = false;
            var clientPipeStream = new NamedPipeClientStream(".", pipeName, PipeDirection.InOut, Options);
            try
            {
                if (await TryConnectToServer(clientPipeStream, pipeName, connectionTimeout, token)) 
                {
                    isConnected = true;
                    onConnect.Invoke();
                    while (!token.IsCancellationRequested && clientPipeStream.IsConnected)
                    {
                        await action.Invoke(clientPipeStream, token);                        
                    }
                }
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
                DisconnectClient(clientPipeStream, pipeName);
                if (isConnected)
                    onDisconnect.Invoke();
            }
        }
    }

    public async Task RunServerMessageLoop(
        CancellationToken token, string pipeName, Func<NamedPipeServerStream, CancellationToken, Task> action, Action onConnect, Action onDisconnect)
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
                    onConnect.Invoke();
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
                    onDisconnect.Invoke();
            }            
        }
    }
}
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NUnit.Logger;
using RpcPipes.PipeConnections;

namespace RpcPipes.Tests.PipeConnections;

[TestFixture]
public class PipeConnectionGroupTests
{
    private readonly ILogger _logger = new NUnitLogger(nameof(PipeClientConnection));
    private readonly Meter _meter = new(nameof(PipeClientConnection));    

    [Test]
    public async Task BorrowConnection_WhenBrokenConnectionRestored_RestoredConnectionReturned()
    {
        var connectionGroup = new PipeConnectionGroup<PipeClientConnection>(_logger, "test", 3, DummyConnection, TimeSpan.FromSeconds(30), TimeSpan.Zero);
        for (var i = 0; i < 3; i++)
        {
            var (connection, error) = await connectionGroup.BorrowConnection(0);
            Assert.That(connection, Is.Not.Null);
            connectionGroup.ReturnConnection(connection, new Exception("error"));
        }
        var borrowTasks = new List<ValueTask<(PipeClientConnection Connection, AggregateException Error)>>();
        for (var i = 0; i < 3; i++)
        {
            borrowTasks.Add(connectionGroup.BorrowConnection(10000));
        }
        var waitTime = new Stopwatch();
        waitTime.Start();
        connectionGroup.RestoreConnections();
        var connections = await Task.WhenAll(borrowTasks.Select(v => v.AsTask()));
        waitTime.Stop();
        Assert.That(connections.Select(c => c.Connection).ToList(), Has.All.Not.Null);
        Assert.That(waitTime.ElapsedMilliseconds, Is.LessThan(500));

        PipeClientConnection DummyConnection(int id, string name)
        {
            var mock = Substitute.For<PipeClientConnection>(_logger, _meter, id, name, TimeSpan.Zero, TimeSpan.Zero);                
            mock.ConnectionErrors.Returns(_ => 3);
            return mock;
        }
    }
}
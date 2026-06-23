namespace PollyKafka.Tests;

public class ResilientConsumerTests
{
    private static ConsumeResult<string, string> MakeConsumeResult() =>
        new()
        {
            Topic = "test-topic",
            Partition = new Partition(0),
            Offset = new Offset(5),
            Message = new Message<string, string> { Key = "k", Value = "v" },
        };

    // ── Success path ──────────────────────────────────────────────────────

    [Fact]
    public async Task ConsumeAsync_Success_ReturnsResult()
    {
        var inner = Substitute.For<IConsumer<string, string>>();
        var expected = MakeConsumeResult();
        inner.Consume(Arg.Any<CancellationToken>()).Returns(expected);

        var consumer = new ResilientConsumer<string, string>(inner, TestFactory.FastOptions());
        var result = await consumer.ConsumeAsync();

        result.Should().Be(expected);
    }

    // ── Retry on transient errors ──────────────────────────────────────────

    [Theory]
    [InlineData(ErrorCode.BrokerNotAvailable)]
    [InlineData(ErrorCode.RequestTimedOut)]
    [InlineData(ErrorCode.Local_TimedOut)]
    [InlineData(ErrorCode.Local_AllBrokersDown)]
    public async Task ConsumeAsync_TransientError_Retries(ErrorCode code)
    {
        int calls = 0;
        var inner = Substitute.For<IConsumer<string, string>>();
        var expected = MakeConsumeResult();

        inner.Consume(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            calls++;
            if (calls < 2) throw TestFactory.MakeKafkaException(code);
            return expected;
        });

        var consumer = new ResilientConsumer<string, string>(inner,
            TestFactory.FastOptions(o => { o.MaxRetries = 3; o.CircuitBreakerMinimumThroughput = 100; }));

        var result = await consumer.ConsumeAsync();
        result.Should().Be(expected);
        calls.Should().Be(2);
    }

    [Fact]
    public async Task ConsumeAsync_NonTransientError_NotRetried()
    {
        int calls = 0;
        var inner = Substitute.For<IConsumer<string, string>>();
        inner.Consume(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            calls++;
            throw TestFactory.MakeKafkaException(ErrorCode.TopicAuthorizationFailed);
            return null!;
        });

        var consumer = new ResilientConsumer<string, string>(inner,
            TestFactory.FastOptions(o => { o.MaxRetries = 3; o.CircuitBreakerMinimumThroughput = 100; }));

        var act = () => consumer.ConsumeAsync();
        await act.Should().ThrowAsync<KafkaException>();
        calls.Should().Be(1);
    }

    [Fact]
    public async Task ConsumeAsync_ExhaustsRetries_ThrowsTransientKafkaException()
    {
        var inner = Substitute.For<IConsumer<string, string>>();
        inner.Consume(Arg.Any<CancellationToken>())
             .Returns(_ => throw TestFactory.MakeKafkaException(ErrorCode.BrokerNotAvailable));

        var consumer = new ResilientConsumer<string, string>(inner,
            TestFactory.FastOptions(o => { o.MaxRetries = 2; o.CircuitBreakerMinimumThroughput = 100; }));

        var act = () => consumer.ConsumeAsync();
        await act.Should().ThrowAsync<TransientKafkaException>()
            .Where(e => e.ErrorCode == ErrorCode.BrokerNotAvailable);
    }

    // ── Null guards ───────────────────────────────────────────────────────

    [Fact]
    public void Constructor_NullInner_Throws()
    {
        Action act = () => new ResilientConsumer<string, string>(null!, new PollyKafkaOptions());
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_NullOptions_Throws()
    {
        var inner = Substitute.For<IConsumer<string, string>>();
        Action act = () => new ResilientConsumer<string, string>(inner, null!);
        act.Should().Throw<ArgumentNullException>();
    }
}

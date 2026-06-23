namespace PollyKafka.Tests;

public class ResilientProducerTests
{
    // ── Success path ──────────────────────────────────────────────────────

    [Fact]
    public async Task ProduceAsync_Success_ReturnsDeliveryResult()
    {
        var inner = Substitute.For<IProducer<string, string>>();
        var expected = TestFactory.MakeDeliveryResult();
        inner.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, string>>(), Arg.Any<CancellationToken>())
             .Returns(Task.FromResult(expected));

        var producer = new ResilientProducer<string, string>(inner, TestFactory.FastOptions());
        var result = await producer.ProduceAsync("test-topic", new Message<string, string> { Key = "k", Value = "v" });

        result.Should().Be(expected);
    }

    // ── Retry on transient errors ──────────────────────────────────────────

    [Theory]
    [InlineData(ErrorCode.BrokerNotAvailable)]
    [InlineData(ErrorCode.LeaderNotAvailable)]
    [InlineData(ErrorCode.RequestTimedOut)]
    [InlineData(ErrorCode.NetworkException)]
    [InlineData(ErrorCode.Local_AllBrokersDown)]
    [InlineData(ErrorCode.Local_TimedOut)]
    public async Task ProduceAsync_TransientError_Retries(ErrorCode code)
    {
        int calls = 0;
        var inner = Substitute.For<IProducer<string, string>>();
        var expected = TestFactory.MakeDeliveryResult();

        inner.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, string>>(), Arg.Any<CancellationToken>())
             .Returns(_ =>
             {
                 calls++;
                 if (calls < 2) throw TestFactory.MakeKafkaException(code);
                 return Task.FromResult(expected);
             });

        var producer = new ResilientProducer<string, string>(inner,
            TestFactory.FastOptions(o => { o.MaxRetries = 3; o.CircuitBreakerMinimumThroughput = 100; }));

        var result = await producer.ProduceAsync("test-topic", new Message<string, string> { Key = "k", Value = "v" });

        result.Should().Be(expected);
        calls.Should().Be(2);
    }

    [Fact]
    public async Task ProduceAsync_NonTransientError_NotRetried()
    {
        int calls = 0;
        var inner = Substitute.For<IProducer<string, string>>();

        inner.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, string>>(), Arg.Any<CancellationToken>())
             .Returns<Task<DeliveryResult<string, string>>>(_ =>
             {
                 calls++;
                 throw TestFactory.MakeKafkaException(ErrorCode.TopicAuthorizationFailed);
             });

        var producer = new ResilientProducer<string, string>(inner,
            TestFactory.FastOptions(o => { o.MaxRetries = 3; o.CircuitBreakerMinimumThroughput = 100; }));

        var act = () => producer.ProduceAsync("test-topic", new Message<string, string> { Key = "k", Value = "v" });
        await act.Should().ThrowAsync<KafkaException>();
        calls.Should().Be(1);
    }

    [Fact]
    public async Task ProduceAsync_ExhaustsRetries_ThrowsTransientKafkaException()
    {
        var inner = Substitute.For<IProducer<string, string>>();
        inner.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, string>>(), Arg.Any<CancellationToken>())
             .Returns<Task<DeliveryResult<string, string>>>(_ =>
                 throw TestFactory.MakeKafkaException(ErrorCode.BrokerNotAvailable));

        var producer = new ResilientProducer<string, string>(inner,
            TestFactory.FastOptions(o => { o.MaxRetries = 2; o.CircuitBreakerMinimumThroughput = 100; }));

        var act = () => producer.ProduceAsync("test-topic", new Message<string, string> { Key = "k", Value = "v" });
        await act.Should().ThrowAsync<TransientKafkaException>()
            .Where(e => e.ErrorCode == ErrorCode.BrokerNotAvailable);
    }

    // ── Circuit breaker ───────────────────────────────────────────────────

    [Fact]
    public async Task CircuitBreaker_OpensAfterThreshold()
    {
        var inner = Substitute.For<IProducer<string, string>>();
        inner.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, string>>(), Arg.Any<CancellationToken>())
             .Returns<Task<DeliveryResult<string, string>>>(_ =>
                 throw TestFactory.MakeKafkaException(ErrorCode.BrokerNotAvailable));

        var producer = new ResilientProducer<string, string>(inner, TestFactory.FastOptions(o =>
        {
            o.MaxRetries = 0;
            o.CircuitBreakerMinimumThroughput = 3;
            o.CircuitBreakerFailureRatio = 0.5;
            o.CircuitBreakerSamplingDuration = TimeSpan.FromSeconds(10);
            o.CircuitBreakerBreakDuration = TimeSpan.FromSeconds(10);
        }));

        var exceptions = new List<Exception>();
        for (int i = 0; i < 10; i++)
        {
            try { await producer.ProduceAsync("t", new Message<string, string> { Key = "k", Value = "v" }); }
            catch (Exception ex) { exceptions.Add(ex); }
        }

        exceptions.Should().Contain(e => e is BrokenCircuitException);
    }

    // ── Timeout ───────────────────────────────────────────────────────────

    [Fact]
    public async Task ProduceAsync_Timeout_ThrowsTimeoutRejectedException()
    {
        var inner = Substitute.For<IProducer<string, string>>();
        inner.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, string>>(), Arg.Any<CancellationToken>())
             .Returns(async _ =>
             {
                 await Task.Delay(TimeSpan.FromSeconds(5));
                 return TestFactory.MakeDeliveryResult();
             });

        var producer = new ResilientProducer<string, string>(inner, new PollyKafkaOptions
        {
            MaxRetries = 0,
            OperationTimeout = TimeSpan.FromMilliseconds(50),
            CircuitBreakerMinimumThroughput = 100,
        });

        var act = () => producer.ProduceAsync("t", new Message<string, string> { Key = "k", Value = "v" });
        await act.Should().ThrowAsync<TimeoutRejectedException>();
    }

    // ── Null guards ───────────────────────────────────────────────────────

    [Fact]
    public void Constructor_NullInner_Throws()
    {
        Action act = () => new ResilientProducer<string, string>(null!, new PollyKafkaOptions());
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_NullOptions_Throws()
    {
        var inner = Substitute.For<IProducer<string, string>>();
        Action act = () => new ResilientProducer<string, string>(inner, null!);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public async Task ProduceAsync_NullTopic_Throws()
    {
        var inner = Substitute.For<IProducer<string, string>>();
        var producer = new ResilientProducer<string, string>(inner, TestFactory.FastOptions());
        var act = () => producer.ProduceAsync(null!, new Message<string, string> { Key = "k", Value = "v" });
        await act.Should().ThrowAsync<ArgumentException>();
    }

    // ── TransientKafkaException properties ────────────────────────────────

    [Fact]
    public async Task TransientKafkaException_HasCorrectProperties()
    {
        var inner = Substitute.For<IProducer<string, string>>();
        inner.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, string>>(), Arg.Any<CancellationToken>())
             .Returns<Task<DeliveryResult<string, string>>>(_ =>
                 throw TestFactory.MakeKafkaException(ErrorCode.LeaderNotAvailable));

        var producer = new ResilientProducer<string, string>(inner,
            TestFactory.FastOptions(o => { o.MaxRetries = 0; o.CircuitBreakerMinimumThroughput = 100; }));

        var act = () => producer.ProduceAsync("t", new Message<string, string> { Key = "k", Value = "v" });
        var ex = await act.Should().ThrowAsync<TransientKafkaException>();
        ex.Which.ErrorCode.Should().Be(ErrorCode.LeaderNotAvailable);
        ex.Which.KafkaException.Should().NotBeNull();
    }
}

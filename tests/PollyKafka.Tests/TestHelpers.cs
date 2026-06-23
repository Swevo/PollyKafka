namespace PollyKafka.Tests;

internal static class TestFactory
{
    public static PollyKafkaOptions FastOptions(Action<PollyKafkaOptions>? configure = null)
    {
        var opts = new PollyKafkaOptions
        {
            BaseDelay = TimeSpan.Zero,
            MaxDelay = TimeSpan.Zero,
            OperationTimeout = TimeSpan.FromSeconds(10),
            CircuitBreakerMinimumThroughput = 100,
            CircuitBreakerSamplingDuration = TimeSpan.FromSeconds(10),
            CircuitBreakerBreakDuration = TimeSpan.FromSeconds(1),
        };
        configure?.Invoke(opts);
        return opts;
    }

    /// <summary>Builds a faulted DeliveryReport as a KafkaException.</summary>
    public static KafkaException MakeKafkaException(ErrorCode code)
        => new(new Error(code, "test error", false));

    public static DeliveryResult<string, string> MakeDeliveryResult(string topic = "test-topic")
        => new()
        {
            Topic = topic,
            Partition = new Partition(0),
            Offset = new Offset(1),
            Message = new Message<string, string> { Key = "k", Value = "v" },
            Status = PersistenceStatus.Persisted,
        };
}

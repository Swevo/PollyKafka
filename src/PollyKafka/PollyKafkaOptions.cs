namespace PollyKafka;

/// <summary>
/// Configuration options for Polly resilience applied to Kafka producers and consumers.
/// </summary>
public sealed class PollyKafkaOptions
{
    // ── Retry ─────────────────────────────────────────────────────────────

    /// <summary>Number of retry attempts. Set to 0 to disable retries.</summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>Base delay for exponential back-off with jitter.</summary>
    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromMilliseconds(200);

    /// <summary>Maximum delay cap for exponential back-off.</summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(30);

    // ── Circuit breaker ───────────────────────────────────────────────────

    /// <summary>Fraction of failures (0–1) required to open the circuit.</summary>
    public double CircuitBreakerFailureRatio { get; set; } = 0.5;

    /// <summary>Minimum calls within the sampling window before the circuit can open.</summary>
    public int CircuitBreakerMinimumThroughput { get; set; } = 10;

    /// <summary>Sliding window over which failures are measured.</summary>
    public TimeSpan CircuitBreakerSamplingDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How long the circuit stays open before moving to half-open.</summary>
    public TimeSpan CircuitBreakerBreakDuration { get; set; } = TimeSpan.FromSeconds(5);

    // ── Timeout ───────────────────────────────────────────────────────────

    /// <summary>Maximum time allowed per produce or consume operation.</summary>
    public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromSeconds(10);

    // ── Transient error codes ─────────────────────────────────────────────

    /// <summary>
    /// Kafka error codes that should be treated as transient and therefore retried.
    /// Defaults to the most common broker-side transient errors.
    /// </summary>
    public HashSet<ErrorCode> TransientErrorCodes { get; set; } = new()
    {
        ErrorCode.BrokerNotAvailable,
        ErrorCode.LeaderNotAvailable,
        ErrorCode.NotLeaderForPartition,
        ErrorCode.RequestTimedOut,
        ErrorCode.NetworkException,
        ErrorCode.UnknownTopicOrPart,   // during topic creation race
        ErrorCode.KafkaStorageError,
        ErrorCode.Local_AllBrokersDown,
        ErrorCode.Local_TimedOut,
        ErrorCode.Local_Transport,
        ErrorCode.Local_MsgTimedOut,
    };
}

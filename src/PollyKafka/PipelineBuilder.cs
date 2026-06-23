namespace PollyKafka;

/// <summary>
/// Internal helper that constructs the Polly v8 resilience pipeline shared by
/// <see cref="ResilientProducer{TKey,TValue}"/> and <see cref="ResilientConsumer{TKey,TValue}"/>.
/// </summary>
internal static class PipelineBuilder
{
    /// <summary>Builds an untyped pipeline (used by the producer).</summary>
    public static ResiliencePipeline Build(PollyKafkaOptions options)
    {
        var predicate = new PredicateBuilder().Handle<TransientKafkaException>();
        var builder = new ResiliencePipelineBuilder();

        if (options.MaxRetries >= 1)
        {
            builder.AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = predicate,
                MaxRetryAttempts = options.MaxRetries,
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                Delay = options.BaseDelay,
                MaxDelay = options.MaxDelay,
            });
        }

        builder
            .AddCircuitBreaker(new CircuitBreakerStrategyOptions
            {
                ShouldHandle = predicate,
                FailureRatio = options.CircuitBreakerFailureRatio,
                MinimumThroughput = options.CircuitBreakerMinimumThroughput,
                SamplingDuration = options.CircuitBreakerSamplingDuration,
                BreakDuration = options.CircuitBreakerBreakDuration,
            })
            .AddTimeout(new TimeoutStrategyOptions
            {
                Timeout = options.OperationTimeout,
            });

        return builder.Build();
    }

    /// <summary>Builds a typed pipeline (used by the consumer).</summary>
    public static ResiliencePipeline<T> BuildTyped<T>(PollyKafkaOptions options)
    {
        var predicate = new PredicateBuilder<T>().Handle<TransientKafkaException>();
        var builder = new ResiliencePipelineBuilder<T>();

        if (options.MaxRetries >= 1)
        {
            builder.AddRetry(new RetryStrategyOptions<T>
            {
                ShouldHandle = predicate,
                MaxRetryAttempts = options.MaxRetries,
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                Delay = options.BaseDelay,
                MaxDelay = options.MaxDelay,
            });
        }

        builder
            .AddCircuitBreaker(new CircuitBreakerStrategyOptions<T>
            {
                ShouldHandle = predicate,
                FailureRatio = options.CircuitBreakerFailureRatio,
                MinimumThroughput = options.CircuitBreakerMinimumThroughput,
                SamplingDuration = options.CircuitBreakerSamplingDuration,
                BreakDuration = options.CircuitBreakerBreakDuration,
            })
            .AddTimeout(new TimeoutStrategyOptions
            {
                Timeout = options.OperationTimeout,
            });

        return builder.Build();
    }
}

namespace PollyKafka;

/// <summary>
/// A resilient Kafka consumer that wraps <see cref="IConsumer{TKey,TValue}"/>
/// with a Polly v8 pipeline: retry → circuit breaker → timeout.
/// </summary>
public sealed class ResilientConsumer<TKey, TValue> : IDisposable
{
    private readonly IConsumer<TKey, TValue> _inner;
    private readonly ResiliencePipeline<ConsumeResult<TKey, TValue>?> _pipeline;
    private readonly PollyKafkaOptions _options;

    /// <summary>
    /// Initialises the resilient consumer.
    /// </summary>
    public ResilientConsumer(IConsumer<TKey, TValue> inner, PollyKafkaOptions options)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(options);
        _inner = inner;
        _options = options;
        _pipeline = PipelineBuilder.BuildTyped<ConsumeResult<TKey, TValue>?>(options);
    }

    /// <summary>
    /// Subscribes to the specified topics.
    /// </summary>
    public void Subscribe(IEnumerable<string> topics) => _inner.Subscribe(topics);

    /// <summary>
    /// Subscribes to a single topic.
    /// </summary>
    public void Subscribe(string topic) => _inner.Subscribe(topic);

    /// <summary>
    /// Consumes the next message with Polly resilience applied.
    /// Transient <see cref="KafkaException"/>s are retried automatically.
    /// </summary>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    /// <returns>
    /// The next <see cref="ConsumeResult{TKey,TValue}"/>, or <c>null</c> if the
    /// timeout elapsed with no message available.
    /// </returns>
    public async Task<ConsumeResult<TKey, TValue>?> ConsumeAsync(
        CancellationToken cancellationToken = default)
    {
        return await _pipeline.ExecuteAsync(ct =>
        {
            try
            {
                // IConsumer.Consume is synchronous; run on thread pool to avoid blocking.
                var result = _inner.Consume(ct);
                return ValueTask.FromResult<ConsumeResult<TKey, TValue>?>(result);
            }
            catch (KafkaException ex) when (_options.TransientErrorCodes.Contains(ex.Error.Code))
            {
                throw new TransientKafkaException(ex);
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Commits offsets for the current partition assignment.
    /// </summary>
    public void Commit() => _inner.Commit();

    /// <summary>
    /// Commits the offset of a specific consume result.
    /// </summary>
    public void Commit(ConsumeResult<TKey, TValue> result) => _inner.Commit(result);

    /// <inheritdoc />
    public void Dispose()
    {
        _inner.Close();
        _inner.Dispose();
    }
}

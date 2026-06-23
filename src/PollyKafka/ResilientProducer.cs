namespace PollyKafka;

/// <summary>
/// A resilient Kafka producer that wraps <see cref="IProducer{TKey,TValue}"/>
/// with a Polly v8 pipeline: retry → circuit breaker → timeout.
/// </summary>
public sealed class ResilientProducer<TKey, TValue> : IDisposable
{
    private readonly IProducer<TKey, TValue> _inner;
    private readonly ResiliencePipeline _pipeline;
    private readonly PollyKafkaOptions _options;

    /// <summary>
    /// Initialises the resilient producer.
    /// </summary>
    public ResilientProducer(IProducer<TKey, TValue> inner, PollyKafkaOptions options)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(options);
        _inner = inner;
        _options = options;
        _pipeline = PipelineBuilder.Build(options);
    }

    /// <summary>
    /// Produces a message to Kafka with Polly resilience applied.
    /// Transient <see cref="KafkaException"/>s are retried automatically.
    /// </summary>
    /// <param name="topic">Target topic.</param>
    /// <param name="message">The message to produce.</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    /// <returns>The <see cref="DeliveryResult{TKey,TValue}"/> from the broker.</returns>
    public async Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        string topic,
        Message<TKey, TValue> message,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(message);

        return await _pipeline.ExecuteAsync(async ct =>
        {
            try
            {
                var task = _inner.ProduceAsync(topic, message, ct);
                return await task.WaitAsync(ct).ConfigureAwait(false);
            }
            catch (KafkaException ex) when (_options.TransientErrorCodes.Contains(ex.Error.Code))
            {
                throw new TransientKafkaException(ex);
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Flushes outstanding produce requests to the broker.
    /// </summary>
    public void Flush(TimeSpan timeout) => _inner.Flush(timeout);

    /// <inheritdoc />
    public void Dispose() => _inner.Dispose();
}

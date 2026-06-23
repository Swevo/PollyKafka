namespace PollyKafka;

/// <summary>
/// Thrown when a <see cref="KafkaException"/> with a transient
/// <see cref="ErrorCode"/> is caught, allowing Polly to identify it
/// without a dependency on Confluent.Kafka inside the pipeline predicate.
/// </summary>
public sealed class TransientKafkaException : Exception
{
    /// <summary>The original <see cref="KafkaException"/>.</summary>
    public KafkaException KafkaException { get; }

    /// <summary>The Kafka error code.</summary>
    public ErrorCode ErrorCode => KafkaException.Error.Code;

    /// <inheritdoc />
    public TransientKafkaException(KafkaException inner)
        : base(inner.Message, inner)
    {
        ArgumentNullException.ThrowIfNull(inner);
        KafkaException = inner;
    }
}

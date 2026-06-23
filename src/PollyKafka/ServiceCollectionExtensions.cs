namespace PollyKafka;

/// <summary>
/// Extension methods for registering resilient Kafka producers and consumers
/// with <see cref="IServiceCollection"/>.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers a <see cref="ResilientProducer{TKey,TValue}"/> as a singleton,
    /// wrapping a <see cref="IProducer{TKey,TValue}"/> built from the supplied config.
    /// </summary>
    /// <typeparam name="TKey">The message key type.</typeparam>
    /// <typeparam name="TValue">The message value type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="producerConfig">Confluent.Kafka producer configuration.</param>
    /// <param name="configure">Optional delegate to customise resilience options.</param>
    public static IServiceCollection AddResilientKafkaProducer<TKey, TValue>(
        this IServiceCollection services,
        ProducerConfig producerConfig,
        Action<PollyKafkaOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(producerConfig);

        return services.AddSingleton(sp =>
        {
            var options = new PollyKafkaOptions();
            configure?.Invoke(options);
            var inner = new ProducerBuilder<TKey, TValue>(producerConfig).Build();
            return new ResilientProducer<TKey, TValue>(inner, options);
        });
    }

    /// <summary>
    /// Registers a <see cref="ResilientConsumer{TKey,TValue}"/> as a singleton,
    /// wrapping a <see cref="IConsumer{TKey,TValue}"/> built from the supplied config.
    /// </summary>
    /// <typeparam name="TKey">The message key type.</typeparam>
    /// <typeparam name="TValue">The message value type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="consumerConfig">Confluent.Kafka consumer configuration.</param>
    /// <param name="configure">Optional delegate to customise resilience options.</param>
    public static IServiceCollection AddResilientKafkaConsumer<TKey, TValue>(
        this IServiceCollection services,
        ConsumerConfig consumerConfig,
        Action<PollyKafkaOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(consumerConfig);

        return services.AddSingleton(sp =>
        {
            var options = new PollyKafkaOptions();
            configure?.Invoke(options);
            var inner = new ConsumerBuilder<TKey, TValue>(consumerConfig).Build();
            return new ResilientConsumer<TKey, TValue>(inner, options);
        });
    }
}

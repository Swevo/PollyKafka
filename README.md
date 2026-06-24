# PollyKafka

[![NuGet](https://img.shields.io/nuget/v/PollyKafka.svg)](https://www.nuget.org/packages/PollyKafka)
[![NuGet Downloads](https://img.shields.io/nuget/dt/PollyKafka.svg)](https://www.nuget.org/packages/PollyKafka)
[![Build](https://github.com/Swevo/PollyKafka/actions/workflows/build.yml/badge.svg)](https://github.com/Swevo/PollyKafka/actions/workflows/build.yml)

**Polly v8 resilience for Confluent.Kafka** — automatic retry, circuit breaker, and per-operation timeout for Kafka producers and consumers. Drop-in wrappers, no custom serialisers required.

## Why PollyKafka?

Kafka brokers fail transiently: leader elections, broker restarts, network blips. Without resilience, a single `BrokerNotAvailable` or `RequestTimedOut` error can crash your producer or leave your consumer in a broken state. PollyKafka wraps every produce and consume operation in a Polly v8 pipeline so transient failures are retried automatically, persistent broker failures trip the circuit breaker, and hanging operations are cancelled by a configurable timeout.

| Feature | Raw Confluent.Kafka | PollyKafka |
|---------|:---:|:---:|
| Automatic retry | ❌ | ✅ |
| Circuit breaker | ❌ | ✅ |
| Per-operation timeout | ❌ | ✅ |
| Configurable transient error codes | ❌ | ✅ |
| DI registration | ❌ | ✅ |
| Targets net8 + net9 | ✅ | ✅ |

## Installation

```bash
dotnet add package PollyKafka
```

## Quick Start

### Producer

```csharp
// Manual construction
var producerConfig = new ProducerConfig { BootstrapServers = "localhost:9092" };
var inner = new ProducerBuilder<string, string>(producerConfig).Build();
var producer = new ResilientProducer<string, string>(inner, new PollyKafkaOptions
{
    MaxRetries  = 3,
    BaseDelay   = TimeSpan.FromMilliseconds(200),
});

var result = await producer.ProduceAsync("my-topic", new Message<string, string>
{
    Key   = "order-id",
    Value = System.Text.Json.JsonSerializer.Serialize(order),
});
Console.WriteLine($"Delivered to {result.TopicPartitionOffset}");
```

### Consumer

```csharp
var consumerConfig = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId          = "my-consumer-group",
    AutoOffsetReset  = AutoOffsetReset.Earliest,
};
var inner    = new ConsumerBuilder<string, string>(consumerConfig).Build();
var consumer = new ResilientConsumer<string, string>(inner, new PollyKafkaOptions());

consumer.Subscribe("my-topic");

while (!stoppingToken.IsCancellationRequested)
{
    var result = await consumer.ConsumeAsync(stoppingToken);
    if (result is null) continue; // timeout — no message available

    // process result.Message.Value ...
    consumer.Commit(result);
}
```

### With Dependency Injection

```csharp
// Program.cs
builder.Services.AddResilientKafkaProducer<string, string>(
    new ProducerConfig { BootstrapServers = "localhost:9092" },
    o =>
    {
        o.MaxRetries      = 3;
        o.OperationTimeout = TimeSpan.FromSeconds(15);
    });

builder.Services.AddResilientKafkaConsumer<string, string>(
    new ConsumerConfig
    {
        BootstrapServers = "localhost:9092",
        GroupId          = "orders-consumer",
    });

// Inject ResilientProducer<string, string> / ResilientConsumer<string, string>
```

## Configuration

```csharp
var options = new PollyKafkaOptions
{
    // Retry
    MaxRetries = 3,                              // 0 = no retry
    BaseDelay  = TimeSpan.FromMilliseconds(200), // exponential base
    MaxDelay   = TimeSpan.FromSeconds(30),

    // Circuit breaker
    CircuitBreakerFailureRatio      = 0.5,
    CircuitBreakerMinimumThroughput = 10,
    CircuitBreakerSamplingDuration  = TimeSpan.FromSeconds(30),
    CircuitBreakerBreakDuration     = TimeSpan.FromSeconds(5),

    // Timeout
    OperationTimeout = TimeSpan.FromSeconds(10),

    // Which Kafka error codes trigger retry/CB
    TransientErrorCodes = new HashSet<ErrorCode>
    {
        ErrorCode.BrokerNotAvailable,
        ErrorCode.LeaderNotAvailable,
        ErrorCode.NotLeaderForPartition,
        ErrorCode.RequestTimedOut,
        ErrorCode.NetworkException,
        ErrorCode.KafkaStorageError,
        ErrorCode.Local_AllBrokersDown,
        ErrorCode.Local_TimedOut,
        ErrorCode.Local_Transport,
        ErrorCode.Local_MsgTimedOut,
    },
};
```

| Property | Default | Description |
|---|---|---|
| `MaxRetries` | `3` | Retry attempts (0 = disabled) |
| `BaseDelay` | `200 ms` | Base delay for exponential back-off with jitter |
| `MaxDelay` | `30 s` | Cap for exponential back-off delay |
| `CircuitBreakerFailureRatio` | `0.5` | Failure ratio to open circuit |
| `CircuitBreakerMinimumThroughput` | `10` | Minimum calls before CB can open |
| `CircuitBreakerSamplingDuration` | `30 s` | Sliding window for failure ratio |
| `CircuitBreakerBreakDuration` | `5 s` | How long the circuit stays open |
| `OperationTimeout` | `10 s` | Max time per produce/consume before `TimeoutRejectedException` |
| `TransientErrorCodes` | see above | `ErrorCode` set that triggers retry/CB |

## Error Handling

Non-transient `KafkaException`s (e.g. `TopicAuthorizationFailed`) are rethrown as-is. After retries are exhausted the last exception is a `TransientKafkaException` wrapping the original:

```csharp
try
{
    await producer.ProduceAsync("my-topic", message);
}
catch (TransientKafkaException ex)
{
    // All retries failed
    Console.WriteLine($"Kafka error: {ex.ErrorCode} — {ex.KafkaException.Message}");
}
catch (BrokenCircuitException)
{
    // Circuit is open — fail fast
}
catch (TimeoutRejectedException)
{
    // Operation exceeded OperationTimeout
}
```

## Resilience Pipeline Order

Operations flow through the pipeline in this order:

```
Retry → Circuit Breaker → Timeout → Kafka operation
```

## Related Packages

| Package | Description |
|---|---|
| [PollyElasticsearch](https://github.com/Swevo/PollyElasticsearch) | Polly v8 for Elastic.Clients.Elasticsearch |
| [PollyAzureKeyVault](https://github.com/Swevo/PollyAzureKeyVault) | Polly v8 for Azure Key Vault |
| [PollySendGrid](https://github.com/Swevo/PollySendGrid) | Polly v8 for SendGrid |
| [PollyMassTransit](https://github.com/Swevo/PollyMassTransit) | Polly v8 for MassTransit |
| [PollyAzureTableStorage](https://github.com/Swevo/PollyAzureTableStorage) | Polly v8 for Azure Table Storage |
| [PollyMailKit](https://github.com/Swevo/PollyMailKit) | MailKit SMTP email client |
| [PollyAzureQueueStorage](https://github.com/Swevo/PollyAzureQueueStorage) | Azure Queue Storage QueueClient |
| [PollyHangfire](https://github.com/Swevo/PollyHangfire) | Hangfire IBackgroundJobClient |
| [PollyBackoff](https://www.nuget.org/packages/PollyBackoff) | Advanced back-off strategies with jitter |
| [PollyChaos](https://www.nuget.org/packages/PollyChaos) | Chaos engineering — inject faults in tests |
| [PollyMediatR](https://www.nuget.org/packages/PollyMediatR) | Polly pipeline behaviour for MediatR |
| [PollyEFCore](https://www.nuget.org/packages/PollyEFCore) | Resilient EF Core execution strategies |
| [PollyHealthChecks](https://www.nuget.org/packages/PollyHealthChecks) | Health check endpoints for Polly circuits |
| [PollyOpenAI](https://www.nuget.org/packages/PollyOpenAI) | Retry + rate-limit handling for OpenAI / Azure OpenAI |
| [PollyRedis](https://www.nuget.org/packages/PollyRedis) | Resilient StackExchange.Redis wrapper |
| [PollySignalR](https://www.nuget.org/packages/PollySignalR) | Reconnect policy for SignalR `HubConnection` |
| [PollyGrpc](https://www.nuget.org/packages/PollyGrpc) | Polly v8 resilience for gRPC .NET clients via Interceptor |
| [PollyCaching](https://www.nuget.org/packages/PollyCaching) | Distributed cache with Polly resilience |
| [PollyBulkhead](https://www.nuget.org/packages/PollyBulkhead) | Bulkhead isolation for concurrent workloads |

| [PollyAzureEventHub](https://github.com/Swevo/PollyAzureEventHub) | Polly v8 for Azure Event Hubs |
| [PollyAzureServiceBus](https://www.nuget.org/packages/PollyAzureServiceBus) | Polly v8 resilience (retry, CB, timeout) for Azure Service Bus senders and receivers |
## 💼 Need .NET consulting?

The author of this package is available for consulting on **Polly v8 resilience**, **Azure cloud architecture**, and **clean .NET design**.

**[→ solidqualitysolutions.com](https://www.solidqualitysolutions.com/)**
## License

MIT

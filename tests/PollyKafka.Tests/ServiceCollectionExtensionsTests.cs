namespace PollyKafka.Tests;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddResilientKafkaProducer_NullServices_Throws()
    {
        IServiceCollection? services = null;
        Action act = () => services!.AddResilientKafkaProducer<string, string>(
            new ProducerConfig { BootstrapServers = "localhost:9092" });
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void AddResilientKafkaProducer_NullConfig_Throws()
    {
        var services = new ServiceCollection();
        Action act = () => services.AddResilientKafkaProducer<string, string>(null!);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void AddResilientKafkaProducer_RegistersSuccessfully()
    {
        var services = new ServiceCollection();
        var result = services.AddResilientKafkaProducer<string, string>(
            new ProducerConfig { BootstrapServers = "localhost:9092" });
        result.Should().NotBeNull();
        services.Should().Contain(d => d.ServiceType == typeof(ResilientProducer<string, string>));
    }

    [Fact]
    public void AddResilientKafkaConsumer_NullServices_Throws()
    {
        IServiceCollection? services = null;
        Action act = () => services!.AddResilientKafkaConsumer<string, string>(
            new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "test" });
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void AddResilientKafkaConsumer_NullConfig_Throws()
    {
        var services = new ServiceCollection();
        Action act = () => services.AddResilientKafkaConsumer<string, string>(null!);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void AddResilientKafkaConsumer_RegistersSuccessfully()
    {
        var services = new ServiceCollection();
        var result = services.AddResilientKafkaConsumer<string, string>(
            new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "test" });
        result.Should().NotBeNull();
        services.Should().Contain(d => d.ServiceType == typeof(ResilientConsumer<string, string>));
    }
}

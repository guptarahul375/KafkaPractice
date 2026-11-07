using Confluent.Kafka;
using KafkaPractice.Api.Configuration;

namespace KafkaPractice.Api.Kafka;

public sealed record ConsumedKafkaMessage(
    string Topic,
    int Partition,
    long Offset,
    string? Key,
    string Value);

public interface IKafkaConsumerClient : IDisposable
{
    void Subscribe(string topic);
    ConsumedKafkaMessage? Consume(CancellationToken cancellationToken);
    void Commit(ConsumedKafkaMessage message);
}

public interface IKafkaConsumerClientFactory
{
    IKafkaConsumerClient Create(KafkaOptions options);
}

public sealed class ConfluentKafkaConsumerClientFactory : IKafkaConsumerClientFactory
{
    private readonly Func<ConsumerConfig, IConsumer<string, string>> _consumerBuilder;

    public ConfluentKafkaConsumerClientFactory(
        Func<ConsumerConfig, IConsumer<string, string>>? consumerBuilder = null)
    {
        _consumerBuilder = consumerBuilder ?? BuildConsumer;
    }

    public IKafkaConsumerClient Create(KafkaOptions options)
    {
        var consumer = _consumerBuilder(new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.ConsumerGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        });

        return new ConfluentKafkaConsumerClient(consumer);
    }

    private static IConsumer<string, string> BuildConsumer(ConsumerConfig config)
    {
        return new ConsumerBuilder<string, string>(config).Build();
    }
}

public sealed class ConfluentKafkaConsumerClient : IKafkaConsumerClient
{
    private readonly IConsumer<string, string> _consumer;

    public ConfluentKafkaConsumerClient(IConsumer<string, string> consumer)
    {
        _consumer = consumer;
    }

    public void Subscribe(string topic) => _consumer.Subscribe(topic);

    public ConsumedKafkaMessage? Consume(CancellationToken cancellationToken)
    {
        var result = _consumer.Consume(cancellationToken);
        if (result is null)
        {
            return null;
        }

        return new ConsumedKafkaMessage(
            result.Topic,
            result.Partition.Value,
            result.Offset.Value,
            result.Message.Key,
            result.Message.Value);
    }

    public void Commit(ConsumedKafkaMessage message)
    {
        _consumer.Commit(new[]
        {
            new TopicPartitionOffset(
                message.Topic,
                new Partition(message.Partition),
                new Offset(message.Offset))
        });
    }

    public void Dispose() => _consumer.Dispose();
}

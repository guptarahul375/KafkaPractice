using Confluent.Kafka;
using KafkaPractice.Api.Configuration;
using Microsoft.Extensions.Options;

namespace KafkaPractice.Api.Kafka;

public sealed record PublishedMessage(string Topic, int Partition, long Offset, string Key);

public interface IMessageProducer
{
    Task<PublishedMessage> PublishAsync(
        string value,
        string? key,
        CancellationToken cancellationToken);
}

public sealed class KafkaProducerService : IMessageProducer, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly string _topic;

    public KafkaProducerService(IOptions<KafkaOptions> options)
        : this(options, CreateProducer(options.Value))
    {
    }

    public KafkaProducerService(
        IOptions<KafkaOptions> options,
        IProducer<string, string> producer)
    {
        var kafkaOptions = options.Value;
        _topic = kafkaOptions.Topic;
        _producer = producer;
    }

    public async Task<PublishedMessage> PublishAsync(
        string value,
        string? key,
        CancellationToken cancellationToken)
    {
        var messageKey = string.IsNullOrWhiteSpace(key) ? Guid.NewGuid().ToString("N") : key;
        var result = await _producer.ProduceAsync(_topic, new Message<string, string>
        {
            Key = messageKey,
            Value = value
        }, cancellationToken);

        return new PublishedMessage(
            result.Topic,
            result.Partition.Value,
            result.Offset.Value,
            result.Message.Key);
    }

    public void Dispose()
    {
        _producer.Dispose();
    }

    private static IProducer<string, string> CreateProducer(KafkaOptions options)
    {
        return new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers
        }).Build();
    }
}

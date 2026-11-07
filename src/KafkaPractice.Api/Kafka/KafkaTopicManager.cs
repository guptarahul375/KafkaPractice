using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaPractice.Api.Configuration;
using Microsoft.Extensions.Options;

namespace KafkaPractice.Api.Kafka;

public interface IKafkaTopicManager
{
    Task EnsureTopicExistsAsync(CancellationToken cancellationToken);
}

public sealed class KafkaTopicManager : IKafkaTopicManager
{
    private readonly IAdminClient _adminClient;
    private readonly KafkaOptions _options;
    private readonly ILogger<KafkaTopicManager> _logger;

    public KafkaTopicManager(
        IAdminClient adminClient,
        IOptions<KafkaOptions> options,
        ILogger<KafkaTopicManager> logger)
    {
        _adminClient = adminClient;
        _options = options.Value;
        _logger = logger;
    }

    public async Task EnsureTopicExistsAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _adminClient.CreateTopicsAsync(
                new[]
                {
                    new TopicSpecification
                    {
                        Name = _options.Topic,
                        NumPartitions = 1,
                        ReplicationFactor = 1
                    }
                },
                new CreateTopicsOptions { OperationTimeout = TimeSpan.FromSeconds(15) });
            _logger.LogInformation("Created Kafka topic {Topic}", _options.Topic);
        }
        catch (CreateTopicsException exception) when (
            exception.Results.All(result => result.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            _logger.LogInformation("Kafka topic {Topic} already exists", _options.Topic);
        }
    }
}

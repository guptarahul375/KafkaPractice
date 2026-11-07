using System.Text.Json;
using KafkaPractice.Api.Configuration;
using Microsoft.Extensions.Options;

namespace KafkaPractice.Api.Kafka;

public sealed class KafkaMessageConsumer : BackgroundService
{
    private readonly KafkaOptions _options;
    private readonly ILogger<KafkaMessageConsumer> _logger;
    private readonly string _outputDirectory;
    private readonly IKafkaTopicManager _topicManager;
    private readonly IKafkaConsumerClientFactory _consumerFactory;
    private readonly IMessageFileWriter _fileWriter;

    public KafkaMessageConsumer(
        IOptions<KafkaOptions> options,
        IHostEnvironment environment,
        ILogger<KafkaMessageConsumer> logger,
        IKafkaTopicManager topicManager,
        IKafkaConsumerClientFactory consumerFactory,
        IMessageFileWriter fileWriter)
    {
        _options = options.Value;
        _logger = logger;
        _topicManager = topicManager;
        _consumerFactory = consumerFactory;
        _fileWriter = fileWriter;

        var contentRoot = Path.GetFullPath(environment.ContentRootPath);
        _outputDirectory = Path.GetFullPath(Path.Combine(contentRoot, _options.MessageOutputDirectory));
        var relativeOutputPath = Path.GetRelativePath(contentRoot, _outputDirectory);
        if (Path.IsPathRooted(relativeOutputPath)
            || relativeOutputPath == ".."
            || relativeOutputPath.StartsWith($"..{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "Kafka:MessageOutputDirectory must be inside the application directory.");
        }
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await _topicManager.EnsureTopicExistsAsync(cancellationToken);
        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Directory.CreateDirectory(_outputDirectory);

        using var consumer = _consumerFactory.Create(_options);
        consumer.Subscribe(_options.Topic);
        _logger.LogInformation("Consuming messages from Kafka topic {Topic}", _options.Topic);

        while (!stoppingToken.IsCancellationRequested)
        {
            var result = consumer.Consume(stoppingToken);
            if (result is null)
            {
                continue;
            }

            var receivedAt = DateTimeOffset.UtcNow;
            var message = new SavedKafkaMessage(
                receivedAt,
                result.Key,
                result.Value,
                result.Topic,
                result.Partition,
                result.Offset);
            var filePath = Path.Combine(_outputDirectory, $"{receivedAt:yyyy-MM-dd}.txt");
            var line = JsonSerializer.Serialize(message) + Environment.NewLine;

            await _fileWriter.AppendAsync(filePath, line, stoppingToken);
            consumer.Commit(result);
            _logger.LogInformation(
                "Saved Kafka message from {TopicPartitionOffset} to {FilePath}",
                $"{result.Topic} [[{result.Partition}]] @{result.Offset}",
                filePath);
        }
    }

    private sealed record SavedKafkaMessage(
        DateTimeOffset ReceivedAtUtc,
        string? Key,
        string Value,
        string Topic,
        int Partition,
        long Offset);
}

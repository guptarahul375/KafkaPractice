using System.Text.Json;
using KafkaPractice.Api.Configuration;
using KafkaPractice.Api.Kafka;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KafkaPractice.Tests.Kafka;

public sealed class KafkaMessageConsumerTests
{
    [Fact]
    public async Task StartAsync_CreatesTopicConsumesAndPersistsMessagesBeforeCommitting()
    {
        var root = CreateTemporaryDirectory();
        var topicManager = new RecordingTopicManager();
        var message = new ConsumedKafkaMessage("test-topic", 1, 42, "key", "hello");
        var consumer = new RecordingConsumer(null, message);
        var writer = new BlockingFileWriter();
        var service = CreateConsumer(root, "KafkaMessages", topicManager, consumer, writer);

        try
        {
            await service.StartAsync(CancellationToken.None);
            var captured = await writer.Captured.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Empty(consumer.Committed);

            writer.Release.TrySetResult();
            var saved = await writer.Completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await service.StopAsync(CancellationToken.None);

            Assert.True(topicManager.WasCalled);
            Assert.Equal("test-topic", consumer.SubscribedTopic);
            Assert.Same(message, Assert.Single(consumer.Committed));
            Assert.Equal(Path.Combine(root, "KafkaMessages", $"{DateTimeOffset.UtcNow:yyyy-MM-dd}.txt"), saved.Path);

            var json = JsonSerializer.Deserialize<SavedMessage>(saved.Content.Trim());
            Assert.NotNull(json);
            Assert.Equal("hello", json.Value);
            Assert.Equal("key", json.Key);
            Assert.Equal("test-topic", json.Topic);
            Assert.Equal(1, json.Partition);
            Assert.Equal(42, json.Offset);
        }
        finally
        {
            writer.Release.TrySetResult();
            await service.StopAsync(CancellationToken.None);
            Directory.Delete(root, recursive: true);
        }
    }

    [Theory]
    [InlineData("..")]
    [InlineData("..\\outside")]
    public void Constructor_RejectsOutputPathOutsideContentRoot(string outputPath)
    {
        var root = CreateTemporaryDirectory();

        try
        {
            var exception = Assert.Throws<InvalidOperationException>(() =>
                CreateConsumer(
                    root,
                    outputPath,
                    new RecordingTopicManager(),
                    new RecordingConsumer(),
                    new BlockingFileWriter()));

            Assert.Contains("inside the application directory", exception.Message);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    private static KafkaMessageConsumer CreateConsumer(
        string root,
        string outputDirectory,
        RecordingTopicManager topicManager,
        RecordingConsumer consumer,
        BlockingFileWriter writer)
    {
        var options = Options.Create(new KafkaOptions
        {
            Topic = "test-topic",
            MessageOutputDirectory = outputDirectory
        });
        var environment = new TestHostEnvironment(root);

        return new KafkaMessageConsumer(
            options,
            environment,
            NullLogger<KafkaMessageConsumer>.Instance,
            topicManager,
            new RecordingConsumerFactory(consumer),
            writer);
    }

    private static string CreateTemporaryDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private sealed class RecordingTopicManager : IKafkaTopicManager
    {
        public bool WasCalled { get; private set; }

        public Task EnsureTopicExistsAsync(CancellationToken cancellationToken)
        {
            WasCalled = true;
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingConsumerFactory(RecordingConsumer consumer) : IKafkaConsumerClientFactory
    {
        public IKafkaConsumerClient Create(KafkaOptions options) => consumer;
    }

    private sealed class RecordingConsumer(params ConsumedKafkaMessage?[] messages) : IKafkaConsumerClient
    {
        private readonly Queue<ConsumedKafkaMessage?> _messages = new(messages);

        public string? SubscribedTopic { get; private set; }
        public List<ConsumedKafkaMessage> Committed { get; } = [];

        public void Subscribe(string topic) => SubscribedTopic = topic;

        public ConsumedKafkaMessage? Consume(CancellationToken cancellationToken)
        {
            if (_messages.TryDequeue(out var message))
            {
                return message;
            }

            cancellationToken.WaitHandle.WaitOne(TimeSpan.FromMilliseconds(10));
            return null;
        }

        public void Commit(ConsumedKafkaMessage message) => Committed.Add(message);

        public void Dispose()
        {
        }
    }

    private sealed class BlockingFileWriter : IMessageFileWriter
    {
        public TaskCompletionSource<(string Path, string Content)> Captured { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<(string Path, string Content)> Completed { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task AppendAsync(string path, string content, CancellationToken cancellationToken)
        {
            Captured.TrySetResult((path, content));
            await Release.Task.WaitAsync(cancellationToken);
            await new MessageFileWriter().AppendAsync(path, content, cancellationToken);
            Completed.TrySetResult((path, content));
        }
    }

    private sealed record SavedMessage(
        DateTimeOffset ReceivedAtUtc,
        string? Key,
        string Value,
        string Topic,
        int Partition,
        long Offset);

    private sealed class TestHostEnvironment(string contentRoot) : IHostEnvironment
    {
        public string EnvironmentName { get; set; } = Environments.Development;
        public string ApplicationName { get; set; } = "KafkaPractice.Tests";
        public string ContentRootPath { get; set; } = contentRoot;
        public IFileProvider ContentRootFileProvider { get; set; } = new NullFileProvider();
    }
}

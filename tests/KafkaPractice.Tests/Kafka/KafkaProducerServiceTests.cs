using Confluent.Kafka;
using KafkaPractice.Api.Configuration;
using KafkaPractice.Api.Kafka;
using Microsoft.Extensions.Options;
using Moq;

namespace KafkaPractice.Tests.Kafka;

public sealed class KafkaProducerServiceTests
{
    [Fact]
    public async Task PublishAsync_UsesProvidedKeyAndReturnsBrokerDeliveryDetails()
    {
        var producer = CreateProducerMock();
        var sentMessage = default(Message<string, string>);
        producer
            .Setup(client => client.ProduceAsync(
                "test-topic",
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, Message<string, string>, CancellationToken>(
                (_, message, _) => sentMessage = message)
            .Returns((string _, Message<string, string> message, CancellationToken _) =>
                Task.FromResult(CreateDeliveryResult(message)));

        using var service = new KafkaProducerService(
            Options.Create(new KafkaOptions { Topic = "test-topic" }),
            producer.Object);

        var result = await service.PublishAsync("payload", "explicit-key", CancellationToken.None);

        Assert.Equal(new PublishedMessage("test-topic", 2, 17, "explicit-key"), result);
        Assert.NotNull(sentMessage);
        Assert.Equal("payload", sentMessage.Value);
        Assert.Equal("explicit-key", sentMessage.Key);
        producer.Verify(client => client.ProduceAsync(
            "test-topic",
            It.IsAny<Message<string, string>>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task PublishAsync_GeneratesKeyWhenKeyIsMissingOrWhitespace(string? key)
    {
        Message<string, string>? sentMessage = null;
        var producer = CreateProducerMock();
        producer
            .Setup(client => client.ProduceAsync(
                "test-topic",
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, Message<string, string>, CancellationToken>(
                (_, message, _) => sentMessage = message)
            .Returns((string _, Message<string, string> message, CancellationToken _) =>
                Task.FromResult(CreateDeliveryResult(message)));

        using var service = new KafkaProducerService(
            Options.Create(new KafkaOptions { Topic = "test-topic" }),
            producer.Object);

        var result = await service.PublishAsync("payload", key, CancellationToken.None);

        Assert.NotNull(sentMessage);
        Assert.False(string.IsNullOrWhiteSpace(sentMessage.Key));
        Assert.Equal(32, sentMessage.Key.Length);
        Assert.Equal(sentMessage.Key, result.Key);
    }

    [Fact]
    public void Dispose_DisposesUnderlyingProducer()
    {
        var producer = CreateProducerMock();
        var service = new KafkaProducerService(
            Options.Create(new KafkaOptions()),
            producer.Object);

        service.Dispose();

        producer.Verify(client => client.Dispose(), Times.Once);
    }

    [Fact]
    public void Constructor_CreatesConfluentProducerFromOptions()
    {
        using var service = new KafkaProducerService(
            Options.Create(new KafkaOptions { BootstrapServers = "localhost:9092" }));

        Assert.NotNull(service);
    }

    private static Mock<IProducer<string, string>> CreateProducerMock()
    {
        return new Mock<IProducer<string, string>>();
    }

    private static DeliveryResult<string, string> CreateDeliveryResult(Message<string, string> message)
    {
        return new DeliveryResult<string, string>
        {
            Topic = "test-topic",
            Partition = new Partition(2),
            Offset = new Offset(17),
            Message = message
        };
    }
}

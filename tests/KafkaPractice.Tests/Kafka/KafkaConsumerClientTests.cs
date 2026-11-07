using Confluent.Kafka;
using KafkaPractice.Api.Configuration;
using KafkaPractice.Api.Kafka;
using Moq;

namespace KafkaPractice.Tests.Kafka;

public sealed class KafkaConsumerClientTests
{
    [Fact]
    public void Client_DelegatesSubscribeConsumeCommitAndDispose()
    {
        var result = new ConsumeResult<string, string>
        {
            Topic = "test-topic",
            Partition = new Partition(2),
            Offset = new Offset(9),
            Message = new Message<string, string> { Key = "key", Value = "payload" }
        };
        var consumer = new Mock<IConsumer<string, string>>();
        consumer.SetupSequence(client => client.Consume(It.IsAny<CancellationToken>()))
            .Returns((ConsumeResult<string, string>)null!)
            .Returns(result);
        consumer.Setup(client => client.Commit(It.IsAny<IEnumerable<TopicPartitionOffset>>()));
        var client = new ConfluentKafkaConsumerClient(consumer.Object);

        client.Subscribe("test-topic");
        Assert.Null(client.Consume(CancellationToken.None));
        var message = client.Consume(CancellationToken.None);

        Assert.Equal(new ConsumedKafkaMessage("test-topic", 2, 9, "key", "payload"), message);
        client.Commit(message!);
        client.Dispose();

        consumer.Verify(value => value.Subscribe("test-topic"), Times.Once);
        consumer.Verify(value => value.Commit(It.Is<IEnumerable<TopicPartitionOffset>>(offsets =>
            offsets.Single().Topic == "test-topic"
            && offsets.Single().Partition.Value == 2
            && offsets.Single().Offset.Value == 9)), Times.Once);
        consumer.Verify(value => value.Dispose(), Times.Once);
    }

    [Fact]
    public void Factory_CreatesConsumerWithConfiguredSettings()
    {
        ConsumerConfig? captured = null;
        var consumer = new Mock<IConsumer<string, string>>();
        var factory = new ConfluentKafkaConsumerClientFactory(config =>
        {
            captured = config;
            return consumer.Object;
        });

        var result = factory.Create(new KafkaOptions
        {
            BootstrapServers = "broker:9093",
            ConsumerGroupId = "test-group"
        });

        Assert.IsType<ConfluentKafkaConsumerClient>(result);
        Assert.Equal("broker:9093", captured!.BootstrapServers);
        Assert.Equal("test-group", captured.GroupId);
        Assert.Equal(AutoOffsetReset.Earliest, captured.AutoOffsetReset);
        Assert.False(captured.EnableAutoCommit);
    }

    [Fact]
    public void Factory_CreatesConfluentConsumerByDefault()
    {
        using var consumer = new ConfluentKafkaConsumerClientFactory().Create(new KafkaOptions());

        Assert.IsType<ConfluentKafkaConsumerClient>(consumer);
    }
}

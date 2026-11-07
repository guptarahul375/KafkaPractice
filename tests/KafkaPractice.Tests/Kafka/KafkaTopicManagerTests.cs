using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaPractice.Api.Configuration;
using KafkaPractice.Api.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace KafkaPractice.Tests.Kafka;

public sealed class KafkaTopicManagerTests
{
    [Fact]
    public async Task EnsureTopicExistsAsync_CreatesConfiguredTopic()
    {
        var admin = new Mock<IAdminClient>();
        admin.Setup(client => client.CreateTopicsAsync(
                It.IsAny<IEnumerable<TopicSpecification>>(),
                It.IsAny<CreateTopicsOptions>()))
            .Returns(Task.CompletedTask);
        var manager = CreateManager(admin.Object);

        await manager.EnsureTopicExistsAsync(CancellationToken.None);

        admin.Verify(client => client.CreateTopicsAsync(
            It.Is<IEnumerable<TopicSpecification>>(topics =>
                topics.Single().Name == "test-topic"
                && topics.Single().NumPartitions == 1
                && topics.Single().ReplicationFactor == 1),
            It.IsAny<CreateTopicsOptions>()), Times.Once);
    }

    [Fact]
    public async Task EnsureTopicExistsAsync_IgnoresAlreadyExistingTopic()
    {
        var admin = new Mock<IAdminClient>();
        admin.Setup(client => client.CreateTopicsAsync(
                It.IsAny<IEnumerable<TopicSpecification>>(),
                It.IsAny<CreateTopicsOptions>()))
            .ThrowsAsync(new CreateTopicsException(new List<CreateTopicReport>
            {
                new()
                {
                    Topic = "test-topic",
                    Error = new Error(ErrorCode.TopicAlreadyExists)
                }
            }));

        await CreateManager(admin.Object).EnsureTopicExistsAsync(CancellationToken.None);
    }

    [Fact]
    public async Task EnsureTopicExistsAsync_PropagatesOtherKafkaErrors()
    {
        var error = new CreateTopicsException(new List<CreateTopicReport>
        {
            new()
            {
                Topic = "test-topic",
                Error = new Error(ErrorCode.TopicAuthorizationFailed)
            }
        });
        var admin = new Mock<IAdminClient>();
        admin.Setup(client => client.CreateTopicsAsync(
                It.IsAny<IEnumerable<TopicSpecification>>(),
                It.IsAny<CreateTopicsOptions>()))
            .ThrowsAsync(error);

        var actual = await Assert.ThrowsAsync<CreateTopicsException>(
            () => CreateManager(admin.Object).EnsureTopicExistsAsync(CancellationToken.None));

        Assert.Same(error, actual);
    }

    private static KafkaTopicManager CreateManager(IAdminClient adminClient)
    {
        return new KafkaTopicManager(
            adminClient,
            Options.Create(new KafkaOptions { Topic = "test-topic" }),
            NullLogger<KafkaTopicManager>.Instance);
    }
}

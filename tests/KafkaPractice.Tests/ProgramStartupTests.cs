using System.Net;
using System.Net.Http.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaPractice.Api.Configuration;
using KafkaPractice.Api.Endpoints;
using KafkaPractice.Api.Kafka;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace KafkaPractice.Tests;

public sealed class ProgramStartupTests
{
    [Fact]
    public async Task DevelopmentEnvironment_MapsOpenApiAndPublishesMessages()
    {
        var producer = new RecordingProducer();
        using var factory = CreateFactory(Environments.Development, producer);
        using var client = factory.CreateClient();

        var openApiResponse = await client.GetAsync("/openapi/v1.json");
        var messageResponse = await client.PostAsJsonAsync(
            "/messages",
            new PublishMessageRequest("program test", "program-key"));

        Assert.Equal(HttpStatusCode.OK, openApiResponse.StatusCode);
        Assert.Equal(HttpStatusCode.Accepted, messageResponse.StatusCode);
        Assert.Equal("program test", producer.Value);
        Assert.Equal("program-key", producer.Key);
    }

    [Fact]
    public async Task ProductionEnvironment_DoesNotMapOpenApi()
    {
        using var factory = CreateFactory(Environments.Production, new RecordingProducer());
        using var client = factory.CreateClient();

        var response = await client.GetAsync("/openapi/v1.json");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Theory]
    [InlineData("Kafka:BootstrapServers", " ")]
    [InlineData("Kafka:Topic", "")]
    [InlineData("Kafka:ConsumerGroupId", "  ")]
    public void InvalidRequiredKafkaSetting_FailsApplicationStartup(string key, string value)
    {
        using var factory = CreateFactory(
            Environments.Production,
            new RecordingProducer(),
            new Dictionary<string, string?> { [key] = value });

        Assert.Throws<OptionsValidationException>(() => factory.CreateClient());
    }

    private static WebApplicationFactory<Program> CreateFactory(
        string environment,
        RecordingProducer producer,
        Dictionary<string, string?>? configuration = null)
    {
        return new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.UseEnvironment(environment);
            if (configuration is not null)
            {
                builder.ConfigureAppConfiguration((_, config) =>
                    config.AddInMemoryCollection(configuration));
            }

            builder.ConfigureTestServices(services =>
            {
                services.RemoveAll<IMessageProducer>();
                services.AddSingleton<IMessageProducer>(producer);
                services.RemoveAll<IKafkaTopicManager>();
                services.AddSingleton<IKafkaTopicManager, ResolvingAdminClientTopicManager>();
                services.RemoveAll<IKafkaConsumerClientFactory>();
                services.AddSingleton<IKafkaConsumerClientFactory, WaitingConsumerFactory>();
                services.RemoveAll<IMessageFileWriter>();
                services.AddSingleton<IMessageFileWriter, WaitingFileWriter>();
            });
        });
    }

    private sealed class RecordingProducer : IMessageProducer
    {
        public string? Value { get; private set; }
        public string? Key { get; private set; }

        public Task<PublishedMessage> PublishAsync(
            string value,
            string? key,
            CancellationToken cancellationToken)
        {
            Value = value;
            Key = key;
            return Task.FromResult(new PublishedMessage("practice-messages", 0, 0, key ?? ""));
        }
    }

    private sealed class ResolvingAdminClientTopicManager(IServiceProvider services) : IKafkaTopicManager
    {
        public Task EnsureTopicExistsAsync(CancellationToken cancellationToken)
        {
            _ = services.GetRequiredService<IAdminClient>();
            return Task.CompletedTask;
        }
    }

    private sealed class WaitingConsumerFactory : IKafkaConsumerClientFactory
    {
        public IKafkaConsumerClient Create(KafkaOptions options) => new WaitingConsumer();
    }

    private sealed class WaitingConsumer : IKafkaConsumerClient
    {
        private int _consumed;

        public void Subscribe(string topic)
        {
        }

        public ConsumedKafkaMessage? Consume(CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _consumed) == 1)
            {
                return new ConsumedKafkaMessage("test", 0, 0, "key", "value");
            }

            cancellationToken.WaitHandle.WaitOne();
            return null;
        }

        public void Commit(ConsumedKafkaMessage message)
        {
        }

        public void Dispose()
        {
        }
    }

    private sealed class WaitingFileWriter : IMessageFileWriter
    {
        public Task AppendAsync(string path, string content, CancellationToken cancellationToken)
        {
            return Task.Delay(Timeout.Infinite, cancellationToken);
        }
    }
}

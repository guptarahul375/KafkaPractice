using System.Net;
using System.Net.Http.Json;
using KafkaPractice.Api.Endpoints;
using KafkaPractice.Api.Kafka;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaPractice.Tests.Endpoints;

public sealed class MessageEndpointsTests
{
    [Theory]
    [InlineData("")]
    [InlineData("  ")]
    public async Task PostMessages_RejectsEmptyMessage(string message)
    {
        var producer = new RecordingProducer();
        await using var app = await CreateAppAsync(producer);
        using var client = app.GetTestClient();

        var response = await client.PostAsJsonAsync("/messages", new PublishMessageRequest(message));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal(0, producer.CallCount);
    }

    [Fact]
    public async Task PostMessages_PublishesMessageAndReturnsDeliveryDetails()
    {
        var producer = new RecordingProducer();
        await using var app = await CreateAppAsync(producer);
        using var client = app.GetTestClient();

        var response = await client.PostAsJsonAsync(
            "/messages",
            new PublishMessageRequest("hello", "key-1"));
        var result = await response.Content.ReadFromJsonAsync<PublishedMessage>();

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Equal(new PublishedMessage("practice-messages", 0, 3, "key-1"), result);
        Assert.Equal("hello", producer.Value);
        Assert.Equal("key-1", producer.Key);
    }

    private static async Task<WebApplication> CreateAppAsync(IMessageProducer producer)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddSingleton(producer);
        builder.Services.AddSingleton<IMessageProducer>(producer);

        var app = builder.Build();
        app.MapMessageEndpoints();
        await app.StartAsync();
        return app;
    }

    private sealed class RecordingProducer : IMessageProducer
    {
        public int CallCount { get; private set; }
        public string? Value { get; private set; }
        public string? Key { get; private set; }

        public Task<PublishedMessage> PublishAsync(
            string value,
            string? key,
            CancellationToken cancellationToken)
        {
            CallCount++;
            Value = value;
            Key = key;
            return Task.FromResult(new PublishedMessage("practice-messages", 0, 3, key ?? ""));
        }
    }
}

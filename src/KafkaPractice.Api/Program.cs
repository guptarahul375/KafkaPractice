using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaPractice.Api.Configuration;
using KafkaPractice.Api.Kafka;
using KafkaPractice.Api.Endpoints;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();
builder.Services
    .AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection("Kafka"))
    .Validate(options => !string.IsNullOrWhiteSpace(options.BootstrapServers),
        "Kafka:BootstrapServers must be configured.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.Topic),
        "Kafka:Topic must be configured.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.ConsumerGroupId),
        "Kafka:ConsumerGroupId must be configured.")
    .ValidateOnStart();
builder.Services.AddSingleton<KafkaProducerService>();
builder.Services.AddSingleton<IMessageProducer>(
    services => services.GetRequiredService<KafkaProducerService>());
builder.Services.AddSingleton<IAdminClient>(services =>
{
    var options = services.GetRequiredService<IOptions<KafkaOptions>>().Value;
    return new AdminClientBuilder(new AdminClientConfig
    {
        BootstrapServers = options.BootstrapServers
    }).Build();
});
builder.Services.AddSingleton<IKafkaTopicManager, KafkaTopicManager>();
builder.Services.AddSingleton<IKafkaConsumerClientFactory, ConfluentKafkaConsumerClientFactory>();
builder.Services.AddSingleton<IMessageFileWriter, MessageFileWriter>();
builder.Services.AddHostedService<KafkaMessageConsumer>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.MapMessageEndpoints();

app.Run();

public partial class Program;

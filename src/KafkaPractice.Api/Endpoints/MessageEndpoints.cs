using KafkaPractice.Api.Kafka;

namespace KafkaPractice.Api.Endpoints;

public static class MessageEndpoints
{
    public static void MapMessageEndpoints(this WebApplication app)
    {
        app.MapPost("/messages", async (
            PublishMessageRequest request,
            IMessageProducer producer,
            CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrWhiteSpace(request.Message))
            {
                return Results.BadRequest(new { error = "Message must not be empty." });
            }

            var result = await producer.PublishAsync(request.Message, request.Key, cancellationToken);
            return Results.Accepted(value: result);
        });
    }
}

public sealed record PublishMessageRequest(string Message, string? Key = null);

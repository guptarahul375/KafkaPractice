namespace KafkaPractice.Api.Kafka;

public interface IMessageFileWriter
{
    Task AppendAsync(string path, string content, CancellationToken cancellationToken);
}

public sealed class MessageFileWriter : IMessageFileWriter
{
    public Task AppendAsync(string path, string content, CancellationToken cancellationToken)
    {
        return File.AppendAllTextAsync(path, content, cancellationToken);
    }
}

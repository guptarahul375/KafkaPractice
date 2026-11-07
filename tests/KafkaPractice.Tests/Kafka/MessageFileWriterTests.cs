using KafkaPractice.Api.Kafka;

namespace KafkaPractice.Tests.Kafka;

public sealed class MessageFileWriterTests
{
    [Fact]
    public async Task AppendAsync_AppendsContentToFile()
    {
        var directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "messages.txt");
        var writer = new MessageFileWriter();

        try
        {
            await writer.AppendAsync(path, "first line" + Environment.NewLine, CancellationToken.None);
            await writer.AppendAsync(path, "second line" + Environment.NewLine, CancellationToken.None);

            Assert.Equal(
                new[] { "first line", "second line" },
                await File.ReadAllLinesAsync(path));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}

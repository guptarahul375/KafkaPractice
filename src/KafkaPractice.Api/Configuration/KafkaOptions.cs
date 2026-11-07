namespace KafkaPractice.Api.Configuration;

public sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = "practice-messages";
    public string ConsumerGroupId { get; set; } = "kafka-practice-consumer";
    public string MessageOutputDirectory { get; set; } = "KafkaMessages";
}

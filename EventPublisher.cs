using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Threading.Tasks;

namespace KafkaEventPublisher.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class EventPublisherController : ControllerBase
    {
        private readonly ProducerConfig _producerConfig;
        private readonly string _topicName;

        public EventPublisherController()
        {
            // Configure Kafka Producer
            _producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092" // Update with your Kafka broker's address
            };

            // Set the Kafka topic name
            _topicName = "your-topic-name";
        }

        [HttpPost]
        public async Task<IActionResult> PublishEvent(string message)
        {
            try
            {
                using (var producer = new ProducerBuilder<Null, string>(_producerConfig).Build())
                {
                    // Publish message to Kafka topic
                    var deliveryResult = await producer.ProduceAsync(_topicName, new Message<Null, string> { Value = message });
                    Console.WriteLine($"Delivered message to {deliveryResult.TopicPartitionOffset}");
                    return Ok($"Message '{message}' published successfully to topic {_topicName}");
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Failed to publish message: {ex.Message}");
            }
        }
    }
}

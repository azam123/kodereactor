using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace EventConsumer.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class EventConsumerController : ControllerBase
    {
        private readonly ConsumerConfig _consumerConfig;
        private readonly string _topicName;

        public EventConsumerController()
        {
            // Configure Kafka Consumer
            _consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092", // Update with your Kafka broker's address
                GroupId = "group-id" // Specify a consumer group ID
            };

            // Set the Kafka topic name
            _topicName = "your-topic-name";
        }

        [HttpGet]
        public async Task<IActionResult> ConsumeEvent(CancellationToken cancellationToken)
        {
            try
            {
                using (var consumer = new ConsumerBuilder<Ignore, string>(_consumerConfig).Build())
                {
                    // Subscribe to Kafka topic
                    consumer.Subscribe(_topicName);

                    while (!cancellationToken.IsCancellationRequested)
                    {
                        // Consume messages
                        var consumeResult = consumer.Consume(cancellationToken);
                        
                        // Process consumed message
                        Console.WriteLine($"Consumed message: {consumeResult.Message.Value}");
                        // Add your custom processing logic here

                        // Optionally commit offsets
                        consumer.Commit(consumeResult);
                    }

                    return Ok("Consumer stopped");
                }
            }
            catch (OperationCanceledException)
            {
                return Ok("Consumer stopped");
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Failed to consume message: {ex.Message}");
            }
        }
    }
}

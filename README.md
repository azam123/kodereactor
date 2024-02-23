Here's a basic example of how you can create an API controller in .NET Core to publish events to a Kafka topic using the Confluent Kafka library:

Make sure to install the Confluent.Kafka NuGet package (Install-Package Confluent.Kafka), and replace "your-topic-name" and "localhost:9092" with your Kafka topic name and broker address respectively.

This example creates an API endpoint (/api/EventPublisher) that accepts POST requests with a message payload. It then publishes this message to the specified Kafka topic using the Confluent Kafka library.

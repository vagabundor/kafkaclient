# KafkaClient

KafkaClient is a Go library designed to simplify and enhance interactions with Apache Kafka. It provides robust features for message production, including connection retries, batch processing, and seamless integration with a ring buffer for efficient data handling.

## Features

- **Reliable Kafka Connection**: Built-in retry logic to ensure stable connections.
- **Batch Message Processing**: Efficiently send messages in batches to improve throughput.
- **Ring Buffer Support**: Integrates with `kafkabuff` for optimized buffering and flow control.
- **Customizable Logger**: Plug in your logger or use the default standard logger.
- **Readiness State**: Track Kafka client's readiness state for better monitoring and resilience.

## Installation

```bash
go get github.com/vagabundor/kafkaclient/v2
```
## Usage
Creating a Kafka Client

```go
package main

import (
    "time"

    "github.com/IBM/sarama"
    "github.com/vagabundor/kafkaclient"
)

func main() {
    brokers := []string{"localhost:9092"}
    maxRetries := 5
    retryInterval := 2 * time.Second

    client, err := kafkaclient.NewKafkaClient(brokers, maxRetries, retryInterval, nil, nil)
    if err != nil {
        panic(err)
    }
    defer client.Close()

    // Check readiness
    if client.IsReady() {
        println("Kafka client is ready!")
    }
}
```
## Sending a Batch of Messages
```go
messages := []*sarama.ProducerMessage{
    {Topic: "example-topic", Value: sarama.StringEncoder("Message 1")},
    {Topic: "example-topic", Value: sarama.StringEncoder("Message 2")},
}

err := client.SendBatch(messages, "example-topic")
if err != nil {
    println("Failed to send messages:", err)
}
```
## Using the Ring Buffer for Batch Sending
```go
import "github.com/vagabundor/kafkabuff"

ringBuffer := kafkabuff.NewRingBuffer(1000) // Create a ring buffer with a capacity of 1000
ringBuffer.Add(&sarama.ProducerMessage{Topic: "example-topic", Value: sarama.StringEncoder("Message")})

client.StartBatchSender(ringBuffer, 10, time.Second, "example-topic")
```
## Configuration Options
- **Brokers:** Kafka broker addresses (e.g., []string{"localhost:9092"}).
- **Retries:** Maximum number of retry attempts for connecting or sending messages.
- **Retry Interval:** Duration between retries. When set to 0, retries are infinite.
- **Logger:** Use a custom logger by implementing the Logger interface.

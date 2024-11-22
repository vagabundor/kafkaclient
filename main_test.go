package kafkaclient

import (
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
)

func TestKafkaClientSendBatch_Success(t *testing.T) {
	// Mock producer setup
	mockProducer := mocks.NewSyncProducer(t, nil)

	// Expect a successful message send
	mockProducer.ExpectSendMessageAndSucceed()

	client := &KafkaClient{
		producer:      mockProducer,
		kafkaBroker:   "localhost:9092",
		maxRetries:    3,
		retryInterval: 1 * time.Second,
	}
	client.isReady.Store(true)

	// Batch of messages to send
	batch := []*sarama.ProducerMessage{
		{Topic: "test-topic", Value: sarama.StringEncoder("test-message")},
	}

	// Act: Send batch
	err := client.SendBatch(batch, "test-topic")

	// Assert: No error expected
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestKafkaClientSendBatch_Failure(t *testing.T) {
	// Mock producer setup
	mockProducer := mocks.NewSyncProducer(t, nil)

	// Expect failures for all retries
	for i := 0; i < 3; i++ {
		mockProducer.ExpectSendMessageAndFail(sarama.ErrOutOfBrokers)
	}

	client := &KafkaClient{
		producer:      mockProducer,
		kafkaBroker:   "localhost:9092",
		maxRetries:    3,
		retryInterval: 1 * time.Second,
	}
	client.isReady.Store(true)

	// Batch of messages to send
	batch := []*sarama.ProducerMessage{
		{Topic: "test-topic", Value: sarama.StringEncoder("test-message")},
	}

	// Act: Send batch
	err := client.SendBatch(batch, "test-topic")

	// Assert: Error is expected
	if err == nil {
		t.Errorf("Expected error, got nil")
	}

	// Assert: Client should not be ready after failure
	if client.IsReady() {
		t.Errorf("Expected client to be not ready after failure")
	}
}

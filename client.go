package kafkaclient

import (
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/vagabundor/kafkabuff"
)

type KafkaClient struct {
	producer      sarama.SyncProducer
	isReady       atomic.Value
	kafkaBroker   string
	maxRetries    int
	retryInterval time.Duration
}

// NewKafkaClient creates a new KafkaClient instance.
func NewKafkaClient(broker string, maxRetries int, retryInterval time.Duration) (*KafkaClient, error) {
	client := &KafkaClient{
		kafkaBroker:   broker,
		maxRetries:    maxRetries,
		retryInterval: retryInterval,
	}

	err := client.connect()
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	client.isReady.Store(true)
	return client, nil
}

// connect establishes a connection to Kafka.
func (kc *KafkaClient) connect() error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForLocal

	attemptCount := 0

	for i := 0; i < kc.maxRetries || kc.maxRetries == 0; i++ {
		attemptCount++
		producer, err := sarama.NewSyncProducer([]string{kc.kafkaBroker}, config)
		if err == nil {
			kc.producer = producer
			log.Printf("Connected to Kafka after %d attempt(s)\n", attemptCount)
			return nil
		}

		if kc.maxRetries == 0 {
			log.Printf("Failed to connect to Kafka (attempt %d/∞): %v\n", attemptCount, err)
		} else {
			log.Printf("Failed to connect to Kafka (attempt %d/%d): %v\n", attemptCount, kc.maxRetries, err)
		}

		time.Sleep(kc.retryInterval)

		if kc.maxRetries == 0 {
			i--
		}
	}

	return fmt.Errorf("could not connect to Kafka after %d attempts", attemptCount)
}

// IsReady returns the readiness state of the Kafka client.
func (kc *KafkaClient) IsReady() bool {
	return kc.isReady.Load().(bool)
}

// SendBatch sends a batch of messages to Kafka with retries.
func (kc *KafkaClient) SendBatch(batch []*sarama.ProducerMessage, topic string) error {
	for i := 0; i < kc.maxRetries || kc.maxRetries == 0; i++ {
		if !kc.ensureConnected() {
			log.Println("Failed to connect to Kafka. Aborting batch send.")
			return errors.New("kafka connection failed")
		}

		err := kc.producer.SendMessages(batch)
		if err == nil {
			log.Printf("Batch of %d messages sent to Kafka topic(%s)\n", len(batch), topic)
			kc.isReady.Store(true)
			return nil
		}

		var producerErrors sarama.ProducerErrors
		if errors.As(err, &producerErrors) {
			for _, pe := range producerErrors {
				log.Printf("Failed message: topic=%s, value=%v, error=%v", pe.Msg.Topic, pe.Msg.Value, pe.Err)
			}
		}

		log.Printf("Retrying to send batch of %d messages", len(batch))
		time.Sleep(kc.retryInterval)
	}

	log.Println("Failed to send batch to Kafka after retries")
	kc.isReady.Store(false)
	return errors.New("failed to send batch after retries")
}

// StartBatchSender starts sending messages from the ring buffer to Kafka.
func (kc *KafkaClient) StartBatchSender(ringBuffer *kafkabuff.RingBuffer, batchSize int, interval time.Duration, kafkaTopic string) {
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				if ringBuffer.Size() > 0 {
					batch := ringBuffer.GetBatch(batchSize)
					err := kc.SendBatch(batch, kafkaTopic)
					if err != nil {
						log.Printf("Error sending batch: %v", err)
					}
				}
			default:
				if ringBuffer.Size() >= batchSize {
					batch := ringBuffer.GetBatch(batchSize)
					err := kc.SendBatch(batch, kafkaTopic)
					if err != nil {
						log.Printf("Error sending batch: %v", err)
					}
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()
}

// ensureConnected checks the connection to Kafka and tries to reconnect if necessary.
func (kc *KafkaClient) ensureConnected() bool {
	if kc.producer == nil {
		log.Println("Producer is not initialized. Trying to reconnect.")
		err := kc.connect()
		if err != nil {
			log.Printf("Failed to reconnect to Kafka: %v\n", err)
			return false
		}
		kc.isReady.Store(true)
	}
	return true
}

// Close closes the connection to Kafka.
func (kc *KafkaClient) Close() error {
	if kc.producer != nil {
		return kc.producer.Close()
	}
	return nil
}

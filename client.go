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
	config        *sarama.Config
	isReady       atomic.Value
	kafkaBrokers  []string
	maxRetries    int
	retryInterval time.Duration
	logger        Logger
}

// Logger interface
type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type StdLogger struct{}

func (l *StdLogger) Debugf(format string, args ...interface{}) {
	log.Printf("[DEBUG] "+format, args...)
}
func (l *StdLogger) Infof(format string, args ...interface{}) { log.Printf("[INFO] "+format, args...) }
func (l *StdLogger) Warnf(format string, args ...interface{}) { log.Printf("[WARN] "+format, args...) }
func (l *StdLogger) Errorf(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}

// NewKafkaClient creates a new KafkaClient instance.
func NewKafkaClient(brokers []string, maxRetries int, retryInterval time.Duration, config *sarama.Config, logger Logger) (*KafkaClient, error) {
	if config == nil {
		config = sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForLocal
	}

	if logger == nil {
		logger = &StdLogger{}
	}

	client := &KafkaClient{
		kafkaBrokers:  brokers,
		maxRetries:    maxRetries,
		retryInterval: retryInterval,
		config:        config,
		logger:        logger,
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
	attemptCount := 0

	for i := 0; i < kc.maxRetries || kc.maxRetries == 0; i++ {
		attemptCount++
		producer, err := sarama.NewSyncProducer(kc.kafkaBrokers, kc.config)
		if err == nil {
			kc.producer = producer
			kc.logger.Infof("Connected to Kafka after %d attempt(s)", attemptCount)
			return nil
		}

		if kc.maxRetries == 0 {
			kc.logger.Warnf("Failed to connect to Kafka (attempt %d/∞): %v", attemptCount, err)
		} else {
			kc.logger.Warnf("Failed to connect to Kafka (attempt %d/%d): %v", attemptCount, kc.maxRetries, err)
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
	var failedMessages []*sarama.ProducerMessage

	for i := 0; i < kc.maxRetries || kc.maxRetries == 0; i++ {
		if !kc.ensureConnected() {
			kc.logger.Errorf("Failed to connect to Kafka. Aborting batch send.")
			return errors.New("kafka connection failed")
		}

		err := kc.producer.SendMessages(batch)
		if err == nil {
			kc.logger.Infof("Batch of %d messages sent to Kafka topic(%s)", len(batch), topic)
			kc.isReady.Store(true)
			return nil
		}

		// Handle producer errors
		var producerErrors sarama.ProducerErrors
		if errors.As(err, &producerErrors) {
			failedMessages = failedMessages[:0] // Reset failedMessages slice
			for _, pe := range producerErrors {
				kc.logger.Debugf("Failed message: topic=%s, value=%v, error=%v", pe.Msg.Topic, pe.Msg.Value, pe.Err)
				// Collect only failed messages
				failedMessages = append(failedMessages, pe.Msg)
			}
			batch = failedMessages
			kc.logger.Warnf("Retrying to send %d failed messages", len(failedMessages))
		} else {
			kc.logger.Errorf("Unexpected error when sending batch to Kafka: %v", err)
			kc.isReady.Store(false)
			break
		}

		time.Sleep(kc.retryInterval)
	}

	if len(failedMessages) > 0 {
		kc.logger.Errorf("Failed to send %d messages to Kafka after retries", len(failedMessages))
	}
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
						kc.logger.Errorf("Error sending batch: %v", err)
					}
				}
			default:
				if ringBuffer.Size() >= batchSize {
					batch := ringBuffer.GetBatch(batchSize)
					err := kc.SendBatch(batch, kafkaTopic)
					if err != nil {
						kc.logger.Errorf("Error sending batch: %v", err)
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
		kc.logger.Infof("Producer is not initialized. Trying to reconnect.")
		err := kc.connect()
		if err != nil {
			kc.logger.Errorf("Failed to reconnect to Kafka: %v\n", err)
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

package kafka

import (
	"context"
	"log"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

// abstracts kafka.Producer message type
type ProducerMessage struct {
	Topic string
	Key   []byte
	Value []byte
}

type Producer interface {
	// caller should run the returned function in a goroutine, and consume
	// the returned error channel until it's closed at shutdown.
	Background() (func(), chan error)

	// user-facing event emit API
	Send(ProducerMessage) error
}

// internal type implementing kafka.Producer contract
type kafkaProducer struct {
	ctx  context.Context
	conf Config

	producer sarama.AsyncProducer
	errors   chan error
	logger   *log.Logger
}

// the caller can cancel the producer's context to initiate shutdown.
func NewProducer(ctx context.Context, conf Config, logger *log.Logger) (Producer, error) {
	producer, err := createProducer(conf, logger)
	if err != nil {
		return nil, err
	}

	return &kafkaProducer{
		conf:     conf,
		ctx:      ctx,
		producer: producer,
		errors:   make(chan error, errorQueueSize),
		logger:   logger,
	}, nil
}

// user-facing event emit API
func (kp *kafkaProducer) Send(msg ProducerMessage) error {
	if len(msg.Topic) == 0 {
		return errors.New("message Topic is required")
	}

	if len(msg.Key) == 0 && len(msg.Value) == 0 {
		return errors.New("at least one of message fields Key or Value is required")
	}

	kmsg := &sarama.ProducerMessage{
		Topic: msg.Topic,
		Key:   sarama.ByteEncoder(msg.Key),
		Value: sarama.ByteEncoder(msg.Value),
	}

	// if shutdown is triggered, drop the message
	select {
	case <-kp.ctx.Done():
		return errors.Wrapf(kp.ctx.Err(), "message lost: shutdown triggered during send")

	case kp.producer.Input() <- kmsg:
		// fall through, msg has been queued for write
	}

	return nil
}

// caller should run the returned function in a goroutine, and consume
// the returned error channel until it's closed at shutdown.
func (kp *kafkaProducer) Background() (func(), chan error) {
	// proxy all Sarama errors to the caller until Close() drains and closes it
	go func() {
		for err := range kp.producer.Errors() {
			kp.errors <- err
		}

		kp.logger.Printf("Kafka producer: shutting down error reporter")
	}()

	return func() {
		defer func() {
			kp.errors <- kp.producer.Close()
			close(kp.errors)
		}()

		<-kp.ctx.Done()
		kp.logger.Printf("Kafka producer: shutdown triggered")
	}, kp.errors
}

func createProducer(conf Config, logger *log.Logger) (sarama.AsyncProducer, error) {
	if logger != nil {
		sarama.Logger = logger
	}

	cfg, err := configureProducer(conf)
	if err != nil {
		return nil, err
	}

	brokers := strings.Split(conf.Brokers, ",")
	producer, err := sarama.NewAsyncProducer(brokers, cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create Kafka producer")
	}

	return producer, nil
}

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/elireisman/sarama-easy/kafka"
)

var (
	conf  kafka.Config
	topic string
	count int
)

func init() {
	conf = kafka.NewKafkaConfig()

	// apply minimal config only for example run
	flag.StringVar(&conf.Brokers, "brokers", "localhost:9092", "CSV list of Kafka seed brokers to produce events to")
	flag.StringVar(&topic, "topic", "example", "CSV list of Kafka topic to produce to")
	flag.BoolVar(&conf.Verbose, "verbose", false, "Log detailed Kafka client internals?")
	flag.IntVar(&count, "count", 10, "number of example messages to produce")
}

func main() {
	flag.Parse()

	logger := log.New(os.Stdout, "[example Kafka producer] ", log.LstdFlags|log.LUTC|log.Lshortfile)

	ctx, cancelable := context.WithCancel(context.Background())

	producer, err := kafka.NewProducer(ctx, conf, logger)
	if err != nil {
		logger.Fatal(err)
	}

	// obtain the background kafka.Producer process, and the error channel to consume
	processor, errors := producer.Background()
	go processor()

	// if not consuming errors in main thread of exec,
	// use wait group (or http.ListenAndServe, etc.) to block
	wg := &sync.WaitGroup{}

	// consume errors channel until kafka.Producer closes it, when context is canceled
	wg.Add(1)
	go func() {
		defer wg.Done()

		// consume this until library closes it for us, indicating client has shut down
		for err := range errors {
			logger.Printf("Kafka message error: %s", err)
		}
	}()

	for i := 0; i < count; i++ {
		msg := kafka.ProducerMessage{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("message_%d", i+1)),
			Value: []byte("example data"),
		}
		logger.Printf("Sending message %d", i+1)

		if err := producer.Send(msg); err != nil {
			logger.Printf(err.Error()) // only happens if context is cancelled while Send() is blocked on full queue
			break
		}
	}

	// send poison pill so consumer-example will know to shut down:
	pill := kafka.ProducerMessage{
		Topic: topic,
		Key:   []byte("poison pill"),
		Value: []byte{},
	}
	producer.Send(pill)

	// signal the producer's background client to gracefully shut down
	cancelable()

	// wait for the client to shut down after draining outgoing message and error queues
	wg.Wait()
	logger.Printf("Run complete, exiting")
}

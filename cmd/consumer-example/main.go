package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/elireisman/sarama-easy/kafka"
)

var conf kafka.Config

func init() {
	conf = kafka.NewKafkaConfig()

	// apply minimal config only for example run
	flag.StringVar(&conf.Brokers, "brokers", "localhost:9092", "CSV list of Kafka seed brokers to request group membership from")
	flag.StringVar(&conf.Topics, "topics", "example", "CSV list of Kafka topics to consume")
	flag.StringVar(&conf.Group, "group", "example", "Kafka consumer group to join")
	flag.BoolVar(&conf.Verbose, "verbose", false, "Log detailed Kafka client internals?")
	flag.StringVar(&conf.InitOffsets, "offsets", "earliest", "Kafka group rebalance strategy")
}

func main() {
	flag.Parse()

	logger := log.New(os.Stdout, "[example Kafka consumer] ", log.LstdFlags|log.LUTC|log.Lshortfile)

	// context can be canceled, initiating kafka.Consumer shutdown, from
	// kafka.Handlers scope or here in the main thread, it doesn't matter
	ctx, cancelable := context.WithCancel(context.Background())

	// implements kafka.Handler. All messages will be passed here by kafka.Consumer
	handlers := &exampleHandler{
		// arbitrary things you might want access to in kafka.Handler scope
		cancelable: cancelable,
		logger:     logger,
	}

	consumer, err := kafka.NewConsumer(ctx, conf, handlers, logger)
	if err != nil {
		logger.Fatal(err)
	}

	// obtain the background kafka.Consumer process that does the work,
	// and the error channel to consume (closed by Consumer on shutdown)
	processor, errors := consumer.Background()
	go processor()

	// could consume the errors channel in a goroutine, as producer-example does.
	for err := range errors {
		log.Printf("Kafka message error: %s", err)
		// note: if an error is deemed fatal you can call cancelable() from here too
	}

	log.Printf("Run complete, exiting")
}

// example kafka.Handler implementation
type exampleHandler struct {
	// included here so I have the option to gracefully shut down the consumer
	cancelable context.CancelFunc

	// so I can log the messages for our example run
	logger *log.Logger
}

// implements kafka.Handler - errors returned here will be treated as fatal
// and will trigger graceful shutdown of the kafka.Consumer
func (eh *exampleHandler) Message(msg *kafka.ConsumerMessage) error {
	key := string(msg.Key)
	value := string(msg.Value)
	topic := msg.Topic

	eh.logger.Printf("Kafka topic %s message received: Key(%s) Value(%s)", topic, key, value)

	// when the example producer sends last message, trigger graceful shutdown
	if key == "poison pill" {
		eh.cancelable()
	}

	// no need to bubble up fatal errors in our example
	return nil
}

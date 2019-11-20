# sarama-easy

[Sarama](https://github.com/Shopify/sarama) is a Golang client for Apache Kafka. This is a light wrapper and example demo for Sarama's async producer and consumer (including auto-rebalancing consumer groups.)

## Usage

#### Run the demo producer/consumer
```bash
# From the root dir of the repo checkout:
script/build

docker-compose up

# From another terminal session:
bin/consumer-example &

bin/producer-example
```

#### Result
* Producer will emit 10 messages and a sentinel value to the example topic, then shut down.
* Consumer will receive 10 example messages, and a sentinel message triggering it's own shutdown.

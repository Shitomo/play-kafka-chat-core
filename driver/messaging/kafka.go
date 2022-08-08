package messaging

import (
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

func NewProducer() (sarama.AsyncProducer, error) {
	bootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	brokers := strings.Split(bootstrapServers, ",")
	config := sarama.NewConfig()

	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 3

	return sarama.NewAsyncProducer(brokers, config)
}

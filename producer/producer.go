package producer

import (
	"github.com/Shopify/sarama"
)

type Producer struct {
    Input chan<- *sarama.ProducerMessage
}

func NewProducer(brokers []string) (*Producer, error) {
    config := sarama.NewConfig()
    config.Producer.Return.Successes = false
    p, err := sarama.NewAsyncProducer(brokers, config)
    if err != nil {
        return nil, err
    }
    return &Producer{
        Input: p.Input(),
    }, nil
}

func (p *Producer) Close() {
    p.Input <- nil
}
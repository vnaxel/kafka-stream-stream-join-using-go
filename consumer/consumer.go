package consumer

import (
	"fmt"

	"github.com/Shopify/sarama"
)

type Consumer struct {
	client sarama.Consumer
}

func NewConsumer(brokers []string) (*Consumer, error) {
	client, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		fmt.Println("Error creating consumer: ", err)
		return nil, err
	}
	return &Consumer{client: client}, nil
}

func (c *Consumer) Consume(topic string, handleMessage func(msg *sarama.ConsumerMessage)) error {
	pc, err := c.client.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		fmt.Println("Error consuming partition: ", err)
		return err
	}
	go func() {
		for msg := range pc.Messages() {
			handleMessage(msg)
		}
	}()
	return nil
}
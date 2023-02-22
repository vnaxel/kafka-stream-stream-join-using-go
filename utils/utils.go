package utils

import (
	"fmt"
	"go-joins-streams/models"
	"go-joins-streams/producer"
	"time"

	"github.com/Shopify/sarama"
	uuid "github.com/satori/go.uuid"
)

var p1, _ = producer.NewProducer([]string{"localhost:9092"})
var p2, _ = producer.NewProducer([]string{"localhost:9092"})


// Remplissage des topics a l'aide d'un producer sarama passé en paramètre.
func LoadTopics(topic_items, topic_requests string) {

	p1.Input <- &sarama.ProducerMessage{
		Topic: topic_requests,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewRequestMarshal(1, "Maileva")),
	}

	p1.Input <- &sarama.ProducerMessage{
		Topic: topic_requests,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewRequestMarshal(2, "Docaposte")),
	}

	p1.Input <- &sarama.ProducerMessage{
		Topic: topic_requests,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewRequestMarshal(3, "La Poste")),
	}

	p2.Input <- &sarama.ProducerMessage{
		Topic: topic_items,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewItemMarshal(4, "Recipient1", 1, "")),
	}

	p2.Input <- &sarama.ProducerMessage{
		Topic: topic_items,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewItemMarshal(5, "Recipient2", 2, "")),
	}

	p2.Input <- &sarama.ProducerMessage{
		Topic: topic_items,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewItemMarshal(6, "Recipient3", 3, "")),
	}

	p2.Input <- &sarama.ProducerMessage{
		Topic: topic_items,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewItemMarshal(7, "Recipient4", 1, "")),
	}

	p2.Input <- &sarama.ProducerMessage{
		Topic: topic_items,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewItemMarshal(8, "Recipient5", 2, "")),
	}

	p2.Input <- &sarama.ProducerMessage{
		Topic: topic_items,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewItemMarshal(9, "Recipient6", 3, "")),
	}

	p2.Input <- &sarama.ProducerMessage{
		Topic: topic_items,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewItemMarshal(10, "Recipient7", 1, "")),
	}

	p2.Input <- &sarama.ProducerMessage{
		Topic: topic_items,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewItemMarshal(11, "Recipient8", 2, "")),
	}

	p2.Input <- &sarama.ProducerMessage{
		Topic: topic_items,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewItemMarshal(12, "Recipient9", 3, "")),
	}

}

func UUID() string {
	return uuid.NewV4().String()
}

func LoadTopicsOnce(topic_1, topic_2 string) {
	time.AfterFunc(2000*time.Millisecond, func() {
		LoadTopics(topic_1, topic_2)
	})
}

func LoadTopicsContinuously(topic_1, topic_2 string, tickerFreq, testDuration time.Duration) {
	time.AfterFunc(2000*time.Millisecond, func() {
		fmt.Println("Let's load topics !")
		tickerFreq := tickerFreq
		testDuration := testDuration
		ticker := time.NewTicker(tickerFreq)
		time.AfterFunc(testDuration, func() {
			time.AfterFunc(1 * time.Second, func() {
		fmt.Printf("Topics have been loaded every %s for %s.\n", tickerFreq, testDuration)
			})
			ticker.Stop()
		})
		for range ticker.C{
			LoadTopics(topic_1, topic_2)
		}
	})
}
package utils

import (
	"fmt"
	"go-joins-streams/models"
	"go-joins-streams/producer"
	"time"

	"github.com/Shopify/sarama"
	uuid "github.com/satori/go.uuid"
)

// Remplissage des topics a l'aide d'un producer sarama passé en paramètre.
func LoadTopics(p *producer.Producer, topic_enfants, topic_parents string) {

	p.Input <- &sarama.ProducerMessage{
		Topic: topic_parents,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewParentMarshal(1, "Michel-Ryan")),
	}

	p.Input <- &sarama.ProducerMessage{
		Topic: topic_parents,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewParentMarshal(2, "Jakeline")),
	}

	p.Input <- &sarama.ProducerMessage{
		Topic: topic_parents,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewParentMarshal(3, "Jean-Bernadette")),
	}

	p.Input <- &sarama.ProducerMessage{
		Topic: topic_enfants,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewEnfantMarshal(4, "Arya", 1, "")),
	}

	p.Input <- &sarama.ProducerMessage{
		Topic: topic_enfants,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewEnfantMarshal(5, "Jon", 2, "")),
	}

	p.Input <- &sarama.ProducerMessage{
		Topic: topic_enfants,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewEnfantMarshal(6, "Mortimer", 3, "")),
	}

	p.Input <- &sarama.ProducerMessage{
		Topic: topic_enfants,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewEnfantMarshal(7, "Mowgli", 1, "")),
	}

	p.Input <- &sarama.ProducerMessage{
		Topic: topic_enfants,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewEnfantMarshal(8, "Oliver", 2, "")),
	}

	p.Input <- &sarama.ProducerMessage{
		Topic: topic_enfants,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewEnfantMarshal(9, "Martine", 3, "")),
	}

	p.Input <- &sarama.ProducerMessage{
		Topic: topic_enfants,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewEnfantMarshal(10, "Mathilde", 1, "")),
	}

	p.Input <- &sarama.ProducerMessage{
		Topic: topic_enfants,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewEnfantMarshal(11, "Myriam", 2, "")),
	}

	p.Input <- &sarama.ProducerMessage{
		Topic: topic_enfants,
		Key:   sarama.StringEncoder(UUID()),
		Value: sarama.StringEncoder(models.NewEnfantMarshal(12, "Ravi", 3, "")),
	}

}

func UUID() string {
	return uuid.NewV4().String()
}

func LoadTopicsOnce(p *producer.Producer, topic_1, topic_2 string) {
	time.AfterFunc(2000*time.Millisecond, func() {
		LoadTopics(p, topic_1, topic_2)
	})
}

func LoadTopicsContinuously(p *producer.Producer, topic_1, topic_2 string, tickerFreq, testDuration time.Duration) {
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
			LoadTopics(p, topic_1, topic_2)
		}
	})
}
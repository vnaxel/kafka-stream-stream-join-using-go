package main

import (
	"encoding/json"
	"go-joins-streams/consumer"
	"go-joins-streams/models"
	"go-joins-streams/producer"
	"go-joins-streams/topology"
	"go-joins-streams/utils"

	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

var broker = []string{"localhost:9092"}
var p, _ = producer.NewProducer(broker)
var c1, _ = consumer.NewConsumer(broker)
var c2, _ = consumer.NewConsumer(broker)

var topic_1 = "topic_1"
var topic_2 = "topic_2"
var topic_3 = "topic_3"

var enfantsChan = make(chan models.Enfant)
var parentsChan = make(chan models.Parent)

func main() {

	var wg sync.WaitGroup

	wg.Add(3)

	fmt.Println("start childs topic consumer")
	go c1.Consume(topic_1, consumeEnfantMessage)

	fmt.Println("start parents topic consumer")
	go c2.Consume(topic_2, consumeParentMessage)


	joinWindow := 150 * time.Millisecond

	go topology.ValueJoiner(enfantsChan, parentsChan, p, topic_3, joinWindow)

	
	// utils.LoadTopicsOnce(p, topic_1, topic_2)
	/*
	En utilisant LoadTopicsContinuously, il faut passer pour la fréquence une valeur de temps plus grand que
	la joinWindow du valueJoinder car ce sont les même données qui vont être envoyées de manière répétée
	*/
	utils.LoadTopicsContinuously(p, topic_1, topic_2, joinWindow + 50 * time.Millisecond, 5 * time.Second)

	wg.Wait()
}


/*
Passe le message du topic enfants au channel correspondant + déserialisation
*/
func consumeEnfantMessage(msg *sarama.ConsumerMessage) {
	var enfant models.Enfant
	err := json.Unmarshal(msg.Value, &enfant)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	enfantsChan <- enfant
}


/*
Passe le message du topic parents au channel correspondant + déserialisation
*/
func consumeParentMessage(msg *sarama.ConsumerMessage) {
	var parent models.Parent
	err := json.Unmarshal(msg.Value, &parent)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	parentsChan <- parent
}





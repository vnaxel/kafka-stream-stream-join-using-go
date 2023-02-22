package main

import (
	"encoding/json"
	"go-joins-streams/consumer"
	"go-joins-streams/models"
	"go-joins-streams/topology"
	"go-joins-streams/utils"

	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

var broker = []string{"localhost:9092"}
var c1, _ = consumer.NewConsumer(broker)
var c2, _ = consumer.NewConsumer(broker)

var topic_1 = "topic_1"
var topic_2 = "topic_2"
var topic_3 = "topic_3"

var itemsChan = make(chan models.Item)
var requestsChan = make(chan models.Request)

func main() {

	var wg sync.WaitGroup

	wg.Add(2)

	fmt.Println("start items topic consumer")
	go c1.Consume(topic_1, consumeItemMessage)

	fmt.Println("start requests topic consumer")
	go c2.Consume(topic_2, consumeRequestMessage)

	joinWindow := 49 * time.Millisecond

	go topology.ValueJoiner(itemsChan, requestsChan, topic_3, joinWindow)
	
	// utils.LoadTopicsOnce(topic_1, topic_2)
	/*
	En utilisant LoadTopicsContinuously, il faut passer pour la fréquence une valeur de temps plus grand que
	la joinWindow du valueJoinder car ce sont les même données qui vont être envoyées de manière répétée
	*/
	utils.LoadTopicsContinuously(topic_1, topic_2, joinWindow + 1 * time.Millisecond, 300 * time.Second)

	wg.Wait()
}


/*
Passe le message du topic items au channel correspondant + déserialisation
*/
func consumeItemMessage(msg *sarama.ConsumerMessage) {
	var enfant models.Item
	err := json.Unmarshal(msg.Value, &enfant)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	itemsChan <- enfant
}


/*
Passe le message du topic requests au channel correspondant + déserialisation
*/
func consumeRequestMessage(msg *sarama.ConsumerMessage) {
	var request models.Request
	err := json.Unmarshal(msg.Value, &request)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	requestsChan <- request
}





package topology

import (
	"fmt"
	"go-joins-streams/models"
	"go-joins-streams/producer"
	"go-joins-streams/utils"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

var p, _ = producer.NewProducer([]string{"localhost:9092"})


func ValueJoiner(
	channel_1 chan models.Item,
	channel_2 chan models.Request,
	topic string,
	joinTimeWindow time.Duration) {

	fmt.Println("start joiner")

	// Création des maps pour stocker temporairement les structs a joindre.
	itemsMap := make(map[string]models.Item)
	requestsMap := make(map[string]models.Request)

	// Création des map pour stocker les timers de chaque message.
	itemsTimers := make(map[string]*time.Timer)
	requestsTimers := make(map[string]*time.Timer)

	windowedTime := joinTimeWindow

	var mutex = sync.Mutex{}

	// Sert a constater que les maps servant de store temporaire pour les jointures sont bien vidées.
	time.AfterFunc(1 * time.Second, func() {
		ticker := time.NewTicker(5 * time.Second)

		for range ticker.C{
			fmt.Println("Temp stores maps status:\nitems:", itemsMap, "\nrequests:", requestsMap)
		}
	})

	for {
		select {
		// Un struct item passe dans le channel.
		case item := <-channel_1:

			// Il est sauvegardé dans la map.
			itemJoinUuid := utils.UUID()
			mutex.Lock()
			itemsMap[itemJoinUuid] = item

			// Il sera supprimé apres le fenêtre de temps s'il n'a pas trouvé le struct correspondant.
			itemsTimers[itemJoinUuid] = time.AfterFunc(windowedTime, func() {
				mutex.Lock()
				delete(itemsMap, itemJoinUuid)
				delete(itemsTimers, itemJoinUuid)
				mutex.Unlock()
			})
			mutex.Unlock()


			// Pour chaque element de la map, 
			// Je cherche la valeur qui correspond dans la la map opposée.
			// (ici la valeur recherchée est item.SenderId == sender.Id)
			mutex.Lock()
			for iKey, iValue := range itemsMap {
				for _, rValue := range requestsMap {
					
					// Si je trouve une correspondance, je produit l'aggregation dans le topic de sortie,
					// la valeur est supprimée de la map.
					if *iValue.RequestId == *rValue.Id {
						p.Input <- &sarama.ProducerMessage{
							Topic: topic,
							Key:   sarama.StringEncoder("aggregated"),
							Value: sarama.StringEncoder(models.NewItemMarshal(*iValue.Id, *iValue.Name, *iValue.RequestId, *rValue.Sender)),
						}
						delete(itemsMap, iKey)
						delete(itemsTimers, iKey)
						fmt.Println(*iValue.Name, "est un destinataire de l'envoi de l'entreprise:", *rValue.Sender)
						break
					}
				}
			}
			mutex.Unlock()


		// Même chose de l'autre coté, a ceci près que ces structs ne sont
		// supprimés de leur map que par expiration de la fenêtre de temps.
		case request := <-channel_2:

			requestJoinUuid := utils.UUID()
			mutex.Lock()
			requestsMap[requestJoinUuid] = request
			requestsTimers[requestJoinUuid] = time.AfterFunc(windowedTime, func() {
				mutex.Lock()
				delete(requestsMap, requestJoinUuid)
				delete(requestsTimers, requestJoinUuid)
				mutex.Unlock()
			})
			mutex.Unlock()

			mutex.Lock()
			for _, rValue := range requestsMap {
				for iKey, iValue := range itemsMap {
					if *rValue.Id == *iValue.RequestId {
						p.Input <- &sarama.ProducerMessage{
							Topic: topic,
							Key:   sarama.StringEncoder("aggregated"),
							Value: sarama.StringEncoder(models.NewItemMarshal(*iValue.Id, *iValue.Name, *iValue.RequestId, *rValue.Sender)),
						}
						delete(itemsMap, iKey)
						delete(itemsTimers, iKey)
						fmt.Println(*iValue.Name, "est un destinataire de l'envoi de l'entreprise:", *rValue.Sender)
						// Pas de break puisqu'un request peut trouver plus d'un item lors d'une iteration
					}
				}
			}
			mutex.Unlock()

		}
	}
}

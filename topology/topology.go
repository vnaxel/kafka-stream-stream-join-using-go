package topology

import (
	"fmt"
	"go-joins-streams/models"
	"go-joins-streams/producer"
	"go-joins-streams/utils"
	"time"

	"github.com/Shopify/sarama"
)

func ValueJoiner(
	channel_1 chan models.Enfant,
	channel_2 chan models.Parent,
	p *producer.Producer,
	topic string,
	joinTimeWindow time.Duration) {

	fmt.Println("start joiner")

	// Création des maps pour stocker temporairement les structs a joindre.
	enfantsMap := make(map[string]models.Enfant)
	parentsMap := make(map[string]models.Parent)

	// Création des map pour stocker les timers de chaque message.
	enfantsTimers := make(map[string]*time.Timer)
	parentsTimers := make(map[string]*time.Timer)

	windowedTime := joinTimeWindow

	// Sert a constater que les maps servant de store temporaire pour les jointures sont bien vidées.
	time.AfterFunc(1 * time.Second, func() {
		ticker := time.NewTicker(5 * time.Second)

		for range ticker.C{
			fmt.Println("Temp stores maps status:\nEnfants:", enfantsMap, "\nParents:", parentsMap)
		}
	})

	for {
		select {
		// Un struct enfant passe dans le channel.
		case enfant := <-channel_1:

			// Il est sauvegardé dans la map.
			enfantJoinUuid := utils.UUID()
			enfantsMap[enfantJoinUuid] = enfant

			// Il sera supprimé apres le fenêtre de temps s'il n'a pas trouvé le struct correspondant.
			enfantsTimers[enfantJoinUuid] = time.AfterFunc(windowedTime, func() {
				delete(enfantsMap, enfantJoinUuid)
				delete(enfantsTimers, enfantJoinUuid)
			})

			// Pour chaque element de la map, 
			// Je cherche la valeur qui correspond dans la la map opposée.
			// (ici la valeur recherchée est enfant.ParentId == parent.Id)
			for eKey, eValue := range enfantsMap {
				for _, pValue := range parentsMap {
					
					// Si je trouve une correspondance, je produit l'aggregation dans le topic de sortie,
					// la valeur est supprimée de la map.
					if *eValue.ParentId == *pValue.Id {
						p.Input <- &sarama.ProducerMessage{
							Topic: topic,
							Key:   sarama.StringEncoder("aggregated"),
							Value: sarama.StringEncoder(models.NewEnfantMarshal(*eValue.Id, *eValue.Name, *eValue.ParentId, *pValue.Name)),
						}
						delete(enfantsMap, eKey)
						delete(enfantsTimers, eKey)
						fmt.Println(*eValue.Name, "says: I found my parent:", *pValue.Name)
						break
					}
				}
			}

		// Même chose de l'autre coté, a ceci près que ces structs ne sont
		// supprimés de leur map que par expiration de la fenêtre de temps.
		case parent := <-channel_2:

			parentJoinUuid := utils.UUID()
			parentsMap[parentJoinUuid] = parent
			parentsTimers[parentJoinUuid] = time.AfterFunc(windowedTime, func() {
				delete(parentsMap, parentJoinUuid)
				delete(parentsTimers, parentJoinUuid)
			})

			for _, pValue := range parentsMap {
				for eKey, eValue := range enfantsMap {
					if *pValue.Id == *eValue.ParentId {
						p.Input <- &sarama.ProducerMessage{
							Topic: topic,
							Key:   sarama.StringEncoder("aggregated"),
							Value: sarama.StringEncoder(models.NewEnfantMarshal(*eValue.Id, *eValue.Name, *eValue.ParentId, *pValue.Name)),
						}
						delete(enfantsMap, eKey)
						delete(enfantsTimers, eKey)
						fmt.Println(*eValue.Name, "says: I found my parent:", *pValue.Name)
						// Pas de break puisqu'un parent peut trouver plus d'un enfant lors d'une iteration
					}
				}
			}
		}
	}
}

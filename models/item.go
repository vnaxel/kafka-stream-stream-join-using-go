package models

import "encoding/json"

type Item struct {
	Id         	*int    	`json:"id"`
	Name       	*string 	`json:"name"`
	RequestId   *int    	`json:"senderId"`
	Sender 		*string 	`json:"sender"`
}

func NewItemMarshal(id int, name string, requestId int, sender string) []byte {
	item := new(Item)

	if id == 0 {
		item.Id = nil
	} else {
		item.Id = &id
	}

	if name == "" {
		item.Name = nil
	} else {
		item.Name = &name
	}

	if requestId == 0 {
		item.RequestId = nil
	} else {
		item.RequestId = &requestId
	}

	if sender == "" {
		item.Sender = nil
	} else {
		item.Sender = &sender
	}
	
	marshal, _ := json.Marshal(item)
	return marshal
}
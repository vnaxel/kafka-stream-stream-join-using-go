package models

import "encoding/json"

type Request struct {
	Id 		*int		`json:"id"`
	Sender 	*string		`json:"sender"`
}

func NewRequestMarshal(id int, sender string) []byte {
	request := new(Request)

	if id == 0 {
		request.Id = nil
	} else {
		request.Id = &id
	}

	if sender == "" {
		request.Sender = nil
	} else {
		request.Sender = &sender
	}
	
	marshal, _ := json.Marshal(request)
	return marshal
}
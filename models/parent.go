package models

import "encoding/json"

type Parent struct {
	Id 		*int		`json:"id"`
	Name 	*string		`json:"name"`
}

func NewParentMarshal(id int, name string) []byte {
	parent := new(Parent)

	if id == 0 {
		parent.Id = nil
	} else {
		parent.Id = &id
	}

	if name == "" {
		parent.Name = nil
	} else {
		parent.Name = &name
	}
	
	marshal, _ := json.Marshal(parent)
	return marshal
}
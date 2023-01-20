package models

import "encoding/json"

type Enfant struct {
	Id         	*int    	`json:"id"`
	Name       	*string 	`json:"name"`
	ParentId   	*int    	`json:"parentId"`
	ParentName 	*string 	`json:"parentName"`
}

func NewEnfantMarshal(id int, name string, parentId int, parentName string) []byte {
	enfant := new(Enfant)

	if id == 0 {
		enfant.Id = nil
	} else {
		enfant.Id = &id
	}

	if name == "" {
		enfant.Name = nil
	} else {
		enfant.Name = &name
	}

	if parentId == 0 {
		enfant.ParentId = nil
	} else {
		enfant.ParentId = &parentId
	}

	if parentName == "" {
		enfant.ParentName = nil
	} else {
		enfant.ParentName = &parentName
	}
	
	marshal, _ := json.Marshal(enfant)
	return marshal
}
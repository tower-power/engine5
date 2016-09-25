//
// ENGINE JSON Datastructures
//
//
package engine3

import (
	"encoding/json"
	"log"
)

func toJson(v interface{}) []byte {

	b, err := json.Marshal(v)
	if err != nil {
		log.Panic(err.Error)
	}
	return b
}

func fromJson(b []byte, v interface{}) {

	err := json.Unmarshal(b, &v)
	if err != nil {
		log.Panic(err.Error)
	}

}

func test() {
	m := Systems{"Alice"}

	b, err := json.Marshal(m)
	if err != nil {
		log.Panic(err.Error)
	}

	b1 := []byte(`{"Name":"Alice"}`)

	err = json.Unmarshal(b1, &m)
	if err != nil {
		log.Panic(err.Error)
	}

	err = json.Unmarshal(b, &m)
	if err != nil {
		log.Panic(err.Error)
	}

}

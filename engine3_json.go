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

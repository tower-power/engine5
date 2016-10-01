// ENGINE SYNC
//
// Package for manage power engine data
// Synchronization
//
//
package engine3

import (
	"database/sql"
	//	"errors"
	"fmt"
	_ "github.com/lib/pq"
	//"log"
	//	"os"
	//	"strings"
	//	"sync"
)

type Thing struct {
	ckey    []byte
	cval    []byte
	url     []byte
	data    []byte
	clockid int64
	tsn     int64
}

type Things []Thing

/* read sql Rows into Thing structure
 *
 * the assumed position in the rows is
 * $1  ckey
 * $2  cval
 * $3  ulr
 * $4  data
 * $5  clockid
 * $6  tsn
 */
func rowsToThings(rows *sql.Rows) Things {
	var (
		t      Thing
		result Things
		err    error
	)

	checkRows("Things", rows)

	for i := 0; rows.Next(); i++ {
		err := rows.Scan(&t.ckey, &t.cval, &t.url, &t.data, &t.clockid, &t.tsn)
		checkErr("scan things", err)

		result = append(result, t)
	}
	err = rows.Err()
	checkErr("end reading things loop", err)

	fmt.Printf("returning things: %d rows\n", len(result))
	return result
}

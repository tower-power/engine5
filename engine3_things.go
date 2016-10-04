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

/*
func getThings(dbconnect *sql.DB, in_url string, in_table string) Things {

	var out_value
	var statement string = "select * from " + "get" + in_table + "( $1 )"

	row := dbconnect.QueryRow(statement, in_url)
	checkRow(row)

	err := row.Scan(&out_value)
	checkErr(statement, err)


}
*/
/*
// Put a new value
func putPowerData(dbconnect *sql.DB, in_key string, in_value string) {

	_, err := dbconnect.Exec("select power.put( $1, $2 )", in_key, in_value)

	checkErr("power.put", err)

	return
}

// delete an entry
func deletePowerData(dbconnect *sql.DB, in_key string) {

	_, err := dbconnect.Exec("select power.delete( $1 )", in_key)

	checkErr("power.delete", err)

	return
}
*/

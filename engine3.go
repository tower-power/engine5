// ENGINE NODES
//
// Package for manage power engine data
//
// The package offers RESTful functions to store and retrieve objects
// Objects are versioned using a sequence number generator
//
package engine3

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	//"log"
	//	"os"
	//	"strings"
	//	"sync"
)

type HighWaterMark struct {
	clockid int64
	tsn     int64
}

type HighWaterMarks []HighWaterMark

// helper function for error handling (go panic!)
func checkRows(trace string, rows *sql.Rows) {

	cols, err := rows.Columns()
	if err != nil {
		panic(err.Error())
	}

	length := len(cols)

	fmt.Printf("%s ROWS: %v\n", trace, length)

	for i, colName := range cols {
		// var raw_value = *(values[i].(*interface{}))
		// var raw_type = reflect.TypeOf(raw_value)

		// fmt.Println(colName,raw_type,raw_value)
		fmt.Printf("%v %v\n", i, colName)
	}
}

/* read sql Rows into HighWaterMarks structure
 *
 * the assumed position in the rows is
 * $2  cockid
 * $3  tsn
 */
func rowsToHighWaterMarks(rows *sql.Rows) HighWaterMarks {
	var (
		hwm    HighWaterMark
		result []HighWaterMark
		err    error
	)

	checkRows("HighWaterMarks", rows)
	for i := 0; rows.Next(); i++ {
		err := rows.Scan(&hwm.clockid, &hwm.tsn)
		checkErr("scan high water mark", err)

		result = append(result, hwm)
	}
	err = rows.Err()
	checkErr("end loop", err)

	return result
}

/*
// helper function for error handling (go panic!)
func checkErr(trace string, err error) {

	if err != nil {
		fmt.Printf("ERROR: %#v\n", err)
		log.Panic(err)
	}

}

// helper function for tracing a SQL return row
// some better idea needed eventually (->tracing)
func checkRow(row *sql.Row) {

	// fmt.Printf( "ROW: %#v\n", row )

}
*/

// Calling database stored functions

// Retrieve a new TSN from database as int64
func newTSN2(dbconnect *sql.DB) int64 {

	var tsn int64

	row := dbconnect.QueryRow("select nodes.new_tsn()")
	checkRow(row)

	err := row.Scan(&tsn)
	checkErr("nodes: newTSN", err)

	return tsn
}

// Register node
func registerLocalNode(dbconnect *sql.DB, in_url string, in_data string) string {

	var out_id string

	row := dbconnect.QueryRow("select nodes.register( $1, $2 )", in_url, in_data)
	checkRow(row)

	err := row.Scan(&out_id)

	checkErr("nodes.register", err)

	return out_id
}

func getRemoteHighs(dbconnect *sql.DB) HighWaterMarks {

	/* select * from stored functions returns 'structure' */
	rows, err := dbconnect.Query("select * from nodes.getRemoteHighs()")
	defer rows.Close()

	checkErr("getRemoteHighs", err)

	return rowsToHighWaterMarks(rows)
}

// Check high water mark for clock
func checkHigh(dbconnect *sql.DB, in_clockid int64) int64 {

	var out_tsn int64

	row := dbconnect.QueryRow("select nodes.checkHigh( $1 )", in_clockid)
	checkRow(row)

	err := row.Scan(&out_tsn)

	checkErr("nodes.checkHigh", err)

	return out_tsn
}

//
// PACKAGE EXPORTS

// From a given database object retrieve the next TSN
//
// Package Export
func (db *Database) NewNodesTSN() (tsn int64, err error) {

	defer func() {

		if r := recover(); r != nil {
			// recover from panic
			err = errors.New("error while reading nodes TSN")

		}

	}()

	tsn = newTSN(db.dbconnect)
	return
}

// Intial Registration
//
// Package Export
func (db *Database) RegisterLocalNode(in_url string, in_data string) (out_value string, err error) {

	defer func() {

		if r := recover(); r != nil {
			// recover from panic
			err = errors.New("error while register node")

		}

	}()

	out_value = registerLocalNode(db.dbconnect, in_url, in_data)

	return out_value, err
}

// Read received HighWaterMarks for remote nodes
//
// Package Export
func (db *Database) GetRemoteHighs() (hwms HighWaterMarks, err error) {

	defer func() {

		if r := recover(); r != nil {
			// recover from panic
			err = errors.New("error while getting remote high water marks")

		}

	}()

	hwms = getRemoteHighs(db.dbconnect)
	return hwms, err
}

// Check HighWater mark on remote node (with cutoff value)
//
// Package Export
func (db *Database) CheckHigh(in_clockid int64) (out_tsn int64, err error) {

	defer func() {

		if r := recover(); r != nil {
			// recover from panic
			err = errors.New("error while getting remote high water marks")

		}

	}()
	out_tsn = checkHigh(db.dbconnect, in_clockid)
	return
}

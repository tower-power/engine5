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

func checkRows(trace string, rows *sql.Rows) {

	cols, err := rows.Columns()
	if err != nil {
		panic(err.Error())
	}

	length := len(cols)

	fmt.Printf("%s COLS: %v\n", trace, length)

	for i, colName := range cols {
		// var raw_value = *(values[i].(*interface{}))
		// var raw_type = reflect.TypeOf(raw_value)

		// fmt.Println(colName,raw_type,raw_value)
		fmt.Printf("%v %v\n", i, colName)
	}
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
//
// Register node at central server locally
//
func registerLocalNode(dbconnect *sql.DB, in_url string, in_data []byte) int64 {

	var out_id int64

	row := dbconnect.QueryRow("select nodes.register( $1, $2 )", in_url, in_data)
	checkRow(row)

	err := row.Scan(&out_id)

	checkErr("nodes.register", err)

	return out_id
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
func (db *Database) RegisterLocalNode(in_url string, in_data []byte) (out_value int64, err error) {

	defer func() {

		if r := recover(); r != nil {
			// recover from panic
			err = errors.New("error while register node")

		}

	}()

	out_value = registerLocalNode(db.dbconnect, in_url, in_data)

	return out_value, err
}

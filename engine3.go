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
// Register master node
//
func registerMasterNode(dbconnect *sql.DB, in_url string, in_data []byte) int64 {

	var out_id int64

	row := dbconnect.QueryRow("select nodes.register( $1, $2 )", in_url, in_data)
	checkRow(row)

	err := row.Scan(&out_id)

	checkErr("nodes.register", err)

	return out_id
}

// Register node
//
// Register local node (with clockiD generation)
//
func registerLocalNode(master *sql.DB, dbconnect *sql.DB, in_url string, in_data []byte) int64 {

	var out_id int64

	row := master.QueryRow("select nodes.register( $1, $2 )", in_url, in_data)
	checkRow(row)

	err := row.Scan(&out_id)

	checkErr("master nodes.register", err)

	row = dbconnect.QueryRow("select nodes.setmyclockid( $1 )", out_id)
	checkErr("set clockid", err)

	return registerLocalClockID(dbconnect, out_id)
}

// Register node
//
// Register local node to master node (without clockiD generation)
//
func registerLocalNodeToMaster(master *sql.DB, in_url string, in_data []byte) int64 {

	var out_id int64

	row := master.QueryRow("select nodes.register( $1, $2 )", in_url, in_data)
	checkRow(row)

	err := row.Scan(&out_id)

	checkErr("master nodes.register", err)

	return out_id
}

// Register node
//
// Register clockiD to local node
//
func registerLocalClockID(dbconnect *sql.DB, out_id int64) int64 {
	_, err := dbconnect.Exec("select nodes.setmyclockid( $1 )", out_id)

	checkErr("set clockid", err)

	return out_id
}

// Nodes Rest Functions
//
// GET
// PUT
// DELETE
//
// ....
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
func (db *Database) RegisterMasterNode(in_url string, in_data []byte) (out_value int64, err error) {

	defer func() {

		if r := recover(); r != nil {
			// recover from panic
			err = errors.New("error while register node")

		}

	}()

	out_value = registerMasterNode(db.dbconnect, in_url, in_data)

	return out_value, err
}

// Intial Registration
//
// Package Export
func (db *Database) RegisterLocalNode(local *Database, in_url string, in_data []byte) (out_value int64, err error) {

	defer func() {

		if r := recover(); r != nil {
			// recover from panic
			err = errors.New("error while register node")

		}

	}()

	out_value = registerLocalNode(db.dbconnect, local.dbconnect, in_url, in_data)

	return out_value, err
}

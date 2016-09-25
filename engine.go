// ENGINE
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
	"log"
	"os"
	"strings"
	"sync"
)

// Database Instance
//
//
// We assume that we have one wallclock per database instance
//
type Database struct {
	dbconnect *sql.DB // database connection (from database/sql, is pooled)
	name      string
	dbname    string
}

// the global list of database instances known in the process
//
// we can have many databases and many wallclocks
var databases = map[string]*Database{}

// a Read-write mutex to protect it`
var databasesRWLock sync.RWMutex

// database connect string,
// to be read from configuration
//
// We read from the environment later (secure store needed eventually)
var dbTemplate string = "user=rainer dbname=$database$ sslmode=disable"

// helper function for tracing (some better idea needed)
func checkString(name, value string) {

	fmt.Printf("CHECK: %v %v\n", name, value)

}

// get the database connection string template (from environment for now)
func getDbTemplate() string {
	if dbTemplate == "" {
		dbTemplate = os.Getenv("ENGINE_DB")
	}
	return dbTemplate
}

// translate database name into db connection string
//
// conncetion string needs to have a $database$ variable to be replaced by the database name
//
func dbname(name string) string {
	dn := strings.Replace(getDbTemplate(), "$database$", name, 1)

	checkString("generated database name", dn)
	return dn
}

// get a database object for a given name
func get(name string) *Database {

	// lock global map for reading
	databasesRWLock.RLock()
	db, ok := databases[name]
	databasesRWLock.RUnlock()

	if ok {
		checkString("get database ok", name)
		return db
	}

	db = add(name)
	return db
}

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

// Test database connections
//
// Initial test for live database connection`
func ping(dbconnect *sql.DB) {
	var err error

	err = dbconnect.Ping()

	checkErr("ping", err)
}

// Add a new database connection
func add(name string) *Database {

	db := new(Database)

	db.name = name
	db.dbname = dbname(name)
	dbconnect, err := sql.Open("postgres", db.dbname)
	checkErr("add", err)

	db.dbconnect = dbconnect

	// lock for writing
	databasesRWLock.Lock()
	databases[name] = db
	databasesRWLock.Unlock()

	// ping
	ping(dbconnect)

	return db
}

// Calling database stored functions

// Retrieve a new TSN from database as int64
func newTSN(dbconnect *sql.DB) int64 {

	var tsn int64

	row := dbconnect.QueryRow("select nodes.new_tsn()")
	checkRow(row)

	err := row.Scan(&tsn)
	checkErr("newTSN", err)

	return tsn
}

// Put a new value
func putPowerData(dbconnect *sql.DB, in_key string, in_value string) {

	_, err := dbconnect.Exec("select power.put( $1, $2 )", in_key, in_value)

	checkErr("power.put", err)

	return
}

// get a value
func getPowerData(dbconnect *sql.DB, in_key string) string {

	var out_value sql.NullString

	row := dbconnect.QueryRow("select power.get( $1 )", in_key)
	checkRow(row)

	err := row.Scan(&out_value)
	checkErr("getPowerData", err)

	if out_value.Valid == true {
		return out_value.String
	} else {
		return ""
		/* NULL value means not there */
	}

}

// delete an entry
func deletePowerData(dbconnect *sql.DB, in_key string) {

	_, err := dbconnect.Exec("select power.delete( $1 )", in_key)

	checkErr("power.delete", err)

	return
}

//
// PACKAGE EXPORTS

// Get the database for a given name
//
// Package export
func GetDatabase(name string) (db *Database, err error) {

	defer func() {

		if r := recover(); r != nil {
			// recover from panic
			err = errors.New("cannot create database connection")

		}

	}()

	db = get(name)

	return db, err
}

// From a given database object retrieve the next TSN
//
// Package Export
func (db *Database) NewTSN() (tsn int64, err error) {

	defer func() {

		if r := recover(); r != nil {
			// recover from panic
			err = errors.New("error while reading TSN")

		}

	}()

	tsn = newTSN(db.dbconnect)
	return
}

// Put power.data
//
// Package Export
func (db *Database) PutPowerData(in_key string, in_value string) (err error) {

	defer func() {

		if r := recover(); r != nil {
			// recover from panic
			err = errors.New("error while inserting power data")

		}

	}()

	putPowerData(db.dbconnect, in_key, in_value)
	return
}

// Get Power.data
//
// Package Export
func (db *Database) GetPowerData(in_key string) (out_value string, err error) {

	defer func() {

		if r := recover(); r != nil {
			// recover from panic
			err = errors.New("error while getting power data")

		}

	}()

	out_value = getPowerData(db.dbconnect, in_key)

	return out_value, err
}

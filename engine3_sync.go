// ENGINE SYNC
//
// Package for manage power engine data
// Synchronization
//
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

type Oplog struct {
	table_name string
	clockid    int64
	tsn        int64
	op         string
}

type HighWaterMarks []HighWaterMark
type Oplogs []Oplog

/* read sql Rows into HighWaterMarks structure
 *
 * the assumed position in the rows is
 * $1  clockid
 * $2  tsn
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

	fmt.Printf("returning HWM: %d rows\n", len(result))
	return result
}

/* read sql Rows into oplog structure
 *
 * the assumed position in the rows is
 * $1  table_name
 * $2  clockid
 * $3  tsn
 * $4  op(code) (I, U, D)
 */
func rowsToOplogs(rows *sql.Rows) Oplogs {
	var (
		ol     Oplog
		result Oplogs
		err    error
	)

	checkRows("Oplogs", rows)
	for i := 0; rows.Next(); i++ {
		err := rows.Scan(&ol.table_name, &ol.clockid, &ol.tsn, &ol.op)
		checkErr("scan operation log", err)

		result = append(result, ol)
	}
	err = rows.Err()
	checkErr("end loop", err)

	fmt.Printf("returning OPS: %d rows\n", len(result))
	return result
}

// Calling database stored functions

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

func getOpLogs(dbconnect *sql.DB, in_clockid int64, in_tsn int64) Oplogs {

	/* select * from stored functions returns 'structure' */
	rows, err := dbconnect.Query("select * from nodes.getOplogTail( $1, $2)", in_clockid, in_tsn)
	defer rows.Close()

	checkErr("get op logs", err)

	return rowsToOplogs(rows)
}

/*
 * Enti-entropy sync from dbconnect1 to dbconnect2
 */
func databaseSync(dbconnect1 *sql.DB, dbconnect2 *sql.DB) {
	var hwms2 HighWaterMarks
	var oplogs1 Oplogs

	hwms2 = getRemoteHighs(dbconnect2)

	fmt.Printf("HWM received: %d rows\n", len(hwms2))
	for i, hwm := range hwms2 {
		fmt.Printf("HWM %d %v\n", i, hwm)
	}

	oplogs1 = getOpLogs(dbconnect1, 0, 0)
	fmt.Printf("OPS received: %d rows\n", len(oplogs1))
	for i, ol := range oplogs1 {
		fmt.Printf("OPS %d %v\n", i, ol)
	}

}

//
// PACKAGE EXPORTS

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

func (db *Database) GetOpLogs(in_clockid int64, in_tsn int64) (out_ols Oplogs, err error) {

	defer func() {

		if r := recover(); r != nil {
			// recover from panic
			err = errors.New("error while oplog")

		}

	}()
	out_ols = getOpLogs(db.dbconnect, in_clockid, in_tsn)
	return
}

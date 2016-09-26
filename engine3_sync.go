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
	//	"fmt"
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

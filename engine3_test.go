//
// Test suite for engine2.go
//

package engine3

import (
	"fmt"
	"testing"
)

var dbname1 = "engine3"
var dbname2 = "engine4"

func jsonSystems_Nodes() []byte {
	firstSystem := Systems{Name: "nodes"}

	return toJson(firstSystem)
}

func TestInit(t *testing.T) {

	fmt.Printf("INIT:\n")
	_, err := GetDatabase(dbname1)

	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	fmt.Printf("Connect to database %v\n", dbname1)

	_, err = GetDatabase(dbname2)

	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	fmt.Printf("Connect to database %v\n", dbname2)
}

func TestRegister(t *testing.T) {

	var id int64

	fmt.Printf("REGISTER:\n")
	db, err := GetDatabase(dbname1)

	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	id, err = db.RegisterLocalNode("test3.towerpower.co", jsonSystems_Nodes())
	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}
	fmt.Printf("Register %v to %d\n", dbname1, id)

	db, err = GetDatabase(dbname1)

	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	id, err = db.RegisterLocalNode("test4.towerpower.co", jsonSystems_Nodes())
	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}
	fmt.Printf("Register %v to %d\n", dbname2, id)

}

func TestNewTSN(t *testing.T) {

	var tsn int64

	fmt.Printf("NEW TSN:\n")

	db, err := GetDatabase("engine3")

	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	tsn, err = db.NewNodesTSN()
	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	fmt.Printf("1 x NEW TSN from engine2 %v\n", tsn)

}

func TestGetRemoteHighs(t *testing.T) {
	var hwms HighWaterMarks

	fmt.Printf("GETREMOTEHIGHS:\n")
	db, err := GetDatabase("engine3")

	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	hwms, err = db.GetRemoteHighs()
	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	fmt.Printf("GetRemoteHighs Len %v Cap %v\n%v\n", len(hwms), cap(hwms), hwms)

}

func TestCheckHigh(t *testing.T) {
	var high int64

	fmt.Printf("CHECKHIGH:\n")

	clockid := int64(19)

	db, err := GetDatabase("engine3")

	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	high, err = db.CheckHigh(clockid)

	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	fmt.Printf("TestCheckHigh %v:  %v\n", clockid, high)

	high, err = db.CheckHigh(clockid)

	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	fmt.Printf("TestCheckHigh %v : %v\n", clockid, high)
}

func TestGetOpLog(t *testing.T) {
	var ol Oplogs

	fmt.Printf("GETOPLOGS:\n")
	db, err := GetDatabase("engine3")

	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	ol, err = db.GetOpLogs(0, 0)
	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	fmt.Printf("Get OPLOGS Len %v Cap %v\n%v\n", len(ol), cap(ol), ol)
}

func TestSync(t *testing.T) {

	fmt.Printf("SYNC:\n")
	db1, err1 := GetDatabase("engine3")

	if err1 != nil {
		fmt.Printf("PANIC %#v\n", err1)
		t.FailNow()
	}
	db2, err2 := GetDatabase("master")
	if err2 != nil {
		fmt.Printf("PANIC %#v\n", err2)
		t.FailNow()
	}

	databaseSync(db2.dbconnect, db1.dbconnect)
}

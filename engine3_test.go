//
// Test suite for engine2.go
//

package engine3

import (
	"fmt"
	"testing"
)

func TestInit(t *testing.T) {
	dbname1 := "engine3"
	dbname2 := "engine4"

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

	var uuid string

	fmt.Printf("REGISTER:\n")
	db, err := GetDatabase("engine3")

	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	uuid, err = db.RegisterLocalNode("test.towerpower.co", "{ \"name\" : \"nodes\" }")
	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	fmt.Printf("Register to %v\n", uuid)

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

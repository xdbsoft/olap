package olap

import (
	"reflect"
	"testing"
)

func createCube() Cube {
	c := Cube{
		Dimensions: []string{"Year", "Month"},
		Fields:     []string{"Count", "PercentOk"},
		Points: [][]interface{}{
			[]interface{}{2018, "Jan"},
			[]interface{}{2018, "Feb"},
			[]interface{}{2017, "Jan"},
		},
		Data: [][]interface{}{
			[]interface{}{100, 0.05},
			[]interface{}{300, 0.01},
			[]interface{}{200, 0.5},
		},
	}

	return c
}

func TestCreation(t *testing.T) {

	c := createCube()

	//IsValid
	if err := c.IsValid(); err != nil {
		t.Error("Cube should be valid")
	}

	//Headers
	headers := c.Headers()
	expectedHeaders := []string{"Year", "Month", "Count", "PercentOk"}

	if !reflect.DeepEqual(expectedHeaders, headers) {
		t.Error("Unexpected headers", headers)
	}

	//Rows
	rows := c.Rows()
	expectedRows := [][]interface{}{
		[]interface{}{2018, "Jan", 100, 0.05},
		[]interface{}{2018, "Feb", 300, 0.01},
		[]interface{}{2017, "Jan", 200, 0.5},
	}

	if !reflect.DeepEqual(expectedRows, rows) {
		t.Error("Unexpected rows", rows)
	}
}

func TestSlice(t *testing.T) {

	c := createCube()
	c = c.Slice("Year", 2018)

	//IsValid
	if err := c.IsValid(); err != nil {
		t.Error("Cube should be valid")
	}

	//Headers
	headers := c.Headers()
	expectedHeaders := []string{"Month", "Count", "PercentOk"}

	if !reflect.DeepEqual(expectedHeaders, headers) {
		t.Error("Unexpected headers", headers)
	}

	//Rows
	rows := c.Rows()
	expectedRows := [][]interface{}{
		[]interface{}{"Jan", 100, 0.05},
		[]interface{}{"Feb", 300, 0.01},
	}

	if !reflect.DeepEqual(expectedRows, rows) {
		t.Error("Unexpected rows", rows)
	}
}

func TestDice(t *testing.T) {

	c := createCube()
	c = c.Dice(func(point []interface{}) bool {
		return point[1] == "Feb"
	})

	//IsValid
	if err := c.IsValid(); err != nil {
		t.Error("Cube should be valid")
	}

	//Headers
	headers := c.Headers()
	expectedHeaders := []string{"Year", "Month", "Count", "PercentOk"}

	if !reflect.DeepEqual(expectedHeaders, headers) {
		t.Error("Unexpected headers", headers)
	}

	//Rows
	rows := c.Rows()
	expectedRows := [][]interface{}{
		[]interface{}{2018, "Feb", 300, 0.01},
	}

	if !reflect.DeepEqual(expectedRows, rows) {
		t.Error("Unexpected rows", rows)
	}
}

func sum(aggregate, value []interface{}) []interface{} {

	s := aggregate[0].(int)

	s += value[0].(int)

	return []interface{}{s}

}

func TestRollUp(t *testing.T) {

	c := createCube()
	c = c.RollUp("Year", []string{"Sum"}, sum, []interface{}{0})

	//IsValid
	if err := c.IsValid(); err != nil {
		t.Error("Cube should be valid")
	}

	//Headers
	headers := c.Headers()
	expectedHeaders := []string{"Year", "Sum"}

	if !reflect.DeepEqual(expectedHeaders, headers) {
		t.Error("Unexpected headers", headers)
	}

	//Rows
	rows := c.Rows()
	expectedRows := [][]interface{}{
		[]interface{}{2018, 400},
		[]interface{}{2017, 200},
	}

	if !reflect.DeepEqual(expectedRows, rows) {
		t.Error("Unexpected rows", rows)
	}
}

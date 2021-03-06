package olap

import (
	"reflect"
	"testing"
)

func createCube() Cube {
	c := Cube{
		Dimensions: []string{"Year", "Month", "Product"},
		Fields:     []string{"Count", "PercentOk"},
		Points: [][]interface{}{
			{2018, "Jan", "A"},
			{2018, "Feb", "A"},
			{2018, "Feb", "B"},
			{2017, "Jan", "A"},
			{2017, "Jan", "B"},
		},
		Data: [][]interface{}{
			{100, 0.05},
			{300, 0.01},
			{100, 0.1},
			{200, 0.5},
			{200, 0.1},
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
	expectedHeaders := []string{"Year", "Month", "Product", "Count", "PercentOk"}

	if !reflect.DeepEqual(expectedHeaders, headers) {
		t.Error("Unexpected headers", headers)
	}

	//Rows
	rows := c.Rows()
	expectedRows := [][]interface{}{
		{2018, "Jan", "A", 100, 0.05},
		{2018, "Feb", "A", 300, 0.01},
		{2018, "Feb", "B", 100, 0.1},
		{2017, "Jan", "A", 200, 0.5},
		{2017, "Jan", "B", 200, 0.1},
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
	expectedHeaders := []string{"Month", "Product", "Count", "PercentOk"}

	if !reflect.DeepEqual(expectedHeaders, headers) {
		t.Error("Unexpected headers", headers)
	}

	//Rows
	rows := c.Rows()
	expectedRows := [][]interface{}{
		{"Jan", "A", 100, 0.05},
		{"Feb", "A", 300, 0.01},
		{"Feb", "B", 100, 0.1},
	}

	if !reflect.DeepEqual(expectedRows, rows) {
		t.Error("Unexpected rows", rows)
	}
}

func TestDice(t *testing.T) {

	c := createCube()
	c = c.Dice(func(cube Cube, idx int) bool {
		return cube.Points[idx][1] == "Feb"
	})

	//IsValid
	if err := c.IsValid(); err != nil {
		t.Error("Cube should be valid")
	}

	//Headers
	headers := c.Headers()
	expectedHeaders := []string{"Year", "Month", "Product", "Count", "PercentOk"}

	if !reflect.DeepEqual(expectedHeaders, headers) {
		t.Error("Unexpected headers", headers)
	}

	//Rows
	rows := c.Rows()
	expectedRows := [][]interface{}{
		{2018, "Feb", "A", 300, 0.01},
		{2018, "Feb", "B", 100, 0.1},
	}

	if !reflect.DeepEqual(expectedRows, rows) {
		t.Error("Unexpected rows", rows)
	}
}

func sum(aggregate, value []interface{}) []interface{} {

	s := aggregate[0].(int)
	pct, ok := aggregate[1].(float64)
	if !ok {
		pct = 1.0
	}

	count := value[0].(int)
	percentage := value[1].(float64)

	if s+count > 0 {
		pct = (float64(s)*pct + float64(count)*percentage) / float64(s+count)
	}

	s += value[0].(int)

	return []interface{}{s, pct}

}
func TestRollUp_singleDimension(t *testing.T) {

	c := createCube()
	c = c.RollUp([]string{"Year"}, []string{"Sum", "PercentOk"}, sum, []interface{}{0, nil})

	//IsValid
	if err := c.IsValid(); err != nil {
		t.Error("Cube should be valid: ", err)
	}

	//Headers
	headers := c.Headers()
	expectedHeaders := []string{"Year", "Sum", "PercentOk"}

	if !reflect.DeepEqual(expectedHeaders, headers) {
		t.Error("Unexpected headers", headers)
	}

	//Rows
	rows := c.Rows()
	expectedRows := [][]interface{}{
		{2018, 500, 0.036},
		{2017, 400, 0.3},
	}

	if !reflect.DeepEqual(expectedRows, rows) {
		t.Error("Unexpected rows", rows)
	}
}

func TestRollUp_multiDimensions(t *testing.T) {

	c := createCube()
	c = c.RollUp([]string{"Year", "Month"}, []string{"Sum", "PercentOk"}, sum, []interface{}{0, nil})

	//IsValid
	if err := c.IsValid(); err != nil {
		t.Error("Cube should be valid: ", err)
	}

	//Headers
	headers := c.Headers()
	expectedHeaders := []string{"Year", "Month", "Sum", "PercentOk"}

	if !reflect.DeepEqual(expectedHeaders, headers) {
		t.Error("Unexpected headers", headers)
	}

	//Rows
	rows := c.Rows()
	expectedRows := [][]interface{}{
		{2018, "Jan", 100, 0.05},
		{2018, "Feb", 400, 0.0325},
		{2017, "Jan", 400, 0.3},
	}

	if !reflect.DeepEqual(expectedRows, rows) {
		t.Error("Unexpected rows", rows)
	}
}

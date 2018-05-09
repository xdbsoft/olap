package olap

import (
	"errors"
)

//Cube is an OLAP cube
type Cube struct {
	Dimensions []string        `json:"dimensions,omitempty"`
	Points     [][]interface{} `json:"points,omitempty"`
	Fields     []string        `json:"fields,omitempty"`
	Data       [][]interface{} `json:"data,omitempty"`
}

//Headers return the name of the columns for the Rows method.
func (c Cube) Headers() []string {

	headers := make([]string, len(c.Dimensions)+len(c.Fields))

	copy(headers, c.Dimensions)
	copy(headers[len(c.Dimensions):], c.Fields)

	return headers
}

//Rows return the Cube content as list of rows. Headers are available via Headers method.
func (c Cube) Rows() [][]interface{} {

	rows := make([][]interface{}, 0, len(c.Points))

	for i := range c.Points {

		row := make([]interface{}, len(c.Dimensions)+len(c.Fields))

		copy(row, c.Points[i])
		copy(row[len(c.Dimensions):], c.Data[i])

		rows = append(rows, row)
	}

	return rows
}

//IsValid verify that a Cube is properly instentiated
func (c Cube) IsValid() error {

	if len(c.Data) > 0 {

		for _, pt := range c.Points {
			if len(pt) != len(c.Dimensions) {
				return errors.New("invalid point")
			}
		}

		for _, d := range c.Data {
			if len(d) != len(c.Fields) {
				return errors.New("invalid slice")
			}
		}

		if len(c.Data) != len(c.Points) {
			return errors.New("orphan slices")
		}

	}

	return nil
}

//AddRows add a set of rows to the cube.
func (c *Cube) AddRows(header []string, rows [][]interface{}) error {

	if len(header) != (len(c.Dimensions) + len(c.Fields)) {
		return errors.New("Invalid header")
	}

	dimIndexes := make([]int, len(c.Dimensions))
	for i, d := range c.Dimensions {
		found := false
		for j, h := range header {
			if d == h {
				dimIndexes[i] = j
				found = true
				break
			}
		}
		if !found {
			return errors.New("Dimension not found")
		}
	}

	fldIndexes := make([]int, len(c.Fields))
	for i, f := range c.Fields {
		found := false
		for j, h := range header {
			if f == h {
				fldIndexes[i] = j
				found = true
				break
			}
		}
		if !found {
			return errors.New("Dimension not found")
		}
	}

	for _, row := range rows {

		point := make([]interface{}, len(c.Dimensions))
		data := make([]interface{}, len(c.Fields))

		for i := range point {
			point[i] = row[dimIndexes[i]]
		}
		for i := range data {
			data[i] = row[fldIndexes[i]]
		}

		c.Points = append(c.Points, point)
		c.Data = append(c.Data, data)
	}

	return nil
}

func sliceStringArray(a []string, idx int) []string {
	n := make([]string, len(a)-1)
	copy(n, a[:idx])
	copy(n[idx:], a[idx+1:])
	return n
}

func copyStringArray(a []string) []string {
	n := make([]string, len(a))
	copy(n, a)
	return n
}

func sliceInterfaceArray(a []interface{}, idx int) []interface{} {
	n := make([]interface{}, len(a)-1)
	copy(n, a[:idx])
	copy(n[idx:], a[idx+1:])
	return n
}

func copyInterfaceArray(a []interface{}) []interface{} {
	n := make([]interface{}, len(a))
	copy(n, a)
	return n
}

func indexOf(v string, array []string) int {

	idx := -1
	for i, d := range array {
		if d == v {
			idx = i
		}
	}
	if idx < 0 {
		panic(errors.New("Not found"))
	}
	return idx

}

//Slice operator picks a rectangular subset of a cube by choosing a single value of its dimensions.
func (c Cube) Slice(dimension string, value interface{}) Cube {

	newCube := Cube{}

	// Dimensions
	dimIndex := indexOf(dimension, c.Dimensions)
	newCube.Dimensions = sliceStringArray(c.Dimensions, dimIndex)

	// Fields
	newCube.Fields = copyStringArray(c.Fields)

	// Points + Data
	for i, pt := range c.Points {
		if pt[dimIndex] == value {

			newPt := sliceInterfaceArray(pt, dimIndex)
			newData := copyInterfaceArray(c.Data[i])

			newCube.Points = append(newCube.Points, newPt)
			newCube.Data = append(newCube.Data, newData)
		}
	}

	return newCube
}

// Dice operator picks a subcube by choosing a specific values of multiple dimensions.
func (c Cube) Dice(selector func(point []interface{}) bool) Cube {

	newCube := Cube{}
	newCube.Dimensions = copyStringArray(c.Dimensions)
	newCube.Fields = copyStringArray(c.Fields)

	// Points + Data
	for i, pt := range c.Points {
		if selector(pt) {
			newPt := copyInterfaceArray(c.Points[i])
			newData := copyInterfaceArray(c.Data[i])

			newCube.Points = append(newCube.Points, newPt)
			newCube.Data = append(newCube.Data, newData)
		}
	}

	return newCube
}

//Aggregator is a summarization method to be used by RollUp operator
type Aggregator func(aggregate, value []interface{}) []interface{}

//RollUp operator summarize the data along multiple dimensions.
//Ex: rollup(['year','month'], ['flights'], (sum, value) => [sum[0]+value[0]], [0])
func (c Cube) RollUp(dimensions []string, fields []string, aggregator Aggregator, initialValue []interface{}) Cube {

	newCube := Cube{}
	newCube.Dimensions = dimensions
	newCube.Fields = copyStringArray(fields)

	dimIndexes := make([]int, 0, len(dimensions))
	for _, dimension := range dimensions {
		dimIndex := indexOf(dimension, c.Dimensions)

		dimIndexes = append(dimIndexes, dimIndex)
	}

	for i, originalPoint := range c.Points {

		newPt := make([]interface{}, 0, len(dimensions))
		for _, dimIndex := range dimIndexes {
			k := originalPoint[dimIndex]
			newPt = append(newPt, k)
		}

		found := -1
		for j, pt := range newCube.Points {
			match := true
			for k, key := range pt {
				if key != newPt[k] {
					match = false
				}
			}
			if match {
				found = j
				break
			}
		}

		if found < 0 {
			found = len(newCube.Points)
			newCube.Points = append(newCube.Points, newPt)
			newCube.Data = append(newCube.Data, copyInterfaceArray(initialValue))

		}
		value := newCube.Data[found]

		newValue := aggregator(value, c.Data[i])

		newCube.Data[found] = newValue
	}

	return newCube
}

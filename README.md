# olap
OLAP cube for in memory data processing

[![GoDoc](https://godoc.org/github.com/xdbsoft/olap?status.svg)](https://godoc.org/github.com/xdbsoft/olap)
[![Go Report Card](https://goreportcard.com/badge/github.com/xdbsoft/olap)](https://goreportcard.com/report/github.com/xdbsoft/olap)

## Install

	go get github.com/xdbsoft/olap

## Use

    package main

    import (
        "fmt"
        "github.com/xdbsoft/olap"
    )
		
    func main() {
        cube := olap.Cube{
            Dimensions : []string{"year", "month", "product"},
            Fields: []string{"revenue"},
        }
    
        cube.AddRows([]string{"year", "month", "product", "revenue"}, [][]interface{}{
            {2017, "Jan", "apple", 100},
            {2017, "Jan", "orange", 80},
            {2018, "Jan", "apple", 120},
            {2018, "Jan", "orange", 40},
            {2018, "Feb", "apple", 75},
            {2018, "Feb", "orange", 75},
        })
    
        cube = cube.Slice("year", 2018)
        cube = cube.Rollup([]string{"product"}, cube.Fields, sum ,[]interface{}{0})

        fmt.Print(cube.Rows())
        /* The following lines will be printed:
         * apple, 195
         * orange, 115
         */
    }
  
    func sum(aggregate, value []interface{}) []interface{} {
        s := aggregate[0].(int)
        s += value[0].(int)
        return []interface{}{s}
    }


## Tests

`go test` is used for testing.


## License

This code is licensed under the MIT license. See [LICENSE](https://github.com/xdbsoft/olap/blob/master/LICENSE).

This code is inspired by https://github.com/fibo/OLAP-cube/ (javascript implementation of an OALP cube)

package main

import (
	"fmt"
	"time-series-engine/engine"
)

func main() {
	fmt.Println("A")
	e, err := engine.NewEngine()
	if err != nil {
		panic(err)
	}
	e.Run()
}

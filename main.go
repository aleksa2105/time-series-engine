package main

import (
	"time-series-engine/engine"
)

func main() {
	e, err := engine.NewEngine()
	if err != nil {
		panic(err)
	}
	e.Run()
}

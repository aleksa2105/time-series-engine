package main

import (
	"fmt"
)

func main() {
	mask := 0b11000000
	val := 0b00001011
	shifted := val << (8 - 2 - 4)
	result := mask | shifted
	fmt.Printf("%08b\n", result)
}

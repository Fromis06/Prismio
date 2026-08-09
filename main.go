package main

import (
	"fmt"
	"log"

	"my-cdc/cmd/cli"
)

func main() {
	fmt.Println("=====================================")
	fmt.Println("  Welcome to Prismio")
	fmt.Println("=====================================")

	log.Println("Starting Terminal UI...")
	cli.Run()
}
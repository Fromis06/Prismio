package main

import (
	"fmt"
	"log"

	"my-cdc/cmd/cli"
	"my-cdc/cmd/server"
)

func main() {
	fmt.Println("=====================================")
	fmt.Println("  Welcome to my-cdc Application")
	fmt.Println("=====================================")
	fmt.Println("Please choose a mode to run:")
	fmt.Println("  1. Interactive Terminal UI")
	fmt.Println("  2. Headless Server (for Web App or background processing)")
	fmt.Println("-------------------------------------")
	fmt.Print("Enter your choice (1 or 2): ")

	var choice int
	_, err := fmt.Scanln(&choice)
	if err != nil {
		log.Fatalf("Invalid input. Please enter a number (1 or 2). Error: %v", err)
	}

	switch choice {
	case 1:
		cli.Run()
	case 2:
		server.Run()
	default:
		log.Fatalf("Invalid choice: %d. Please run again and select 1 or 2.", choice)
	}
}

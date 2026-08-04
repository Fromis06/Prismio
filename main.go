package main

import (
	"flag"
	"fmt"
	"log"

	"my-cdc/cmd/cli"
	"my-cdc/cmd/server"
)

func main() {
	// Sử dụng flag để chọn chế độ chạy, thân thiện hơn với môi trường server/script.
	// Mặc định là "server".
	mode := flag.String("mode", "server", "Chế độ chạy ứng dụng: 'server' (headless) hoặc 'cli' (giao diện terminal).")
	flag.Parse()

	fmt.Println("=====================================")
	fmt.Println("  Welcome to my-cdc Application")
	fmt.Println("=====================================")

	switch *mode {
	case "cli":
		log.Println("Starting in Interactive Terminal UI mode...")
		cli.Run()
	case "server":
		log.Println("Starting in Headless Server mode...")
		server.Run()
	default:
		log.Fatalf("Invalid mode: '%s'. Please use 'server' or 'cli'.", *mode)
	}
}

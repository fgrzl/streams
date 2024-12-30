package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/fgrzl/streams/pkg/grpc"
)

func main() {
	// Default ports if not set via args or environment variables
	defaultPorts := []string{":9180", ":9181"}

	// Check if ports are provided as command-line arguments
	args := os.Args[1:] // Skip the program name
	var ports []string

	if len(args) > 0 {
		ports = args
	} else {
		// Fallback to environment variable
		envPorts := os.Getenv("PORTS")
		if envPorts != "" {
			ports = strings.Split(envPorts, ",") // Assume comma-separated ports
		} else {
			// Fallback to default ports
			ports = defaultPorts
		}
	}

	log.Printf("Starting server on ports: %v", ports)
	grpc.StartServer(context.Background(), make(chan struct{}, 1), ports...)
}

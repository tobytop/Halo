package main

import (
	"context"
	"halo"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// client := halo.NewClient(ctx, "", func() map[string]halo.JobHandler {
	// 	handlers := make(map[string]halo.JobHandler)
	// 	return handlers
	// })
	// client.StartServer()
	// defer client.StopServer()

	consortor, _ := halo.NewConsortor("")
	server := halo.NewServer(ctx, 8000, consortor)
	server.StartServer()
}

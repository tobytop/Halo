package main

import (
	"context"
	"halo"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consortor, _ := halo.NewConsortor("")
	server := halo.NewServer(ctx, 8000, consortor)
	httpserver := halo.NewHttpServer(8081, consortor, server)

	go httpserver.StartServer()
	server.StartServer()
}

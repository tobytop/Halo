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
	server.StartServer()
	httpserver := halo.NewHttpServer(8080, consortor, server)
	httpserver.StartServer()
}

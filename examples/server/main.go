package main

import (
	"context"
	"halo"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consortor, _ := halo.NewConsortor("127.0.0.1:8500")
	halo.NewServer(ctx, 8000, halo.RoundRobin, consortor).
		BuilderHttpService(8081).StartAllServer()
}

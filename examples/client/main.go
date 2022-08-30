package main

import (
	"context"
	"halo"
	"log"
)

func main() {
	client := halo.NewClient(context.Background(), "127.0.0.1:8000", func() map[string]halo.JobHandler {
		handlers := make(map[string]halo.JobHandler)
		handlers["test"] = &TestTask{
			Name: "tsgggggg",
		}
		return handlers
	}, 1)
	defer client.StopServer()
	client.StartServer()
}

type TestTask struct {
	Name string
}

func (t *TestTask) Execute(ctx context.Context, worker interface{}) error {
	log.Print(t.Name)
	return nil
}

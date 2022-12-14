package main

import (
	"context"
	"halo"
	"log"
)

func main() {
	halo.NewDefaultClient("127.0.0.1:8000", func() map[string]halo.JobHandler {
		handlers := make(map[string]halo.JobHandler)
		handlers["test"] = &TestTask{
			Name: "tsgggggg",
		}
		return handlers
	}).StartServer()
}

type TestTask struct {
	Name string
}

func (t *TestTask) Execute(ctx context.Context, worker interface{}) error {
	log.Print(t.Name)
	return nil
}

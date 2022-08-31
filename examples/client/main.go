package main

import (
	"bufio"
	"context"
	"halo"
	"log"
	"os"
)

func main() {
	inputReader := bufio.NewReader(os.Stdin)
	client := halo.NewClient(context.Background(), "127.0.0.1:8000", func() map[string]halo.JobHandler {
		handlers := make(map[string]halo.JobHandler)
		handlers["test"] = &TestTask{
			Name: "tsgggggg",
		}
		return handlers
	}, 1)
	defer client.StopServer()
	go client.StartServer()
	for {
		input, _ := inputReader.ReadString('\n')
		if input[:len(input)-2] == "exit" {
			os.Exit(0)
		}
	}
}

type TestTask struct {
	Name string
}

func (t *TestTask) Execute(ctx context.Context, worker interface{}) error {
	log.Print(t.Name)
	return nil
}

package test

import (
	"context"
	"log"
	"testing"

	"github.com/go-kratos/blades"
	"github.com/go-kratos/blades/contrib/openai"
)

func TestAgent(t *testing.T) {
	model := openai.NewModel("doubao-seed-1-6-flash-250828", openai.Config{
		BaseURL: "https://ark.cn-beijing.volces.com/api/v3",
		APIKey:  "",
	})
	agent, err := blades.NewAgent(
		"Blades Agent",
		blades.WithModel(model),
		blades.WithInstruction("You are a helpful assistant that provides detailed and accurate information."),
	)
	if err != nil {
		log.Fatal(err)
	}
	// Create a new input message

	// Run the agent
	runner := blades.NewRunner(agent)
	stream := runner.RunStream(context.Background(), blades.UserMessage(""))
	for m, err := range stream {
		if err != nil {
			log.Fatal(err)
		}
		log.Println(m.Status, m.Text())
	}
}

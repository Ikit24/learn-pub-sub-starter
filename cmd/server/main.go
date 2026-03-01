package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

    "github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
    "github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
    "github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	connString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connString)
	if err != nil {
		fmt.Println("Couldn't dial:", err)
		return
	}
	defer conn.Close()
	fmt.Println("Connection successful")

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Couln't dial:", err)
		return
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	err = pubsub.PublishJSON(
		ch,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		routing.PlayingState{
			IsPaused: true,
		},
	)

	if err != nil {
		log.Printf("could not publish: %v", err)
	}

	gamelogic.PrintServerHelp()
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			fmt.Println("Sending a pause message...")
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
			if err != nil {
				log.Printf("could not publish pause: %v", err)
			}
		case "resume":
			fmt.Println("Sending a resume message...")
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
			if err != nil {
				log.Printf("could not publish resume: %v", err)
			}
		case "quit":
			fmt.Println("Goodbye!")
			return
		default:
			fmt.Println("unknown command")
		}
	}

	<-signalChan

	fmt.Println("Shutting down.. ")
}

package main

import (
	"log"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	
    "github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
    "github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
)

func main() {
	fmt.Println("Starting Peril client...")

	connString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connString)
	if err != nil {
		fmt.Println("Couldn't dial:", err)
		return
	}
	defer conn.Close()
	fmt.Println("Connection successful")

	username, _ := gamelogic.ClientWelcome()
	gs := gamelogic.NewGameState(username)

	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("couldn't create channel: %v", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		"pause." + username,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gs),
	)
	if err != nil {
			fmt.Println("Unable to subscribe:", err)
			return
		}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix + "." + username,
		routing.ArmyMovesPrefix + ".*",
		pubsub.SimpleQueueTransient,
		handlerMove(gs, publishCh),
	)
	if err != nil {
			fmt.Println("Unable to subscribe army movement:", err)
			return
		}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix + ".*",
		pubsub.SimpleQueueDurable,
		handlerWar(gs),
	)
	if err != nil {
			fmt.Println("Unable to subscribe war events:", err)
			return
		}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			err := gs.CommandSpawn(words)
			if err != nil {
				fmt.Println("Spawn failed:", err)
				continue
			}
		case "move":
			mv, err := gs.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix + "." + mv.Player.Username,
				mv,
				)
			if err != nil {
				fmt.Println("Error, couldn't publish", err)
				return
			}
			fmt.Println("Move successful!")
		case "status":
			gs.CommandStatus()
		case "quit":
			gamelogic.PrintQuit()
			return
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		default:
			fmt.Println("unknown command")
			continue
		}
	}
}

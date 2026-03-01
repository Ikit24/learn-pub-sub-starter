package main

import (
	"fmt"
	"os"
	"os/signal"
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
	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		"pause." + username,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
	)
	if err != nil {
		fmt.Println("Unable to connect:", err)
		return
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
}

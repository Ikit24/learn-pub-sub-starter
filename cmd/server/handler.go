package main

import (
	"fmt"
	"log"
)

func handlerLogs() func(gamelogic.GameLog) pubsub.Acktype {
	return func(gl gamelogic.GameLog) pubsub.Acktype {

	}
}

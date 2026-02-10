package pubsub

import (
	"encoding/json"
)

func Publish JSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	json, _ := json.Marshal(T)
}

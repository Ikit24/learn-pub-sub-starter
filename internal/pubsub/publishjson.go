package pubsub

import (
	"log"
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
    SimpleQueueDurable SimpleQueueType = iota
    SimpleQueueTransient
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        data,
		},
	)
}

func DeclareAndBind(
    conn *amqp.Connection,
    exchange,
    queueName,
    key string,
    queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
    ch, err := conn.Channel()
    if err != nil {
        return nil, amqp.Queue{}, err
    }

    queue, err := ch.QueueDeclare(
        queueName,
        queueType == SimpleQueueDurable,
        queueType != SimpleQueueDurable,
        queueType != SimpleQueueDurable,
        false,
        nil,
    )
    if err != nil {
        return nil, amqp.Queue{}, err
    }

    err = ch.QueueBind(queueName, key, exchange, false, nil)
    if err != nil {
        return nil, amqp.Queue{}, err
    }

    return ch, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for d := range msgs {
			var data T
			err := json.Unmarshal(d.Body, &data)
			if err != nil {
				log.Println(err)
				continue
			}
			handler(data)
			err = d.Ack(false)
			if err != nil {
				log.Println(err)
				continue
			}
		}
	}()

	return nil
}

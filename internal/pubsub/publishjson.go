package pubsub

import (
	"log"
	"fmt"
	"bytes"
	"context"
	"encoding/json"
	"encoding/gob"

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
        amqp.Table{
			"x-dead-letter-exchange":"peril_dlx",
		},
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

type Acktype int

const (
	Ack Acktype = iota
	NackDiscard
	NackRequeue
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	msgs, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
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
			ackType := handler(data)
			switch ackType {
			case Ack:
				d.Ack(false)
				fmt.Println("message is acknowledged")
			case NackRequeue:
				d.Nack(false, true)
				fmt.Println("message is requeued")
			case NackDiscard:
				d.Nack(false, false)
				fmt.Println("message is discarded")
			}
		}
	}()

	return nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(val)
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
			ContentType: "application/gob",
			Body:        buf.Bytes(),
		},
	)
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) Acktype,
	unmarshaller func([]byte) (T, error),
) error

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	return subscribe[T](
		conn, exchange, queueName, key, queueType, handler,
		func(data []byte) (T, error) {
			reader := bytes.NewBuffer(data)
			decoder := gob.NewDecoder(reader)
			var target T
			err := decoder.Decode(&target)
			return target, err
		},
	)
}

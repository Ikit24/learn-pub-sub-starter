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
) error {
	return subscribe[T] (
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			var target T
			err := json.Unmarshal(data, &target)
			return target, err
		},
	)
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
) error {    ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
    if err != nil {
        return fmt.Errorf("could not declare and bind queue: %v", err)
    }

	err = ch.Qos(10, 0, false)
	if err != nil {
		return fmt.Errorf("could not set QoS: %v", err)
	}

    msgs, err := ch.Consume(
        queue.Name, // queue
        "",         // consumer
        false,      // auto-ack
        false,      // exclusive
        false,      // no-local
        false,      // no-wait
        nil,        // args
    )
    if err != nil {
        return fmt.Errorf("could not consume messages: %v", err)
    }

    go func() {
        defer ch.Close()
        for msg := range msgs {
            target, err := unmarshaller(msg.Body)
            if err != nil {
                fmt.Printf("could not unmarshal message: %v\n", err)
                continue
            }
            switch handler(target) {
            case Ack:
                msg.Ack(false)
            case NackDiscard:
                msg.Nack(false, false)
            case NackRequeue:
                msg.Nack(false, true)
            }
        }
    }()
    return nil
}

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

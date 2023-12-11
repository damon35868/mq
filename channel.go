package mq

import (
	"log"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

/**
 * @description: 信道
 * @return {*}
 */
type Channel struct {
	*amqp.Channel
	closed int32
}

func (ch *Channel) IsClosed() bool {
	return atomic.LoadInt32(&ch.closed) == 1
}

func (ch *Channel) Close() error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&ch.closed, 1)

	return ch.Channel.Close()
}

func (ch *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)

	go func() {
		for {
			d, err := ch.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			if err != nil {
				log.Printf("[----消费失败----], %v", err)
				time.Sleep(delay * time.Second)
				continue
			}

			for msg := range d {
				deliveries <- msg
			}

			time.Sleep(delay * time.Second)

			if ch.IsClosed() {
				break
			}
		}
	}()

	return deliveries, nil
}

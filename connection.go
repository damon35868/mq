package mq

import (
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

/**
 * @description: 连接体
 * @return {*}
 */
type Connection struct {
	*amqp.Connection
}

const delay = 3 // 重连间隔 - 秒

func (c *Connection) Channel() (*Channel, error) {
	ch, err := c.Connection.Channel()

	if err != nil {
		return nil, err
	}

	channel := &Channel{
		Channel: ch,
	}

	// channel 同样重连机制
	go func() {
		for {
			reason, ok := <-channel.Channel.NotifyClose(make(chan *amqp.Error))

			if !ok || channel.IsClosed() {
				log.Println("[----channel 关闭----]")
				_ = channel.Close()
				break
			}

			log.Printf("[----channel 关闭----], %v", reason)

			for {
				time.Sleep(delay * time.Second)

				ch, err := c.Connection.Channel()
				if err == nil {
					log.Println("[----channel 重新创建成功----]")
					channel.Channel = ch
					break
				}

				log.Printf("[----channel 重新创建失败----], %v", err)
			}
		}

	}()

	return channel, nil
}

func Dial(url string) (*Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	connection := &Connection{
		Connection: conn,
	}

	// 建新进程处理重连
	go func() {
		for {
			reason, ok := <-connection.Connection.NotifyClose(make(chan *amqp.Error))

			if !ok {
				log.Println("[----连接关闭----]")
				break
			}
			log.Printf("[----连接关闭----], %v", reason)

			for {
				time.Sleep(delay * time.Second)

				conn, err := amqp.Dial(url)
				if err == nil {
					connection.Connection = conn
					log.Println("[----重连连接成功----]")
					break
				}

				log.Printf("[----重连连接失败----], %v", err)
			}
		}
	}()

	return connection, nil
}

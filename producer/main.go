package main

import (
	"context"
	"github.com/go-redis/redis/v8"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
	"zenskar-assignment/shared"
)

const streamName = "zenskar"
const maxLength = 10000

type producer struct {
	id     string
	client *redis.Client
	done   chan bool
}

func (p *producer) send(ctx context.Context, data []byte) (string, error) {
	response := p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		MaxLen: maxLength,
		Values: map[string]interface{}{
			"ts":   time.Now().UnixMilli(),
			"id":   shared.GenerateUuid(),
			"data": data,
		},
	})
	return response.Result()
}

func (p *producer) Start(ctx context.Context) {
	for {
		select {
		case <-p.done:
			return
		default:
			d := make([]byte, 1024*1024)
			_, _ = rand.Read(d)
			id, err := p.send(ctx, d)
			if err != nil {
				log.Printf("error happened while sending %s", err.Error())
			} else {
				log.Printf("message sent with id %s\n", id)
			}

			time.Sleep(100 * time.Millisecond)
		}
	}
}

func NewProducer(client *redis.Client) *producer {
	return &producer{client: client, id: shared.GenerateUuid(), done: make(chan bool)}
}

func main() {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   4,
	})
	p := NewProducer(client)

	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		log.Println("interrupt received")
		p.done <- true
	}()

	p.Start(context.TODO())
}

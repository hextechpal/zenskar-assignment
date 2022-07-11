package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Create an Amazon S3 service client
	rand.Seed(time.Now().UnixMilli())
	s3client := NewS3Client(fmt.Sprintf("zen-%d", rand.Intn(100)))
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   4, // use default DB
	})
	c := NewConsumer(rdb, s3client)

	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		log.Println("interrupt received")
		c.done <- true
	}()

	c.Start(context.TODO())
}

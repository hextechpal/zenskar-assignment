package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"io"
	"log"
	"strconv"
	"time"
	"zenskar-assignment/shared"
)

const groupName = "zenskarc"
const streamName = "zenskar"

type record struct {
	ID         string `json:"id"`
	ProducedAt int64  `json:"producedAt"`
	Data       []byte `json:"data"`
}

type consumer struct {
	id       string
	group    string
	rdb      *redis.Client
	s3Client *s3Client
	done     chan bool
}

func NewConsumer(rdb *redis.Client, s3Client *s3Client) *consumer {
	_ = rdb.XGroupCreateMkStream(context.TODO(), streamName, groupName, "$").Err()
	return &consumer{rdb: rdb, s3Client: s3Client, id: shared.GenerateUuid(), group: groupName, done: make(chan bool)}
}

func (c *consumer) read(ctx context.Context) error {
	claim := c.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   streamName,
		Group:    c.group,
		MinIdle:  15 * time.Second,
		Start:    "0-0",
		Count:    500,
		Consumer: c.id,
	})

	cms, _, err := claim.Result()

	if err != nil {
		return err
	}

	if len(cms) != 0 {
		go c.processMessages(cms)
		return nil
	}

	response := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.group,
		Consumer: c.id,
		Streams:  []string{streamName, ">"},
		Count:    500,
		Block:    10 * time.Millisecond,
		NoAck:    false,
	})

	nms, err := response.Result()

	if err != nil && err != redis.Nil {
		return err
	}

	if nms != nil && len(nms[0].Messages) != 0 {
		go c.processMessages(nms[0].Messages)
		return nil
	}

	log.Println("no messages to process")
	return nil
}

func (c *consumer) processMessages(msgs []redis.XMessage) error {
	log.Printf("processing %d messages", len(msgs))
	processIds := make([]string, 0)
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		for _, m := range msgs {
			ts, _ := strconv.ParseInt(m.Values["ts"].(string), 10, 64)
			r := record{
				ID:         m.Values["id"].(string),
				ProducedAt: ts,
				Data:       []byte(m.Values["data"].(string)),
			}
			b, _ := json.Marshal(r)
			_, _ = pw.Write(b)
			processIds = append(processIds, m.ID)
		}
	}()
	//TODO : handle name collision
	key := fmt.Sprintf("z_%d.json", time.Now().UnixMilli())

	// Error handling
	_ = c.s3Client.upload(key, pr)
	log.Printf("acking %d ids %v", len(processIds), processIds)
	c.rdb.XAck(context.TODO(), streamName, c.group, processIds...)
	return nil

}

func (c *consumer) Start(ctx context.Context) {
	for {
		select {
		case <-c.done:
			return
		default:
			_ = c.read(ctx)
			// This is just to accumulate messages
			time.Sleep(1 * time.Second)
		}
	}
}

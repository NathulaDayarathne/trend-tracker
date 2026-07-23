package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// TrendItem must match the scraper's struct exactly — this is the contract
// between the two programs. Same JSON tags, same field names.
type TrendItem struct {
	ID        int       `json:"id"`
	Title     string    `json:"title"`
	URL       string    `json:"url"`
	Score     int       `json:"score"`
	Author    string    `json:"author"`
	Source    string    `json:"source"`
	ScrapedAt time.Time `json:"scraped_at"`
}

func main() {
	// ctx is cancelled when you press Ctrl+C, so we can shut down cleanly
	// instead of dying mid-message.
	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client, queueURL, err := setupQueue(ctx)
	if err != nil {
		log.Fatalf("queue setup: %v", err)
	}
	log.Printf("consuming from %s", queueURL)
	log.Println("waiting for messages... (Ctrl+C to stop)")

	var count int

	for {
		// Stop if Ctrl+C was pressed.
		select {
		case <-ctx.Done():
			log.Printf("shutting down after %d messages", count)
			return
		default:
		}

		// Ask for up to 10 messages, waiting up to 20s for them to appear.
		out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20, // long polling
			VisibilityTimeout:   30, // hidden from others for 30s
		})
		if err != nil {
			if ctx.Err() != nil {
				return // we're shutting down, not a real error
			}
			log.Printf("receive: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		if len(out.Messages) == 0 {
			log.Println("queue empty, still listening...")
			continue
		}

		for _, msg := range out.Messages {
			// 1. Decode the JSON back into a Go struct.
			var item TrendItem
			if err := json.Unmarshal([]byte(aws.ToString(msg.Body)), &item); err != nil {
				log.Printf("bad message, skipping: %v", err)
				continue
			}

			// 2. Do the work. (Step 5 replaces this with a database write.)
			count++
			log.Printf("#%d [%4d pts] %s", count, item.Score, item.Title)

			// 3. Only now delete it — the work succeeded.
			_, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueURL),
				ReceiptHandle: msg.ReceiptHandle,
			})
			if err != nil {
				log.Printf("delete failed: %v", err)
			}
		}
	}
}

// setupQueue is nearly identical to the scraper's — same endpoint, same queue
// name. That's how the two programs find each other.
func setupQueue(ctx context.Context) (*sqs.Client, string, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(envOr("AWS_REGION", "us-east-1")))
	if err != nil {
		return nil, "", err
	}

	c := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if ep := envOr("SQS_ENDPOINT", "http://localhost:9324"); ep != "" {
			o.BaseEndpoint = aws.String(ep)
		}
	})

	out, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(envOr("QUEUE_NAME", "trends")),
	})
	if err != nil {
		return nil, "", err
	}
	return c, aws.ToString(out.QueueUrl), nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
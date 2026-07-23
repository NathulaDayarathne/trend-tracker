package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// TrendItem is the message contract between scraper and processor.
// Both services must agree on this shape.
type TrendItem struct {
	ID        int       `json:"id"`
	Title     string    `json:"title"`
	URL       string    `json:"url"`
	Score     int       `json:"score"`
	Author    string    `json:"author"`
	Source    string    `json:"source"`
	ScrapedAt time.Time `json:"scraped_at"`
}

type Story struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
	URL   string `json:"url"`
	Score int    `json:"score"`
	By    string `json:"by"`
}

const base = "https://hacker-news.firebaseio.com/v0"

var client = &http.Client{Timeout: 10 * time.Second}

// promauto registers each metric automatically so /metrics picks it up.
// These live at file level so the worker goroutines can reach them.
var (
	itemsScraped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scraper_items_scraped_total",
		Help: "Total stories successfully fetched.",
	})
	itemsPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scraper_items_published_total",
		Help: "Total items published to the queue.",
	})
	scrapeErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scraper_errors_total",
		Help: "Total fetch or publish errors.",
	})
	runDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "scraper_run_duration_seconds",
		Help:    "How long a full scrape run takes.",
		Buckets: prometheus.DefBuckets,
	})
)

func main() {
	const (
		topN    = 50
		workers = 8
	)

	ctx := context.Background()
	start := time.Now()

	// Expose metrics for Prometheus to read. Runs in a goroutine because
	// ListenAndServe blocks forever.
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("metrics on :8080/metrics")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Printf("metrics server: %v", err)
		}
	}()

	// --- connect to the queue ---
	sqsClient, queueURL, err := setupQueue(ctx)
	if err != nil {
		log.Fatalf("queue setup: %v", err)
	}
	log.Printf("publishing to %s", queueURL)

	ids, err := fetchTopIDs()
	if err != nil {
		log.Fatalf("fetching top IDs: %v", err)
	}
	if len(ids) > topN {
		ids = ids[:topN]
	}
	log.Printf("scraping %d stories with %d workers", len(ids), workers)

	idCh := make(chan int)
	var wg sync.WaitGroup
	var published, failed int64
	var mu sync.Mutex // guards the plain int counters below

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for id := range idCh {
				s, err := fetchStory(id)
				if err != nil {
					scrapeErrors.Inc()
					log.Printf("worker %d: fetch %d: %v", workerID, id, err)
					mu.Lock()
					failed++
					mu.Unlock()
					continue
				}
				if s.Title == "" {
					continue
				}
				itemsScraped.Inc()

				item := TrendItem{
					ID:        s.ID,
					Title:     s.Title,
					URL:       s.URL,
					Score:     s.Score,
					Author:    s.By,
					Source:    "hackernews",
					ScrapedAt: time.Now().UTC(),
				}

				if err := publish(ctx, sqsClient, queueURL, item); err != nil {
					scrapeErrors.Inc()
					log.Printf("worker %d: publish %d: %v", workerID, id, err)
					mu.Lock()
					failed++
					mu.Unlock()
					continue
				}
				itemsPublished.Inc()
				mu.Lock()
				published++
				mu.Unlock()
			}
		}(i)
	}

	for _, id := range ids {
		idCh <- id
	}
	close(idCh)
	wg.Wait()

	runDuration.Observe(time.Since(start).Seconds())

	log.Printf("published %d items (%d failed) in %s",
		published, failed, time.Since(start).Round(time.Millisecond))

	// Keep the process alive so Prometheus can scrape /metrics.
	// Step 8 replaces this with a Kubernetes CronJob that exits instead.
	log.Println("run complete — staying up for metrics. Ctrl+C to stop.")
	select {}
}

// setupQueue builds an SQS client pointed at ElasticMQ (or real AWS) and
// creates the queue if it doesn't exist yet, returning its URL.
func setupQueue(ctx context.Context) (*sqs.Client, string, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(envOr("AWS_REGION", "us-east-1")))
	if err != nil {
		return nil, "", err
	}

	c := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		// Pointing at ElasticMQ instead of AWS. Unset this for real SQS.
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

// publish serializes one item to JSON and sends it as a queue message.
func publish(ctx context.Context, c *sqs.Client, queueURL string, item TrendItem) error {
	body, err := json.Marshal(item)
	if err != nil {
		return err
	}
	_, err = c.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(body)),
	})
	return err
}

func fetchTopIDs() ([]int, error) {
	resp, err := client.Get(base + "/topstories.json")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var ids []int
	if err := json.NewDecoder(resp.Body).Decode(&ids); err != nil {
		return nil, err
	}
	return ids, nil
}

func fetchStory(id int) (Story, error) {
	resp, err := client.Get(fmt.Sprintf("%s/item/%d.json", base, id))
	if err != nil {
		return Story{}, err
	}
	defer resp.Body.Close()

	var s Story
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return Story{}, err
	}
	return s, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
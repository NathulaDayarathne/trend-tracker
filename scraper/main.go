package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ---- Prometheus metrics (step 6) --------------------------------------------
// promauto registers each metric automatically so /metrics picks it up.
// These are safe to call from many goroutines at once — no mutex needed.

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

// ---- Types ------------------------------------------------------------------

// TrendItem is the message contract between scraper and processor.
type TrendItem struct {
	ID        int       `json:"id"`
	Title     string    `json:"title"`
	URL       string    `json:"url"`
	Score     int       `json:"score"`
	Author    string    `json:"author"`
	Source    string    `json:"source"`
	ScrapedAt time.Time `json:"scraped_at"`
}

// Story is the shape we decode from the Hacker News API.
type Story struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
	URL   string `json:"url"`
	Score int    `json:"score"`
	By    string `json:"by"`
}

const base = "https://hacker-news.firebaseio.com/v0"

var client = &http.Client{Timeout: 10 * time.Second}

// ---- main -------------------------------------------------------------------

func main() {
	ctx := context.Background()

	// Serve metrics so Prometheus can scrape us.
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		addr := envOr("METRICS_ADDR", ":8080")
		log.Printf("metrics on %s/metrics", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("metrics server: %v", err)
		}
	}()

	sqsClient, queueURL, err := setupQueue(ctx)
	if err != nil {
		log.Fatalf("queue setup: %v", err)
	}
	log.Printf("publishing to %s", queueURL)

	// Scrape once immediately, then on a ticker.
	runScrape(ctx, sqsClient, queueURL)

	interval := time.Duration(envInt("INTERVAL_SECONDS", 60)) * time.Second
	log.Printf("first run complete — looping every %s. Ctrl+C to stop.", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		runScrape(ctx, sqsClient, queueURL)
	}
}

// ---- one scrape run ---------------------------------------------------------

// runScrape fetches the top stories concurrently and publishes each one to the
// queue. This is everything that used to live inline in main().
func runScrape(ctx context.Context, sqsClient *sqs.Client, queueURL string) {
	topN := envInt("TOP_N", 50)
	workers := envInt("WORKERS", 8)

	start := time.Now()
	// Record how long the whole run took, however we exit.
	defer func() { runDuration.Observe(time.Since(start).Seconds()) }()

	ids, err := fetchTopIDs()
	if err != nil {
		scrapeErrors.Inc()
		log.Printf("fetching top IDs: %v", err)
		return
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
					continue // deleted or dead item
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

	log.Printf("published %d items (%d failed) in %s",
		published, failed, time.Since(start).Round(time.Millisecond))
}

// ---- queue ------------------------------------------------------------------

// setupQueue builds an SQS client pointed at ElasticMQ (or real AWS) and
// creates the queue if it doesn't exist yet, returning its URL.
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

// ---- Hacker News API --------------------------------------------------------

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

// ---- helpers ----------------------------------------------------------------

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
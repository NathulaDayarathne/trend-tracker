package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

// Metrics live at file level so every function can reach them.
var (
	processed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "processor_messages_processed_total",
		Help: "Messages successfully stored.",
	})
	failedMsgs = promauto.NewCounter(prometheus.CounterOpts{
		Name: "processor_messages_failed_total",
		Help: "Messages that failed to store.",
	})
	dbLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "processor_db_write_seconds",
		Help:    "Time to write one message to the database.",
		Buckets: prometheus.DefBuckets,
	})
	batchSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "processor_last_batch_size",
		Help: "Messages returned by the most recent receive.",
	})
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Expose metrics on 8081 (8080 belongs to the scraper).
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("metrics on :8081/metrics")
		if err := http.ListenAndServe(":8081", nil); err != nil {
			log.Printf("metrics server: %v", err)
		}
	}()

	// --- connect to Postgres ---
	dsn := envOr("DATABASE_URL",
		"postgres://trends:trends@localhost:5432/trends?sslmode=disable")

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	if err := waitForDB(ctx, pool); err != nil {
		log.Fatalf("postgres not ready: %v", err)
	}
	if err := ensureSchema(ctx, pool); err != nil {
		log.Fatalf("create tables: %v", err)
	}
	log.Println("database ready")

	// --- connect to the queue ---
	client, queueURL, err := setupQueue(ctx)
	if err != nil {
		log.Fatalf("queue setup: %v", err)
	}
	log.Printf("consuming from %s", queueURL)

	var count int
	for {
		select {
		case <-ctx.Done():
			log.Printf("shutting down after %d messages", count)
			return
		default:
		}

		out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20,
			VisibilityTimeout:   30,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("receive: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		batchSize.Set(float64(len(out.Messages)))

		if len(out.Messages) == 0 {
			log.Println("queue empty, still listening...")
			continue
		}

		for _, msg := range out.Messages {
			if err := store(ctx, pool, msg); err != nil {
				failedMsgs.Inc()
				log.Printf("store failed, will retry: %v", err)
				continue // don't delete — let it become visible again
			}
			processed.Inc()
			count++

			_, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueURL),
				ReceiptHandle: msg.ReceiptHandle,
			})
			if err != nil {
				log.Printf("delete failed: %v", err)
			}
		}
		log.Printf("stored %d messages so far", count)
	}
}

// store decodes one message and writes the story plus its keyword counts,
// all inside a single transaction.
func store(ctx context.Context, pool *pgxpool.Pool, msg types.Message) error {
	var item TrendItem
	if err := json.Unmarshal([]byte(aws.ToString(msg.Body)), &item); err != nil {
		return err
	}

	// Time the database work for the latency histogram.
	start := time.Now()
	defer func() { dbLatency.Observe(time.Since(start).Seconds()) }()

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	// If we return early with an error, this undoes everything.
	// After a successful Commit it's a harmless no-op.
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO items (id, title, url, score, author, source, scraped_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO UPDATE
		SET score = EXCLUDED.score, processed_at = now()`,
		item.ID, item.Title, item.URL, item.Score,
		item.Author, item.Source, item.ScrapedAt)
	if err != nil {
		return err
	}

	for _, kw := range keywords(item.Title) {
		_, err = tx.Exec(ctx, `
			INSERT INTO trends (keyword, mentions, last_seen)
			VALUES ($1, 1, now())
			ON CONFLICT (keyword) DO UPDATE
			SET mentions = trends.mentions + 1, last_seen = now()`, kw)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// keywords does crude word extraction: lowercase, split on anything that isn't
// a letter or digit, drop short words and common filler.
func keywords(title string) []string {
	words := strings.FieldsFunc(strings.ToLower(title), func(r rune) bool {
		return !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9')
	})
	seen := map[string]bool{}
	var out []string
	for _, w := range words {
		if len(w) < 4 || stopwords[w] || seen[w] {
			continue
		}
		seen[w] = true // count each word once per title
		out = append(out, w)
	}
	return out
}

var stopwords = map[string]bool{
	"this": true, "that": true, "with": true, "from": true, "have": true,
	"your": true, "will": true, "what": true, "when": true, "they": true,
	"about": true, "into": true, "just": true, "than": true, "then": true,
	"show": true, "using": true, "make": true, "does": true, "here": true,
}

// ensureSchema creates the tables if they don't exist. Safe to run every start.
func ensureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS items (
			id           BIGINT PRIMARY KEY,
			title        TEXT NOT NULL,
			url          TEXT,
			score        INT,
			author       TEXT,
			source       TEXT,
			scraped_at   TIMESTAMPTZ,
			processed_at TIMESTAMPTZ DEFAULT now()
		);
		CREATE TABLE IF NOT EXISTS trends (
			keyword   TEXT PRIMARY KEY,
			mentions  INT NOT NULL DEFAULT 0,
			last_seen TIMESTAMPTZ DEFAULT now()
		);
		CREATE INDEX IF NOT EXISTS trends_mentions_idx
			ON trends (mentions DESC);`)
	return err
}

// waitForDB retries for ~30s so the processor survives being started before
// Postgres has finished booting.
func waitForDB(ctx context.Context, pool *pgxpool.Pool) error {
	var last error
	for i := 0; i < 30; i++ {
		if err := pool.Ping(ctx); err == nil {
			return nil
		} else {
			last = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return last
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
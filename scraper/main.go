package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
)

type Story struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
	URL   string `json:"url"`
	Score int    `json:"score"`
	By    string `json:"by"`
}

const base = "https://hacker-news.firebaseio.com/v0"

// One shared client with a timeout. http.Get uses a default client with NO
// timeout, which can hang forever — never use it in real code.
var client = &http.Client{Timeout: 10 * time.Second}

func main() {
	const (
		topN    = 50 // how many stories to fetch
		workers = 8  // how many at a time
	)

	start := time.Now()

	ids, err := fetchTopIDs()
	if err != nil {
		log.Fatalf("fetching top IDs: %v", err)
	}
	if len(ids) > topN {
		ids = ids[:topN]
	}
	log.Printf("fetching %d stories with %d workers", len(ids), workers)

	idCh := make(chan int)       // work in
	resultCh := make(chan Story) // results out

	// --- fan out: start the worker pool ---
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			// Loops until idCh is closed and drained.
			for id := range idCh {
				s, err := fetchStory(id)
				if err != nil {
					log.Printf("worker %d: story %d: %v", workerID, id, err)
					continue
				}
				if s.Title == "" {
					continue // deleted or dead item
				}
				resultCh <- s
			}
		}(i)
	}

	// --- feed the workers, then signal "no more work" ---
	go func() {
		for _, id := range ids {
			idCh <- id
		}
		close(idCh)
	}()

	// --- close resultCh once every worker has exited ---
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// --- fan in: collect until resultCh closes ---
	var stories []Story
	for s := range resultCh {
		stories = append(stories, s)
	}

	sort.Slice(stories, func(i, j int) bool {
		return stories[i].Score > stories[j].Score
	})

	for _, s := range stories {
		fmt.Printf("[%4d pts] %s\n", s.Score, s.Title)
	}
	log.Printf("fetched %d stories in %s", len(stories), time.Since(start).Round(time.Millisecond))
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
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

// Story mirrors the fields we care about from the HN item endpoint.
// The `json:"..."` tags map each lowercase JSON key to our Go field.
type Story struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
	URL   string `json:"url"`
	Score int    `json:"score"`
	By    string `json:"by"`
}

const base = "https://hacker-news.firebaseio.com/v0"

func main() {
	// 1. Fetch the list of top story IDs.
	ids, err := fetchTopIDs()
	if err != nil {
		log.Fatalf("fetching top IDs: %v", err)
	}
	fmt.Println("first 5 IDs:", ids[:5])

	// 2. Fetch details for the first 5 and print their titles.
	for _, id := range ids[:5] {
		story, err := fetchStory(id)
		if err != nil {
			log.Printf("fetching story %d: %v", id, err)
			continue
		}
		fmt.Printf("[%d pts] %s\n", story.Score, story.Title)
	}
}

func fetchTopIDs() ([]int, error) {
	resp, err := http.Get(base + "/topstories.json")
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
	url := fmt.Sprintf("%s/item/%d.json", base, id)
	resp, err := http.Get(url)
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
// +build ignore

package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
)

func main() {
	// Start the client with defaults
	ctx := context.TODO()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// Get a random queue
	rand.Seed(time.Now().Unix())
	queueName := fmt.Sprintf("sample-%d", rand.Int())
	q, err := client.GetQueue(ctx, queueName)
	if err != nil {
		log.Fatal(err)
	}
	// Add an item to the queue if space is available (non-blocking)
	added, err := q.Add(ctx, "item 1")
	if err != nil {
		log.Fatal(err)
	}
	if added {
		fmt.Println("Added item 1")
	}
	// Get the head of the queue if available and print item
	item, err := q.Poll(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(item)
	// Add an item waiting for capacity until timeout
	added, err = q.AddWithTimeout(ctx, "item 2", 2*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	if added {
		fmt.Println("Added item 2")
	}
	// Wait indefinetely to add an item
	err = q.Put(ctx, "item 3")
	if err != nil {
		log.Fatal(err)
	}
	// Wait indefintely to take the head and print item
	item, err = q.Take(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(item)
	// Shutdown client
	client.Shutdown(ctx)
}

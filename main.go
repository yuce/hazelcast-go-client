package main

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
	"log"
)

func main(){
	cb := hazelcast.NewClientConfigBuilder()
	cb.Cluster().SetName("jet")
	config, err := cb.Config()
	client, err := hazelcast.StartNewClientWithConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	someMap, _ := client.GetMap("someMap2")

	// _, _ = someMap.Put("selam", 1)

	fmt.Println(someMap.Get("selam"))

	client.SqlService().Execute("SELECT * FROM someMap2")
}

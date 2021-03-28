package main

import (
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
	"log"
)

func main(){
	defaultConfig := hazelcast.DefaultConfig()
	defaultConfig.ClusterName = "jet"
	client, err := hazelcast.StartNewClientWithConfig(defaultConfig)
	if err != nil {
		log.Fatal(err)
	}

	someMap, _ := client.GetMap("someMap2")

	_, _ = someMap.Put("selam", 1)


	client.SqlService().Execute("SELECT * FROM someMap2")
}

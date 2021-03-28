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

	client.SqlService().Execute("SELECT * FROM TABLE(generate_series(1,3));")
}

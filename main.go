package main

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
	"log"
)

func main() {
	cb := hazelcast.NewClientConfigBuilder()
	cb.Cluster().SetName("jet")
	config, err := cb.Config()
	client, err := hazelcast.StartNewClientWithConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	someMap, _ := client.GetMap("someMap")

	_ = someMap.Clear()

	_, _ = someMap.Put( 1, "selam")
	_, _ = someMap.Put( 2, "selam2")
	_, _ = someMap.Put(3, "selam3")
	_, _ = someMap.Put(4, "selam4")
	_, _ = someMap.Put(5, "selam5")
	_, _ = someMap.Put(6, "selam6")

	result, err := client.SqlService().Execute("SHOW MAPPINGS")

	if err != nil {
		log.Panicf("could not execute sql %s", err)
	}
	rows := result.Rows()

	counter := 0

	for rows.HasNext() {
		counter++
		fmt.Printf("Row %d: ", counter)
		row := rows.Next()
		rowMetadata := row.Metadata()
		columnCount := rowMetadata.ColumnCount()
		for i := 0; i < columnCount; i++ {
			fmt.Print("Value: ")
			// column := rowMetadata.Column(i)
			// column.Type()
			fmt.Print(row.ValueAtIndex(i))
			fmt.Print(" ")

		}
		fmt.Println()
	}
}

package main

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/sql"
)

func main(){
	fmt.Println(sql.ColumnType(123))
}

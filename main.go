package main

import (
	"context"
	"fmt"
	"github.com/asaintgenis/aws-data-api-mapper/client"
	"log"
)

type Item struct {
	ID string `pgmap:"id"`
}

func main() {
	fmt.Println("Hello, Go SDK Project!")
	ctx := context.Background()

	trx := Item{}

	client, err := client.NewClient("XXX", "XXX", "XXX", "XXX")
	if err != nil {
		log.Fatalf("connection to DB failed: %s", err.Error())
	}
	err = client.SelectFirst(ctx, &trx, "transaction")
	if err != nil {
		log.Fatalf("select failed: %s", err.Error())
	}

	log.Printf("%#v\n", trx)
	log.Printf("finished")
}

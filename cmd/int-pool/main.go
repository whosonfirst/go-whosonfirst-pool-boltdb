package main

import (
	"flag"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-pool"
	"github.com/whosonfirst/go-whosonfirst-pool-boltdb"
	"log"
)

func main() {

	var dsn = flag.String("dsn", "integer.db", "The data source name (dsn) for connecting to BoltDB.")
	var bucket = flag.String("bucket", "pool", "A valid BoltDB list bucket name")

	flag.Parse()

	p, err := boltdb.NewBoltDBLIFOIntPool(*dsn, *bucket)

	if err != nil {
		log.Fatal(err)
	}

	log.Println("LEN", p.Length())

	f1 := pool.NewIntItem(int64(123))
	f2 := pool.NewIntItem(int64(456))

	p.Push(f1)
	p.Push(f2)

	v, ok := p.Pop()

	if !ok {
		log.Fatal("Did not pop")
	}

	fmt.Printf("%d", v.Int())
}

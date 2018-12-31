package boltdb

import (
	"errors"
	"github.com/boltdb/bolt"
	"github.com/whosonfirst/go-whosonfirst-pool"
	"log"
)

type DeflateFunc func(pool.Item) (interface{}, error)

type InflateFunc func(interface{}, error) (pool.Item, error)

type BoltDBLIFOPool struct {
	pool.LIFOPool
	db      *bolt.DB
	bucket  string
	inflate InflateFunc
	deflate DeflateFunc
}

func NewBoltDBLIFOIntPool(dsn string, key string) (pool.LIFOPool, error) {

	deflate := func(i pool.Item) (interface{}, error) {

		return i.Int(), nil
	}

	inflate := func(rsp interface{}, err error) (pool.Item, error) {

		return nil, errors.New("please write me")
	}

	return NewBoltDBLIFOPool(dsn, key, deflate, inflate)
}

func NewBoltDBLIFOPool(dsn string, bucket string, deflate DeflateFunc, inflate InflateFunc) (pool.LIFOPool, error) {

	db, err := bolt.Open(dsn, 0600, nil)

	if err != nil {
		return nil, err
	}

	tx, err := db.Begin(true)

	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	_, err = tx.CreateBucket([]byte(bucket))

	if err != nil {
		return nil, err
	}

	err = tx.Commit()

	if err != nil {
		return nil, err
	}

	pl := BoltDBLIFOPool{
		db:      db,
		bucket:  bucket,
		inflate: inflate,
		deflate: deflate,
	}

	return &pl, nil
}

// basically the interface for pool.LIFOPool should be changed
// to expect errors all over the place but today that is not
// the case... (20181222/thisisaaronland)

func (pl *BoltDBLIFOPool) Length() int64 {

	count := int64(0)

	pl.db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(pl.bucket))

		c := b.Cursor()

		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			count += 1
		}

		return nil
	})

	return count
}

func (pl *BoltDBLIFOPool) Push(pi pool.Item) {

	pl.db.Update(func(tx *bolt.Tx) error {

		i := pi.String()

		b := tx.Bucket([]byte(pl.bucket))
		return b.Put([]byte(i), []byte(i))
	})
}

func (pl *BoltDBLIFOPool) Pop() (pool.Item, bool) {

	pl.db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(pl.bucket))
		c := b.Cursor()

		_, v := c.Last()

		// DO SOMETHING WITH v HERE
		log.Println(v)

		return nil
	})

	return nil, false
}

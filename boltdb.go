package boltdb

import (
	"fmt"
	"github.com/aaronland/go-string/random"
	"github.com/boltdb/bolt"
	"github.com/whosonfirst/go-whosonfirst-pool"
	_ "log"
	"strconv"
)

type DeflateFunc func(pool.Item) (interface{}, error)

type InflateFunc func(interface{}) (pool.Item, error)

type BoltDBLIFOPool struct {
	pool.LIFOPool
	db          *bolt.DB
	bucket      string
	inflate     InflateFunc
	deflate     DeflateFunc
	random_opts *random.Options
}

func NewBoltDBLIFOIntPool(dsn string, bucket string) (pool.LIFOPool, error) {

	deflate := func(i pool.Item) (interface{}, error) {
		return i.String(), nil
	}

	inflate := func(rsp interface{}) (pool.Item, error) {

		b_int := rsp.([]byte)

		int, err := strconv.ParseInt(string(b_int), 10, 64)

		if err != nil {
			return nil, err
		}

		return pool.NewIntItem(int), nil
	}

	return NewBoltDBLIFOPool(dsn, bucket, deflate, inflate)
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

	_, err = tx.CreateBucketIfNotExists([]byte(bucket))

	if err != nil {
		return nil, err
	}

	err = tx.Commit()

	if err != nil {
		return nil, err
	}

	opts := random.DefaultOptions()
	opts.AlphaNumeric = true
	opts.Length = 16

	pl := BoltDBLIFOPool{
		db:          db,
		bucket:      bucket,
		inflate:     inflate,
		deflate:     deflate,
		random_opts: opts,
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

		i, err := pl.deflate(pi)

		if err != nil {
			return err
		}

		r, err := random.String(pl.random_opts)

		if err != nil {
			return err
		}

		v := i.(string)
		k := fmt.Sprintf("%s#%s", v, r)		// mmmmmaybe?

		b := tx.Bucket([]byte(pl.bucket))
		return b.Put([]byte(k), []byte(v))
	})
}

func (pl *BoltDBLIFOPool) Pop() (pool.Item, bool) {

	var pi pool.Item

	err := pl.db.Update(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(pl.bucket))
		c := b.Cursor()

		k, v := c.Last()

		p, err := pl.inflate(v)

		if err != nil {
			return err
		}

		err = b.Delete(k)

		if err != nil {
			return err
		}

		pi = p
		return nil
	})

	if err != nil {
		return nil, false
	}

	return pi, true
}

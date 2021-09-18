package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/timshannon/bolthold"
	bolt "go.etcd.io/bbolt"
)

type FileInfoWithIndex struct {
	HashKey        string
	BindName       string `boltholdIndex:"BindName"`
	LastAccessTime int64  `boltholdIndex:"LastAccessTime"`
	FileSize       int64
}

var store *bolthold.Store

func GenRandomKey(l int) string {
	letterRunes := []rune("1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, l)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
func insertAmountWithIndex() {
	startTime := time.Now()
	for j := 0; j < 1000; j++ {
		err := store.Bolt().Update(func(tx *bolt.Tx) error {
			for i := 0; i < 50000; i++ {
				hashKey := GenRandomKey(16)
				bindName := fmt.Sprintf("bindname-%d", rand.Intn(500))
				fileInfo := FileInfoWithIndex{hashKey, bindName, int64(rand.Intn(100000000)), int64(rand.Intn(3000000))}

				err := store.TxInsert(tx, hashKey, fileInfo)
				if err != nil {
					log.Println(err)
				}
				if i%100000 == 0 {
					log.Println("inserted", i)
				}
			}
			return nil

		})
		if err != nil {
			log.Println(err)
		}

		log.Println("insert the :", j, "th 50000 with time:", time.Since(startTime).Milliseconds(), "ms")

	}

}

func main() {

	os.Remove("test.db")
	var err error
	store, err = bolthold.Open("test.db", 0666, nil)
	if err != nil {
		fmt.Println("bolthold can't open")
	}

	//insert base data
	insertAmountWithIndex()

}

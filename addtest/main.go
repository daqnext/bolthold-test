package main

import (
	"fmt"

	"github.com/timshannon/bolthold"
	bolt "go.etcd.io/bbolt"
)

type BenchDataIndexed struct {
	ID       int
	Category string
}

func main() {

	store, err := bolthold.Open("test.db", 0666, nil)
	if err != nil {
		fmt.Println("bolthold can't open")
	}

	// err = store.Bolt().Update(func(tx *bolt.Tx) error {
	// 	_, err := tx.CreateBucket([]byte("bucket1"))
	// 	return err
	// })
	// if err != nil {
	// 	panic("bucket1 error ")
	// }
	err = store.Bolt().Update(func(tx *bolt.Tx) error {
		for i := 0; i < 40000; i++ {
			store.TxInsert(tx, i, &BenchDataIndexed{
				ID:       i,
				Category: "test category",
			})
		}
		return nil
	})

	if err != nil {
		fmt.Println("err ", err.Error())
	}

	result := &BenchDataIndexed{}

	err = store.Get(21020, result)
	if err != nil {
		fmt.Println("Error getting data from bolthold", err.Error())
	}

	fmt.Println(result.ID)
	fmt.Println(result.Category)

}

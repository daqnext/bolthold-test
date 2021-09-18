package test

import (
	"fmt"
	"github.com/timshannon/bolthold"
	bolt "go.etcd.io/bbolt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

var store *bolthold.Store

type FileInfoWithIndex struct {
	HashKey        string
	BindName       string `boltholdIndex:"BindName"`
	LastAccessTime int64  `boltholdIndex:"LastAccessTime"`
	FileSize       int64
}

type FileInfoWithOutIndex struct {
	HashKey        string
	BindName       string
	LastAccessTime int64
	FileSize       int64
}

type FileLastAccessInfo struct {
	HashKey        string
	LastAccessTime int64 `boltholdIndex:"LastAccessTime"`
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func GenRandomKey(l int) string {
	letterRunes := []rune("1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, l)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func insertAmountWithIndex(count int) {
	log.Printf("start to insert %d records with index use batch\n", count)
	startTime := time.Now()
	err := store.Bolt().Update(func(tx *bolt.Tx) error {
		for i := 0; i < count; i++ {
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
	log.Println("insert 100W record with index cost time:", time.Since(startTime).Milliseconds(), "ms")
}

func queryBindNameWithIndex(bindName string) {
	log.Println("start to find BindName==,", bindName, "record")
	startTime := time.Now()
	var records1 []FileInfoWithIndex
	err := store.Find(&records1, bolthold.Where("BindName").Eq(bindName).Index("BindName"))
	if err != nil {
		log.Println(err)
	}
	log.Println("find records with index cost time:", time.Since(startTime).Milliseconds(), "ms", "find count", len(records1))
}

func queryAccessTimeRange(start, end int64) {
	log.Printf("start to find LastAccessTime>=%d <=%d record\n", start, end)
	startTime := time.Now()
	var records2 []FileInfoWithIndex
	err := store.Find(&records2, bolthold.Where("LastAccessTime").Ge(start).And("LastAccessTime").Le(end).Index("LastAccessTime"))
	if err != nil {
		log.Println(err)
	}
	log.Println("find records with index cost time:", time.Since(startTime).Milliseconds(), "ms", "find count", len(records2))
}

func Test_insertAmountWithIndex(t *testing.T) {
	os.Remove("test.db")
	var err error
	store, err = bolthold.Open("test.db", 0666, nil)
	if err != nil {
		fmt.Println("bolthold can't open")
	}

	//insert base data
	insertAmountWithIndex(1000000)

	//query
	queryBindNameWithIndex("bindname-10")
	queryAccessTimeRange(10000, 1000000)
}

//result
//=== RUN   Test_insertAmountWithIndex
//2021/09/18 17:06:27 start to insert 1000000 records with index use batch
//2021/09/18 17:06:27 inserted 0
//2021/09/18 17:07:02 inserted 100000
//2021/09/18 17:08:50 inserted 200000
//2021/09/18 17:11:51 inserted 300000
//2021/09/18 17:16:15 inserted 400000
//2021/09/18 17:21:42 inserted 500000
//2021/09/18 17:28:19 inserted 600000
//2021/09/18 17:36:09 inserted 700000
//2021/09/18 17:45:11 inserted 800000
//2021/09/18 17:55:54 inserted 900000
//2021/09/18 18:07:54 insert 100W record with index cost time: 3658626 ms
//2021/09/18 18:07:54 start to find BindName==, bindname-10 record
//2021/09/18 18:07:54 find records with index cost time: 53 ms find count 2007
//2021/09/18 18:07:54 start to find LastAccessTime>=10000 <=1000000 record
//2021/09/18 18:07:58 find records with index cost time: 3726 ms find count 9929
//--- PASS: Test_insertAmountWithIndex (3662.42s)

//func Test_insert(t *testing.T) {
//
//	nowTime := time.Now().Unix() //1631845870
//	insterArray := []struct {
//		hashKey  string
//		fileInfo *FileInfoWithIndex
//	}{
//		{"1", &FileInfoWithIndex{"1", "bindname", nowTime, 29943}},
//		{"2", &FileInfoWithIndex{"2", "bindname",  nowTime + 10, 29943}},
//		{"3", &FileInfoWithIndex{"3", "bindname2", nowTime + 20, 29943}},
//		{"4", &FileInfoWithIndex{"4", "bindname2", nowTime + 30, 29943}},
//		{"5", &FileInfoWithIndex{"5", "bindname3",  nowTime + 40, 29943}},
//		{"6", &FileInfoWithIndex{"6", "bindname3", nowTime + 50, 29943}},
//		{"7", &FileInfoWithIndex{"7", "bindname4", nowTime + 60, 29943}},
//		{"8", &FileInfoWithIndex{"8", "bindname4", nowTime + 70, 29943}},
//		{"9", &FileInfoWithIndex{"9", "bindname5",  nowTime + 80, 29943}},
//		{"10", &FileInfoWithIndex{"10", "bindname5",  nowTime + 90, 29943}},
//		{"11", &FileInfoWithIndex{"11", "bindname6",  nowTime + 100, 29943}},
//	}
//
//	err := store.Bolt().Batch(func(tx *bolt.Tx) error {
//		for _, v := range insterArray {
//			err := store.TxInsert(tx, v.hashKey, v.fileInfo)
//			if err != nil {
//				log.Println(err)
//			}
//		}
//		return nil
//	})
//	if err != nil {
//		log.Println(err)
//	}
//}
//
//func Test_Bucket(t *testing.T) {
//
//	files := []string{"1", "2", "3", "4"}
//	type EmptyStruct struct{}
//
//	store.Bolt().Update(func(tx *bolt.Tx) error {
//		tx.CreateBucketIfNotExists([]byte("downloading"))
//		return nil
//	})
//
//	store.Bolt().Update(func(tx *bolt.Tx) error {
//		bkt := tx.Bucket([]byte("downloading"))
//		for _, v := range files {
//			store.InsertIntoBucket(bkt, v, EmptyStruct{})
//		}
//		return nil
//	})
//
//	store.Bolt().View(func(tx *bolt.Tx) error {
//		bkt := tx.Bucket([]byte("downloading"))
//		for _, v := range files {
//			var a EmptyStruct
//			store.GetFromBucket(bkt, v, &a)
//			log.Println("key", v, "value", a)
//		}
//		return nil
//	})
//
//	store.Bolt().Update(func(tx *bolt.Tx) error {
//		bkt := tx.Bucket([]byte("downloading"))
//		for _, v := range files {
//			store.DeleteFromBucket(bkt, v, &EmptyStruct{})
//		}
//		return nil
//	})
//
//	store.Bolt().View(func(tx *bolt.Tx) error {
//		bkt := tx.Bucket([]byte("downloading"))
//		for _, v := range files {
//			var a EmptyStruct
//			err := store.GetFromBucket(bkt, v, &a)
//			if err != nil {
//				log.Println("key", v, "err", err)
//			} else {
//				log.Println("key", v, "value", a)
//			}
//		}
//		return nil
//	})
//}
//
//func Test_update(t *testing.T) {
//
//	err := store.UpdateMatching(&FileInfoWithIndex{}, bolthold.Where("BindName").Eq("bindname2"), func(record interface{}) error {
//		_, ok := record.(*FileInfoWithIndex)
//		if !ok {
//			return errors.New("type error")
//		}
//
//
//		return nil
//	})
//	if err != nil {
//		log.Println(err)
//	}
//}
//
//func Test_query(t *testing.T) {
//	hashKey := "2"
//	var file FileInfoWithIndex
//	err := store.Get(hashKey, &file)
//	if err != nil {
//		log.Println(err)
//	}
//	log.Println(file)
//
//	var files []FileInfoWithIndex
//	err = store.Find(&files, bolthold.Where("BindName").Eq("bindname2"))
//	if err != nil {
//		log.Println(err)
//	}
//	log.Println(files)
//
//	files = []FileInfoWithIndex{}
//	err = store.Find(&files, bolthold.Where("LastAccessTime").Lt(int64(1631845910)))
//	if err != nil {
//		log.Println(err)
//	}
//	log.Println(files)
//}

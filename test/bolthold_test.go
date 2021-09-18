package test

import (
	"errors"
	"fmt"
	"github.com/timshannon/bolthold"
	bolt "go.etcd.io/bbolt"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

var store *bolthold.Store

type FileInfoWithIndex struct {
	HashKey        string
	BindName       string `boltholdIndex:"BindName"`
	OriginName     string
	LastAccessTime int64 `boltholdIndex:"LastAccessTime"`
	FileSize       int64 `boltholdIndex:"FileSize"`
}

type FileInfoWithOutIndex struct {
	HashKey        string
	BindName       string
	OriginName     string
	LastAccessTime int64
	FileSize       int64
}

type FileLastAccessInfo struct {
	HashKey        string
	LastAccessTime int64 `boltholdIndex:"LastAccessTime"`
}

//func init(){
//	var err error
//	store, err = bolthold.Open("test.db", 0666, nil)
//	if err != nil {
//		fmt.Println("bolthold can't open")
//	}
//}

func Test_insertAmountWithIndex(t *testing.T) {
	os.Remove("test.db")
	var err error
	store, err = bolthold.Open("test.db", 0666, nil)
	if err != nil {
		fmt.Println("bolthold can't open")
	}

	log.Println("start to insert 100W records with index use batch")
	startTime := time.Now()
	err = store.Bolt().Update(func(tx *bolt.Tx) error {
		for i := 0; i < 1000000; i++ {
			hashKey := strconv.Itoa(i)
			bindName := fmt.Sprintf("bindname-%d", i%10000)
			originName := fmt.Sprintf("aaa/sdf/sdf%d.jpg", i)
			fileInfo := FileInfoWithIndex{hashKey, bindName, originName, int64(i), int64(i)}
			err := store.TxInsert(tx, hashKey, fileInfo)
			if err != nil {
				log.Println(err)
			}
		}
		return nil
	})
	if err != nil {
		log.Println(err)
	}
	log.Println("insert 100W record with index cost time:", time.Since(startTime).Milliseconds(), "ms")

	log.Println("start to find BindName==bindname-5 record")
	startTime = time.Now()
	var records1 []FileInfoWithIndex
	err = store.Find(&records1, bolthold.Where("BindName").Eq("bindname-5"))
	if err != nil {
		log.Println(err)
	}
	for i := 0; i < len(records1); i++ {
		log.Println(records1[i])
	}
	log.Println("find record in 100W records with index cost time:", time.Since(startTime).Milliseconds(), "ms", "find count", len(records1))

	log.Println("start to find LastAccessTime>=10000&&<30000 record")
	startTime = time.Now()
	var records2 []FileInfoWithIndex
	err = store.Find(&records2, bolthold.Where("LastAccessTime").Ge(int64(10000)).And("LastAccessTime").Lt(int64(30000)))
	if err != nil {
		log.Println(err)
	}
	log.Println("find record in 100W records with index cost time:", time.Since(startTime).Milliseconds(), "ms", "find count", len(records2))

}

func Test_insert(t *testing.T) {

	nowTime := time.Now().Unix() //1631845870
	insterArray := []struct {
		hashKey  string
		fileInfo *FileInfoWithIndex
	}{
		{"1", &FileInfoWithIndex{"1", "bindname", "aaa/sdf/sdf1.jpg", nowTime, 29943}},
		{"2", &FileInfoWithIndex{"2", "bindname", "aaa/sdf/sdf2.jpg", nowTime + 10, 29943}},
		{"3", &FileInfoWithIndex{"3", "bindname2", "aaa/sdf/sdf3.jpg", nowTime + 20, 29943}},
		{"4", &FileInfoWithIndex{"4", "bindname2", "aaa/sdf/sdf4.jpg", nowTime + 30, 29943}},
		{"5", &FileInfoWithIndex{"5", "bindname3", "aaa/sdf/sdf5.jpg", nowTime + 40, 29943}},
		{"6", &FileInfoWithIndex{"6", "bindname3", "aaa/sdf/sdf6.jpg", nowTime + 50, 29943}},
		{"7", &FileInfoWithIndex{"7", "bindname4", "aaa/sdf/sdf7.jpg", nowTime + 60, 29943}},
		{"8", &FileInfoWithIndex{"8", "bindname4", "aaa/sdf/sdf8.jpg", nowTime + 70, 29943}},
		{"9", &FileInfoWithIndex{"9", "bindname5", "aaa/sdf/sdf9.jpg", nowTime + 80, 29943}},
		{"10", &FileInfoWithIndex{"10", "bindname5", "aaa/sdf/sdf10.jpg", nowTime + 90, 29943}},
		{"11", &FileInfoWithIndex{"11", "bindname6", "aaa/sdf/sdf11.jpg", nowTime + 100, 29943}},
	}

	err := store.Bolt().Batch(func(tx *bolt.Tx) error {
		for _, v := range insterArray {
			err := store.TxInsert(tx, v.hashKey, v.fileInfo)
			if err != nil {
				log.Println(err)
			}
		}
		return nil
	})
	if err != nil {
		log.Println(err)
	}
}

func Test_Bucket(t *testing.T) {

	files := []string{"1", "2", "3", "4"}
	type EmptyStruct struct{}

	store.Bolt().Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("downloading"))
		return nil
	})

	store.Bolt().Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("downloading"))
		for _, v := range files {
			store.InsertIntoBucket(bkt, v, EmptyStruct{})
		}
		return nil
	})

	store.Bolt().View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("downloading"))
		for _, v := range files {
			var a EmptyStruct
			store.GetFromBucket(bkt, v, &a)
			log.Println("key", v, "value", a)
		}
		return nil
	})

	store.Bolt().Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("downloading"))
		for _, v := range files {
			store.DeleteFromBucket(bkt, v, &EmptyStruct{})
		}
		return nil
	})

	store.Bolt().View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("downloading"))
		for _, v := range files {
			var a EmptyStruct
			err := store.GetFromBucket(bkt, v, &a)
			if err != nil {
				log.Println("key", v, "err", err)
			} else {
				log.Println("key", v, "value", a)
			}
		}
		return nil
	})
}

func Test_update(t *testing.T) {

	err := store.UpdateMatching(&FileInfoWithIndex{}, bolthold.Where("BindName").Eq("bindname2"), func(record interface{}) error {
		info, ok := record.(*FileInfoWithIndex)
		if !ok {
			return errors.New("type error")
		}
		info.OriginName = "aaa/sdf/sdf3-1.jpg"

		return nil
	})
	if err != nil {
		log.Println(err)
	}
}

func Test_query(t *testing.T) {
	hashKey := "2"
	var file FileInfoWithIndex
	err := store.Get(hashKey, &file)
	if err != nil {
		log.Println(err)
	}
	log.Println(file)

	var files []FileInfoWithIndex
	err = store.Find(&files, bolthold.Where("BindName").Eq("bindname2"))
	if err != nil {
		log.Println(err)
	}
	log.Println(files)

	files = []FileInfoWithIndex{}
	err = store.Find(&files, bolthold.Where("LastAccessTime").Lt(int64(1631845910)))
	if err != nil {
		log.Println(err)
	}
	log.Println(files)
}

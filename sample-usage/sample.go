package main

import (
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
)

func main() {

	db, err := leveldb.OpenFile("./db/", nil)
	defer db.Close()
	if err != nil {
		fmt.Println(err)
	}

	err = db.Put([]byte("bested2"), []byte("valuebested"), nil)
	if err != nil {
		fmt.Println(err)
	}

	err = db.Put([]byte("casted2"), []byte("valuecasted"), nil)
	if err != nil {
		fmt.Println(err)
	}

	err = db.Put([]byte("aested2"), []byte("valueaested"), nil)
	if err != nil {
		fmt.Println(err)
	}
}

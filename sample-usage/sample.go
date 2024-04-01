package main

func main() {

	/* Check what files are created */
	// testDBWithSomeData := func() {
	// 	db, err := leveldb.OpenFile("./db/", nil)
	// 	defer db.Close()
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}

	// 	err = db.Put([]byte("bested2"), []byte("valuebested"), nil)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}

	// 	err = db.Put([]byte("casted2"), []byte("valuecasted"), nil)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}

	// 	err = db.Put([]byte("aested2"), []byte("valueaested"), nil)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}
	// }

	/* Sample of what my skiplist looks like */
	// displaySkipListSample := func() {
	// 	size := 20
	// 	sl := skiplist.NewSkipList(0.25, 12)
	// 	for i := 0; i < size; i++ {
	// 		randItem := rand.Intn(size)
	// 		k, v := []byte(fmt.Sprintf("%d", randItem)), []byte(fmt.Sprintf("%d", randItem))
	// 		sl.Insert(k, v)
	// 	}
	// 	fmt.Println(sl)
	// }
	// displaySkipListSample()

}

package main

func main() {

	/*** Add multiple records and check ***/
	// testDBWithSomeData := func() {
	// 	db, err := leveldb.OpenFile("./db/", nil)
	// defer db.Close()
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// defer db.Close()
	// 	for i := 0; i < 1000000; i++ {
	// 		k, v := []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i))
	// 		err = db.Put(k, v, nil)
	// 		if err != nil {
	// 			fmt.Println(err)
	// 		}
	// 	}
	// }
	// testDBWithSomeData()

	/*** Add small number of records and check ***/
	// testDBWithLittleData := func() {
	// 	db, err := leveldb.OpenFile("./db2/", nil)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}
	// 	defer db.Close()

	// 	err = db.Put([]byte("trash"), []byte("dustbin"), nil)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}

	// 	err = db.Put([]byte("casy"), []byte("ne"), nil)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}

	// 	err = db.Put([]byte("aesthetic"), []byte("che"), nil)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}
	// }
	// testDBWithLittleData()

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

module github.com/chettriyuvraj/leveldb-clone

go 1.20

replace github.com/chettriyuvraj/leveldb-clone/wal => ./wal

replace github.com/chettriyuvraj/leveldb-clone/matureleveldb => ./matureleveldb

require github.com/chettriyuvraj/leveldb-clone/matureleveldb v0.0.0-00010101000000-000000000000

require (
	github.com/chettriyuvraj/leveldb-clone/common v0.0.0-20240403115846-26a9c2864729 // indirect
	github.com/chettriyuvraj/leveldb-clone/skiplist v0.0.0-20240403115846-26a9c2864729 // indirect
	github.com/chettriyuvraj/leveldb-clone/test v0.0.0-20240403115846-26a9c2864729 // indirect
	github.com/chettriyuvraj/leveldb-clone/wal v0.0.0-20240403115846-26a9c2864729 // indirect
)

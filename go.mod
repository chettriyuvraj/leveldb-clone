module github.com/chettriyuvraj/leveldb-clone

go 1.20

replace github.com/chettriyuvraj/leveldb-clone/wal => ./wal

replace github.com/chettriyuvraj/leveldb-clone/memdb => ./memdb

require (
	github.com/chettriyuvraj/leveldb-clone/common v0.0.0-20240405214557-ebb9500fb244
	github.com/chettriyuvraj/leveldb-clone/db v0.0.0-00010101000000-000000000000
)

require (
	github.com/chettriyuvraj/leveldb-clone/memdb v0.0.0-20240405214557-ebb9500fb244 // indirect
	github.com/chettriyuvraj/leveldb-clone/skiplist v0.0.0-20240405214557-ebb9500fb244 // indirect
	github.com/chettriyuvraj/leveldb-clone/test v0.0.0-20240403115846-26a9c2864729 // indirect
	github.com/chettriyuvraj/leveldb-clone/wal v0.0.0-20240405214557-ebb9500fb244 // indirect
)

replace github.com/chettriyuvraj/leveldb-clone/db => ./db

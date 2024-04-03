module github.com/chettriyuvraj/leveldb-clone/matureleveldb

go 1.20

require github.com/stretchr/testify v1.9.0

require (
	github.com/chettriyuvraj/leveldb-clone/common v0.0.0-20240403115846-26a9c2864729 // indirect
	github.com/chettriyuvraj/leveldb-clone/skiplist v0.0.0-20240403115846-26a9c2864729 // indirect
	github.com/chettriyuvraj/leveldb-clone/wal v0.0.0-20240403115846-26a9c2864729 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/chettriyuvraj/leveldb-clone/test => ../test

replace github.com/chettriyuvraj/leveldb-clone/wal => ../wal

replace github.com/chettriyuvraj/leveldb-clone/skiplist => ../skiplist

replace github.com/chettriyuvraj/leveldb-clone/common => ../common

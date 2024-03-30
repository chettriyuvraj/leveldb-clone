module github.com/chettriyuvraj/leveldb-clone/matureleveldb

go 1.20

replace github.com/chettriyuvraj/leveldb-clone/common => ../common

replace github.com/chettriyuvraj/leveldb-clone/skiplist => ../skiplist

require (
	github.com/chettriyuvraj/leveldb-clone/common v0.0.0-00010101000000-000000000000
	github.com/chettriyuvraj/leveldb-clone/skiplist v0.0.0-00010101000000-000000000000
	github.com/chettriyuvraj/leveldb-clone/test v0.0.0-00010101000000-000000000000
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/testify v1.9.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/chettriyuvraj/leveldb-clone/test => ../test

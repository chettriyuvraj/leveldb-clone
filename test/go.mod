module github.com/chettriyuvraj/leveldb-clone/test

go 1.20

require (
	github.com/chettriyuvraj/leveldb-clone/common v0.0.0-20240405203313-e1ded512338c
	github.com/stretchr/testify v1.9.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/chettriyuvraj/leveldb-clone/common => ../common

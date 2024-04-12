# README

### Skiplists

A skip list implementation based on [this paper](https://www.epaperpress.com/sortsearch/download/skiplist.pdf)

Unlike the paper, our nodes contain bytes in the form of Go byte slices as the comparators for nodes




## Misc

- Along with _Search_ we also implement an additional _SearchClosest_ which is mainly for range scan
- Separate function to compare skip lists nodes as we want to handle special comparisons. E.g. when a node is compared to the nil node (last node) of a skip list, nil node must always come out greater (can be thought of as an infinity value)
- This package was created to be used as the in-memory data structure containing data (k:v pairs) in a K:V database. However, the implementation can be used for anything. The interface is fairly unconstrained.
- An additional metadata field exists on each skiplist node - which really can be used for whatever. It's currently used for implementing TOMBSTONES in the memtable for the LevelDB clone (of which this package is a part).
# README


**MemDB**: In-memory database that contains data upto a certain threshold.

## MemDB

- The idea in general is referred to as memtables (refer to DDIA Chapter 3: Storage and Retrieval)
- The underlying Get, Put, Delete operations are implemented using a skip list

## Misc
- All our iterators are designed such that it contains a starting value before the first Next() call
- An extra search op done to change db size for both get and put - to preserve simplicity instead of changing interface
- We send extra metadata to our skip lists as a byte slice to indicate if a note is a regular node or a _tombstone_ node

### To dos
- Look at memdb implementation for refactors.
- DB Size computations not tested
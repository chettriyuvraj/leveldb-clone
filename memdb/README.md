# README

Two main things that this package comprises of: memdb and sstables

**MemDB**: In-memory database that contains data upto a certain threshold.

**SSTables**: MemDB can be flushed to disk once it exceeds a certain capacity, this representation is the SSTable (Sorted-String table)

The high-level idea is that if you want to find an element, you would search the MemDB first, then the most recent SSTable, then the second most recent, and so on...

## MemDB

- The idea in general is referred to as memtables (refer to DDIA Chapter 3: Storage and Retrieval)
- The underlying Get, Put, Delete operations are implemented using a skip list

## SSTables

### Format

- Directory Offset: 8 bytes to indicate the offset for the _Key Directory_
- The data in the form of records, each individual record comprises of
    - Key-length: 4 bytes
    - Key: (key-length) bytes 
    - Val-length: 4 bytes 
    - Val: (val-length) bytes
- Key Directory: contains data in the form of directory records, each individual record comprises of
    - Key-length: 4 bytes
    - Key: (key-length) bytes
    - Offset: 8 bytes
- Note: Our key directory does not contain all SSTables, but instead keys separated by a certain (gap) e.g 10 bytes, this is what is meant by a _sparse index_
- Thus, our SSTable file looks like
```
[Dir Offset]
[KV record 1]
[KV record 2]
    .
    . 
[Directory record 1]
[Directory record 2]
    .
    .
```

### Reading SSTables
- The basic idea is that SSTables are sorted and can be read using the key directory.
- Steps to read a key: 
    - Read the key directory into memory
    - Perform a binary search in the struct for your key, find the closest key to the _left_ of your key i.e. greatest key that is smaller than or equal to your key
    - Seek to that particular offset in the file and scan forwards until you find your key


## Misc
- All our iterators are designed such that it contains a starting value before the first Next() call
- I had initially created an SSTable where the directory contained every key, this took quite a while to change into our _sparse index_
- An extra search op done to change db size for both get and put - to preserve simplicity instead of changing interface

### To dos
- Tests for SSTables are practically unreadable + don't cover a lot of edges (I feel), refactor them.
- Code for sstables get/iterating also feels like it can be refactored.
- Also look at memdb implementation for refactors.
- DB Size computations not tested
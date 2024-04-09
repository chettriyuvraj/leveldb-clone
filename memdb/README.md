# README

Two main things that this package comprises of: memdb and sstables

MemDB: In-memory database that contains data upto a certain threshold.

SSTables: MemDB can be flushed to disk once it exceeds a certain capacity, this representation is the SSTable (Sorted-String table)

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
    - Perform a binary search in the struct for your key
    - If key found in directory, seek to that particular offset in the file and extract the key:val pair.


## Misc

### To dos
- Tests for SSTables are practically unreadable, refactor them.
- Have a look at memdb and sstable implementations for refactors.
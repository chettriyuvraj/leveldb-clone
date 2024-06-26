# README


This db aggregates the WAL, memdb and sstables




## Misc
- Vals of length 0 are disallowed, this is because we use keys of length 0 as a _tombstone_ symbol in our SSTables
- Memdb Size() indicates purely the summation of size of key + value pairs in it, while SSTable Size() indicates the entire file size of sstable including key index directory
- Splitting of data after compaction:
    - considers only KV length for the _size_ and ignores the 
    - edges of data may overflow slightly above prescribed limit
- All iterators defined such that they contain the first value before first call of Next(), i.e. first call to next Next() changes them to their second key/value
- Naming of SSTable files + naming after compaction is extremely hacky + also results in a small time frame where we are removing compaction directory and renaming a temp directory to new compaction directory

## To Dos
- Big refactor
    - In order to get compaction working did some hacky workarounds + some duplicate funcs like emptyDir() and removeSSTFiles()
    - By the end of it felt really messy
    - Compaction using filenames by index (sst1, sst2...) has also led to a lot of code
    - 2 level compaction also kindof inextensible and feels very hardcoded

- Compaction
    - Testing not robust; compaction seems to be proper though (checked with debugger) - lexiographic ordering might trip you up i.e. key1, key10 together due to lexiography after compaction
    - Currently all compaction files being checked for each key (db loads level 0 + level 1 on startup), create a _manifest_ that makes note of compaction file ranges so we don't need to load their index
- Inconsistencies
    - MemDB implements full scan by passing limitKey as nil; SSTables and top level db implement full scan using a separate function
- Refactor
    - Certain funcs can be merged eg. GetSSTableData, GetSSTableDataUntilLimit
- We are deleting files after tests using emptyDir  - feels kindof hacky - infact whereever the function is used - feels kindof hacky

# README

## WAL

The WAL(Write-Ahead-Log) is basically a log of each action performed on the current memtable stored on disk. We flush our memtable to disk as SSTables only when it exceeds a threshold, until which time it is purely in-memory.

**Q**: What happens when DB is closed or when the DB crashes? <br>
**A**: This is exactly when the WAL is required. It can be used to restore the in-memory memtable in such cases. So each time the DB is restarted, we use the WAL to restore its contents.


## Log format

Each individual record in our WAL has the following format, in Big-Endian:

- Op-type: 1 byte
- Key-length: 4 bytes
- Key: {key-length} bytes
- Val-length: 4 bytes
- Val: {val-length} bytes

``` 
|----1-----|------4-------|---(key-length)---|------4-------|---(val-length)---| 

|--OpType--|----KeyLen----|-------Key--------|----ValLen----|-------Val--------|
```

## Misc

- _func (record *LogRecord) UnmarshalBinary(data []byte) error {}_ did not end up being used anywhere, still keeping it around.
- The WAL struct could seemingly do without a filename.
- The WAL does not call Sync(), so something like a power failure could, in theory end up causing a loss of data. Modern file systems apparently make that window vanishingly small though.
    - Call Sync() on the file
    - Call Sync() on the directory in which the file exists
- Who undertakes the responsibility of executing a Close() on the WAL? Multiple actors calling close may also lead to errors, although a defer Close() call might not usually wait to acknowledge the error. In our setup a top-level DB encapsulates the wal and the client will have access to only the DB - so we provide a public db.Close(), which in turn executes all cleanup for our db, including calling wal.Close()
# README


This db aggregates the WAL, memdb and sstables




## Misc
- Vals of length 0 are disallowed, this is because we use keys of length 0 as a _tombstone_ symbol in our SSTables


## To Dos

- RangeScan are failing because we don't search both SST + mem
- We are deleting files after tests using emptyAllFiles  - feels kindof hacky - infact whereever the function is used - feels kindof hacky
- Can eventually pass configs such as db name, memdb limit, level-config as one Config{} struct
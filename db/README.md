# README


This db aggregates the WAL, memdb and sstables





## To Dos

- RangeScan and Delete tests are failing because we don't search both SST + mem
- We are deleting files after tests using emptyAllFiles  - feels kindof hacky - infact whereever the function is used - feels kindof hacky
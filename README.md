# README


This is a LevelDB clone that seeks to implement ideas from LevelDB including

- Skip Lists
- Write Ahead Logs
- SSTables
- Overarching ideas of LSM trees/Levelled compaction


## Status

- **Skip Lists**: completed
- **Write Ahead Logs**: completed
- **SSTables**:
    - Design and implementation completed
    - Flushing memtable to disk as SSTables completed
    - Get and Put ops completed
    - Delete and RangeScan completed
- **LSM Trees/Levelled Compaction**:
    - completed
- **Bloom Filters**:
    - to start!


## Misc

- Take a day to refactor once completed, taken some liberties in terms of cleanliness [:(] to get the Levelled Compaction working!
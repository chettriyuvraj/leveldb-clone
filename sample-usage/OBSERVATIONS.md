## Observations

- A new manifest file seems to be created at each run of the DB, the old one is deleted
- CURRENT and CURRENT.bak: 
    - Stores new manifest file name and prev manifest file name respectively
    - CURRENT.bak created only after 2nd run.
- LOG: Informational messages
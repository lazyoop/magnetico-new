# This directory

Data is obtained from the queue and stored in the database for persistence.

## Development model

Create separate Go files for connecting to the queue and the database. The queue should implement the `PersistentStorageServer` interface, while the database needs to implement the `SqlDatabase` interface.
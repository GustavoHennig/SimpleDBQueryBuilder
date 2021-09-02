
### Simple Database Query Builder 

This is a Simple Database **Query Builder** with **Migrations** and **Pooling**.

Initially designed for **Sqlite** but can be used with other databases.

The goal is to be extremely simple, and to use **lightweight** data structures to manipulate in memory.


#### How it works
Using the patter Producer/Consumer, all queries are put int a queue, 
 then a single thread is responsible to execute each command.  
It is optimized for SQLite, running batch operations inside a transaction.
  
#### Known issues

`DbQueue` implementation is problematic, if an error occurs, all transaction commands will be lost.

#### Things to do
- [ ] Unit tests;
- [ ] Refactor `BaseDbConn` and `DbQueue` to respect the single-responsibility principle.
- [ ] Organize folder/namespaces
- [ ] Fix DbQueue reliability problem

#### Do not forget
- SOLID is nice, but KISS is what guides this project :)


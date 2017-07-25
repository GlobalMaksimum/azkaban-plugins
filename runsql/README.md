# runsql

The job can be used for running sql statements against Vertica and Mysql.

* Job files of 'runsql' type may have following parameters:

| Parameter     | Value | Comment |
|---------------|-------|---------|
| `db`          | Parameter for the database _(*)_|Vertica or Mysql _(Default: Vertica)_|
| `type`        | runsql                          ||
| `file`        | name_of_sql_file                ||
| `working.dir` | `/path/to/sql/file`             | Optional *(Default: Current directory of '.job' file)* |
| `param.extra` | Parameter for sql statement     | Optional *(One or more)* |

_* `db` value will be used for getting related values from **private.properties** file. One must change `db-param` in the next section with this db value.)_

* Property files of 'runsql' type may have following parameters:

| Parameter                      | Value                                      | Comment |
| ------------------------------ | ------------------------------------------ | ------- |
| `jobtype.class`                | `com.globalmaksimum.azkabanJobs.RunSqlJob` | Shows implementation of the job |
| `db.db-param.user`             | User of the database                       |         |
| `db.db-param.pass`             | Pass of the user                           |         |
| `db.db-param.host`             | Host address of the database               |         |
| `db.db-param.db`               | Name of the database                       |         |
| `db.db-param.type`             | Type of the database                       | **Vertica** or **Mysql** |
| `db.db-param.backupservernode` | Addresses of back up server nodes          | Use only for Vertica *(Comma separated)* |

_* Parameters other than `jobtype.class` can be defined multiple times with a different `db-param` value._

### An example job and property files:

```
Test.job
---------
    db=test
    type=runsql
    file=test.sql
    working.dir=/home/test/
```
```
Test2.job
----------
    type=runsql
    file=test.sql
    working.dir=/home/vertica/test/
```
```
Test3.job
----------
    db=prod
    type=runsql
    file=prod.sql
    param.x=5
    param.y=text
```
```
private.properties
-------------------
    db.prod.user=admin
    db.prod.pass=admin
    db.prod.host=host1
    db.prod.db=vertica-prod
    db.prod.type=vertica
    db.prod.backupservernode=host2, host3, host4

    db.test.user=admin
    db.test.pass=admin
    db.test.host=host5
    db.test.db=mysql-test
    db.test.type=mysql

    db.vertica.user=admin
    db.vertica.pass=admin
    db.vertica.host=host5
    db.vertica.db=vertica-test
    db.vertica.type=vertica
```

### How to Use Parameters ?

After defining **_param.param_name_** parameter of a job, all substrings of related sql statement matching with **_{{param_name}}_** will be replaced by the value of the parameter. 

* Example:
```
    Test.job
    ---------
        type=runsql
        file=test.sql
        param.x=30
```
```
        test.sql
        ---------
            SELECT COUNT(*) FROM people WHERE age > {{x}}
```